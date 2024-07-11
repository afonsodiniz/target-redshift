"""Redshift target sink class, which handles writing streams."""

from __future__ import annotations

import csv
import datetime
import json
import os
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

import boto3
import simplejson as json
import sqlalchemy
from botocore.exceptions import ClientError
from singer_sdk.helpers._compat import (
    date_fromisoformat,
    datetime_fromisoformat,
    time_fromisoformat,
)
from singer_sdk.helpers._typing import (
    DatetimeErrorTreatmentEnum,
    get_datelike_property_type,
    handle_invalid_timestamp_in_record,
)
from singer_sdk.sinks import SQLSink

from .connector import RedshiftConnector

if TYPE_CHECKING:
    from redshift_connector import Cursor


class RedshiftSink(SQLSink):
    """Redshift target sink class."""

    connector_class = RedshiftConnector
    MAX_SIZE_DEFAULT = 50000

    def __init__(self, *args, **kwargs) -> None:  # noqa: ANN002, ANN003
        """Initialize SQL Sink. See super class for more details."""
        super().__init__(*args, **kwargs)
        self.temp_table_name = self.generate_temp_table_name()

        region = self.config.get("s3_region")
        if region is None:
            self.s3_client = boto3.client("s3")
        else:
            self.s3_client = boto3.client("s3", region_name=region)

    @property
    def schema_name(self) -> str | None:
        """Return the schema name or `None` if using names with no schema part.

        Returns:
            The target schema name.
        """
        # Look for a default_target_scheme in the configuraion fle
        default_target_schema: str = self.config.get(
            "default_target_schema", os.getenv("MELTANO_EXTRACT__LOAD_SCHEMA")
        )
        parts = self.stream_name.split("-")

        # 1) When default_target_scheme is in the configuration use it
        # 2) if the streams are in <schema>-<table> format use the
        #    stream <schema>
        # 3) Return None if you don't find anything
        if default_target_schema:
            return default_target_schema
        return self.conform_name(parts[-2], "schema") if len(parts) in {2, 3} else None

    def setup(self) -> None:
        """Set up Sink.

        This method is called on Sink creation, and creates the required Schema and
        Table entities in the target database.
        """
        if self.key_properties is None or self.key_properties == []:
            self.append_only = True
        else:
            self.append_only = False
        with self.connector.connect_cursor() as cursor:
            if self.schema_name:
                self.connector.prepare_schema(self.schema_name, cursor=cursor)
            self.connector.prepare_table(
                full_table_name=self.full_table_name,
                schema=self.schema,
                primary_keys=self.key_properties,
                cursor=cursor,
                as_temp_table=False,
            )

    def generate_temp_table_name(self) -> str:
        """Uuid temp table name."""
        # sqlalchemy.exc.IdentifierError: Identifier  # noqa: ERA001
        # 'temp_test_optional_attributes_388470e9_fbd0_47b7_a52f_d32a2ee3f5f6'
        # exceeds maximum length of 63 characters
        # Is hit if we have a long table name, there is no limit on Temporary tables
        # in postgres, used a guid just in case we are using the same session
        return f"{str(uuid.uuid4()).replace('-', '_')}"

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.

        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.

        Args:
            context: Stream partition or context dictionary.
        """
        # If duplicates are merged, these can be tracked via
        # :meth:`~singer_sdk.Sink.tally_duplicate_merged()`.
        with self.connector.connect_cursor() as cursor:
            # Get target table
            table: sqlalchemy.Table = self.connector.get_table(
                full_table_name=self.full_table_name
            )
            # Create a temp table (Creates from the table above)
            temp_table: sqlalchemy.Table = self.connector.copy_table_structure(
                full_table_name=self.temp_table_name,
                from_table=table,
                as_temp_table=True,
                cursor=cursor,
            )
            # Insert into temp table
            self.file = f"{self.stream_name}-{self.temp_table_name}.csv"
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path = os.path.join(self.config["temp_dir"], self.file)
            self.object = os.path.join(self.config["s3_key_prefix"], self.file)

            self.logger.info(f'bulk insert {len(context["records"])} records into {temp_table}')

            # self.bulk_insert_records(
            #     table=temp_table,
            #     schema=self.schema,
            #     primary_keys=self.key_properties,
            #     records=context["records"],
            #     cursor=cursor,
            # )

            # if incremental
            # then
            # self.logger.info(f'merging {len(context["records"])} records into {table}')
            # Merge data from temp table to main table
            # self.upsert(
            #     from_table=temp_table,
            #     to_table=table,
            #     schema=self.schema,
            #     join_keys=self.key_properties,
            #     cursor=cursor,
            # )
            # clean_resources

            # try to insert from temp to final
            self.insert_from_temp_to_final(temp_table, table, cursor)

        self.clean_resources()

    def bulk_insert_records(  # type: ignore[override]
        self,
        table: sqlalchemy.Table,
        records: Iterable[dict[str, Any]],
        cursor: Cursor,
    ) -> int | None:
        """Bulk insert records to an existing destination table.

        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.

        Args:
            table: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.
            cursor: the redshift connector cursor.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        self.write_csv(records)

        # Add Current Day partition folder
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        path_parts = self.object.split('/')
        path_parts.insert(-1, today)
        new_object_key = '/'.join(path_parts)
        
        self.logger.info(f'writing {len(records)} records to s3://{self.config["s3_bucket"]}/{new_object_key}')

        self.copy_to_s3(new_object_key)
        self.copy_to_redshift(table, cursor, new_object_key)

        return True

    def insert_from_temp_to_final(self, from_table: sqlalchemy.Table, to_table: sqlalchemy.Table, cursor: Cursor):
        """
        Inserts records from a temporary table to the final table.

        Args:
            from_table (sqlalchemy.Table): The temporary table containing the data to insert.
            to_table (sqlalchemy.Table): The final table where data should be inserted.
            cursor (Cursor): Database cursor for executing SQL commands.
        """
        # Construct the SQL statement to insert data from the temporary table to the final table.
        # Assuming that the structures of both tables are identical.
        insert_sql = f"""
        INSERT INTO {self.connector.quote(str(to_table))}
        SELECT * FROM {self.connector.quote(str(from_table))}
        """
        
        # Execute the SQL command.
        cursor.execute(insert_sql)
        self.logger.info(f"Inserted records from {from_table} into {to_table}.")

    # def upsert(
    #     self,
    #     from_table: sqlalchemy.Table,
    #     to_table: sqlalchemy.Table,
    #     schema: dict,
    #     join_keys: List[str],
    #     cursor: Cursor,
    # ) -> Optional[int]:
    #     """Merge upsert data from one table to another.

    #     Args:
    #         from_table: The source table.
    #         to_table: The destination table.
    #         schema: Singer Schema message.
    #         join_keys: The merge upsert keys, or `None` to append.
    #         cursor: The database cursor.

    #     Return:
    #         The number of records copied, if detectable, or `None` if the API does not
    #         report number of records affected/inserted.

    #     """
    #     join_predicates = []
    #     to_table_key: sqlalchemy.Column
    #     for key in join_keys:
    #         from_table_key: sqlalchemy.Column = from_table.columns[key]
    #         to_table_key = to_table.columns[key]
    #         join_predicates.append(from_table_key == to_table_key)

    #     join_condition = sqlalchemy.and_(*join_predicates)
    #     merge_sql = f"""
    #         MERGE INTO {self.connector.quote(str(to_table))}
    #         USING {self.connector.quote(str(from_table))}
    #         ON {join_condition}
    #         REMOVE DUPLICATES
    #         """
    #     cursor.execute(merge_sql)
    #     return None

    def write_csv(self, records: list[dict]) -> None:
        """Write records to a local csv file.

        Parameters
        ----------
        records : List[dict]
            the input records.

        Returns:
        -------
        None

        Raises:
        ------
        ValueError
            _description_
        """
        if "properties" not in self.schema:
            msg = "Stream's schema has no properties defined."
            raise ValueError(msg)
        keys: list[str] = list(self.schema["properties"].keys())
        object_keys = [
            key
            for key, value in self.schema["properties"].items()
            if "object" in value["type"] or "array" in value["type"]
        ]
        records = [
            {
                key: (
                    json.dumps(value).replace("None", "")
                    if key in object_keys
                    else value
                )
                for key, value in record.items()
            }
            for record in records
        ]
        with self.path.open("w", encoding="utf-8", newline="") as fp:
            writer = csv.DictWriter(
                fp,
                fieldnames=keys,
                extrasaction="ignore",
                dialect="excel",
            )
            writer.writerows(records)

    def copy_to_s3(self, new_object_key):
        try:
            _ = self.s3_client.upload_file(self.path, self.config["s3_bucket"], new_object_key)
        except ClientError as e:
            self.logger.error(e)

    def copy_to_redshift(self, table: sqlalchemy.Table, cursor: Cursor, new_object_key):
        """Copy the s3 csv file to redshift."""
        copy_credentials = f"IAM_ROLE '{self.config['aws_redshift_copy_role_arn']}'"
        
        # Step 3: Generate copy options - Override defaults from config.json if defined
        copy_options = self.config.get(
            "copy_options",
            """
            EMPTYASNULL BLANKSASNULL TRIMBLANKS TRUNCATECOLUMNS
            DATEFORMAT 'auto' TIMEFORMAT 'auto'
            COMPUPDATE OFF STATUPDATE OFF
        """,
        )
        columns = ", ".join([f'"{column}"' for column in self.schema["properties"]])
        # Step 4: Load into the stage table
        copy_sql = f"""
            COPY {self.connector.quote(str(table))} ({columns})
            FROM 's3://{self.config["s3_bucket"]}/{new_object_key}' 
            {copy_credentials}
            {copy_options}
            CSV
        """

        cursor.execute(copy_sql)

    def parse_timestamps_in_record(
        self,
        record: dict,
        schema: dict,
        treatment: DatetimeErrorTreatmentEnum,
    ) -> None:
        """Parse strings to datetime.datetime values, repairing or erroring on failure.

        Attempts to parse every field that is of type date/datetime/time. If its value
        is out of range, repair logic will be driven by the `treatment` input arg:
        MAX, NULL, or ERROR.

        Args:
            record: Individual record in the stream.
            schema: TODO
            treatment: TODO
        """
        for key, value in record.items():
            if key not in schema["properties"]:
                if value is not None:
                    self.logger.warning("No schema for record field '%s'", key)
                continue
            datelike_type = get_datelike_property_type(schema["properties"][key])
            if datelike_type:
                date_val = value
                try:
                    if value is not None:
                        if datelike_type == "time":
                            date_val = time_fromisoformat(date_val)
                        elif datelike_type == "date":
                            date_val = date_fromisoformat(date_val)
                        else:
                            date_val = datetime_fromisoformat(date_val)
                except ValueError as ex:
                    date_val = handle_invalid_timestamp_in_record(
                        record,
                        [key],
                        date_val,
                        datelike_type,
                        ex,
                        treatment,
                        self.logger,
                    )
                record[key] = date_val

    def clean_resources(self) -> None:
        """Remove local and s3 resources."""
        Path.unlink(self.path)
        if self.config["remove_s3_files"]:
            try:
                _ = self.s3_client.delete_object(
                    Bucket=self.config["s3_bucket"], Key=self.object
                )
            except ClientError:
                self.logger.exception()
