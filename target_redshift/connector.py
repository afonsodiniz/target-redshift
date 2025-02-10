"""Common SQL connectors for Streams and Sinks."""

from __future__ import annotations

import typing as t
from contextlib import contextmanager
from typing import cast

import boto3
import redshift_connector
from redshift_connector import Cursor
from singer_sdk.connectors import SQLConnector
from singer_sdk.helpers._typing import get_datelike_property_type
from singer_sdk.typing import _jsonschema_type_check
from sqlalchemy import DDL, Column, MetaData, Table
from sqlalchemy.engine.url import URL
from sqlalchemy.schema import CreateSchema, CreateTable, DropTable
from sqlalchemy.types import (
    BOOLEAN,
    DATE,
    DATETIME,
    TIME,
    TypeEngine,
)
from sqlalchemy_redshift.dialect import BIGINT, DOUBLE_PRECISION, SUPER, VARCHAR


class RedshiftConnector(SQLConnector):
    """Sets up SQL Alchemy, and other Postgres related stuff."""

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.
    default_varchar_length = 10000

    def prepare_schema(self, schema_name: str, cursor: Cursor) -> None:
        """Create the target database schema.

        Args:
            schema_name: The target schema name.
            cursor: The database cursor.
        """
        schema_exists = self.schema_exists(schema_name)
        if not schema_exists:
            self.create_schema(schema_name, cursor=cursor)

    def create_schema(self, schema_name: str, cursor: Cursor) -> None:
        """Create target schema.

        Args:
            schema_name: The target schema to create.
            cursor: The database cursor.
        """
        cursor.execute(str(CreateSchema(schema_name)))

    @contextmanager
    def connect_cursor(self) -> t.Iterator[Cursor]:
        """Connect to a redshift connector cursor.

        Returns:
        -------
        t.Iterator[Cursor]
            A redshift connector cursor.

        Yields:
        ------
        Iterator[t.Iterator[Cursor]]
            A redshift connector cursor.
        """
        user, password = self.get_credentials()
        with redshift_connector.connect(
            user=user,
            password=password,
            host=self.config["host"],
            port=self.config["port"],
            database=self.config["dbname"],
            ssl=self.config["ssl_enable"],
            sslmode=self.config["ssl_mode"],
        ) as connection:
            with connection.cursor() as cursor:
                yield cursor
            connection.commit()

    def prepare_table(  # type: ignore[override]  # noqa: D417
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: t.Sequence[str],
        cursor: Cursor,
        as_temp_table: bool = False,  # noqa: FBT001, FBT002
    ) -> Table:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the target table name.
            schema: the JSON Schema for the table.
            primary_keys: list of key properties.
            connection: the database connection.
            as_temp_table: True to create a temp table.

        Returns:
            The table object.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = MetaData(schema=schema_name)

        if self.table_exists(full_table_name=full_table_name):
            table: Table = self.get_table(full_table_name=full_table_name)
            columns = {column.name: column for column in table.columns}
            for property_name, property_def in schema["properties"].items():
                column_object = None
                if property_name in columns:
                    column_object = columns[property_name]
                self.prepare_column(
                    full_table_name=table.fullname,
                    column_name=property_name,
                    sql_type=self.to_sql_type(property_def),
                    cursor=cursor,
                    column_object=column_object,
                )
        else:
            table: Table = self.create_empty_table(
                table_name=table_name,
                meta=meta,
                schema=schema,
                primary_keys=primary_keys,
                as_temp_table=as_temp_table,
                cursor=cursor,
            )

        return table

    def get_table(
        self,
        full_table_name: str,
    ) -> Table:
        """Return a table object.

        Args:
            full_table_name: Fully qualified table name.
            column_names: A list of column names to filter to.

        Returns:
            A table object with column list.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = MetaData(schema=schema_name)
        return Table(
            table_name,
            meta,
            autoload_with=self._engine,
        )

    def copy_table_structure(
        self,
        full_table_name: str,
        from_table: Table,
        cursor: Cursor,
        as_temp_table: bool = False,  # noqa: FBT001, FBT002
    ) -> Table:
        """Copy table structure.

        Args:
            full_table_name: the target table name potentially including schema
            from_table: the  source table
            connection: the database connection.
            cursor: A redshift connector cursor.
            as_temp_table: True to create a temp table.

        Returns:
            The new table object.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = MetaData(schema=schema_name)
        new_table: Table
        if self.table_exists(full_table_name=full_table_name):
            msg = "Table already exists"
            raise RuntimeError(msg)
        columns = [column._copy() for column in from_table.columns]  # noqa: SLF001
        if as_temp_table:
            new_table = Table(table_name, meta, *columns, prefixes=["TEMPORARY"])
        else:
            new_table = Table(table_name, meta, *columns)

        create_table_ddl = str(
            CreateTable(new_table).compile(dialect=self._engine.dialect)
        )
        cursor.execute(create_table_ddl)
        return new_table

    def drop_table(self, table: Table, cursor: Cursor) -> None:
        """Drop table data."""
        drop_table_ddl = str(DropTable(table).compile(dialect=self._engine.dialect))
        cursor.execute(drop_table_ddl)

    def to_sql_type(self, jsonschema_type: dict) -> TypeEngine:  # noqa: PLR0911
        """Convert JSON Schema type to a SQL type.

        Args:
            jsonschema_type: The JSON Schema object.

        Returns:
            The SQL type.
        """
        if _jsonschema_type_check(jsonschema_type, ("string",)):
            datelike_type = get_datelike_property_type(jsonschema_type)
            if datelike_type:
                if datelike_type == "date-time":
                    return DATETIME()
                if datelike_type in "time":
                    return TIME()
                if datelike_type == "date":
                    return DATE()
            return VARCHAR(self.default_varchar_length)

        if _jsonschema_type_check(jsonschema_type, ("integer",)):
            return BIGINT()
        if _jsonschema_type_check(jsonschema_type, ("number",)):
            return DOUBLE_PRECISION()
        if _jsonschema_type_check(jsonschema_type, ("boolean",)):
            return BOOLEAN()

        if _jsonschema_type_check(jsonschema_type, ("object", "array")):
            return SUPER()

        return VARCHAR(self.default_varchar_length)

    def create_empty_table(  # type: ignore[override]  # noqa: PLR0913
        self,
        table_name: str,
        meta: MetaData,
        schema: dict,
        cursor: Cursor,
        primary_keys: t.Sequence[str] | None = None,
        as_temp_table: bool = False,  # noqa: FBT001, FBT002
    ) -> Table:
        """Create an empty target table.

        Args:
            table_name: the target table name.
            meta: the SQLAchemy metadata object.
            schema: the JSON schema for the new table.
            cursor: the database cursor.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.

        Returns:
            The new table object.

        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        columns: list[Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError:
            msg = (
                f"Schema for table_name: '{table_name}'"
                f"does not define properties: {schema}"
            )
            raise RuntimeError(msg)  # noqa: B904

        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys
            columns.append(
                Column(
                    property_name,
                    self.to_sql_type(property_jsonschema),
                    primary_key=is_primary_key,
                    autoincrement=False,  # See: https://github.com/MeltanoLabs/target-postgres/issues/193 # noqa: E501
                )
            )
        if as_temp_table:
            new_table = Table(table_name, meta, *columns, prefixes=["TEMPORARY"])
        else:
            new_table = Table(table_name, meta, *columns)

        create_table_ddl = str(
            CreateTable(new_table).compile(dialect=self._engine.dialect)
        )
        cursor.execute(create_table_ddl)
        return new_table

    def prepare_column(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: TypeEngine,
        cursor: Cursor,
        column_object: Column | None = None,
    ) -> None:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the fully qualified table name.
            column_name: the target column name.
            sql_type: the SQLAlchemy type.
            cursor: a database cursor.
            column_object: a SQLAlchemy column. optional.
        """
        column_name = column_name.lower().replace(" ", "_")
        column_exists = column_object is not None or self.column_exists(
            full_table_name, column_name
        )

        if not column_exists:
            self._create_empty_column(
                # We should migrate every function to use Table
                # instead of having to know what the function wants
                full_table_name=full_table_name,
                column_name=column_name,
                sql_type=sql_type,
                cursor=cursor,
            )
            return

        self._adapt_column_type(
            full_table_name=full_table_name,
            column_name=column_name,
            sql_type=sql_type,
            cursor=cursor,
        )

    def _create_empty_column(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: TypeEngine,
        cursor: Cursor,
    ) -> None:
        """Create a new column.

        Args:
            full_table_name: The target table name.
            column_name: The name of the new column.
            sql_type: SQLAlchemy type engine to be used in creating the new column.
            cursor: a database cursor.

        Raises:
            NotImplementedError: if adding columns is not supported.
        """
        if not self.allow_column_add:
            msg = "Adding columns is not supported."
            raise NotImplementedError(msg)

        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        column_add_ddl = str(
            self.get_column_add_ddl(
                table_name=table_name,
                schema_name=schema_name,
                column_name=column_name,
                column_type=sql_type,
            )
        )
        cursor.execute(column_add_ddl)

    def get_column_add_ddl(  # type: ignore[override]
        self,
        table_name: str,
        schema_name: str,
        column_name: str,
        column_type: TypeEngine,
    ) -> DDL:
        """Get the create column DDL statement.

        Args:
            table_name: Fully qualified table name of column to alter.
            schema_name: Schema name.
            column_name: Column name to create.
            column_type: New column sqlalchemy type.

        Returns:
            A sqlalchemy DDL instance.
        """
        column = Column(column_name, column_type)

        return DDL(
            (
                'ALTER TABLE "%(schema_name)s"."%(table_name)s"'
                "ADD COLUMN %(column_name)s %(column_type)s"
            ),
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": column.compile(dialect=self._engine.dialect),
                "column_type": column.type.compile(dialect=self._engine.dialect),
            },
        )

    def _adapt_column_type(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: TypeEngine,
        cursor: Cursor,
    ) -> None:
        """Adapt table column type to support the new JSON schema type.

        Args:
            full_table_name: The target table name.
            column_name: The target column name.
            sql_type: The new SQLAlchemy type.
            cursor: a database cursor.

        Raises:
            NotImplementedError: if altering columns is not supported.
        """
        current_type: TypeEngine = self._get_column_type(
            full_table_name,
            column_name,
        )

        # remove collation if present and save it
        current_type_collation = self.remove_collation(current_type)

        # Check if the existing column type and the sql type are the same
        if str(sql_type) == str(current_type):
            # The current column and sql type are the same
            # Nothing to do
            return

        # Not the same type, generic type or compatible types
        # calling merge_sql_types for assistnace
        compatible_sql_type = self.merge_sql_types([current_type, sql_type])

        if str(compatible_sql_type) == str(current_type):
            # Nothing to do
            return

        # Put the collation level back before altering the column
        if current_type_collation:
            self.update_collation(compatible_sql_type, current_type_collation)

        if not self.allow_column_alter:
            msg = (
                "Altering columns is not supported. Could not convert column "
                f"'{full_table_name}.{column_name}' from '{current_type}' to "
                f"'{compatible_sql_type}'."
            )
            raise NotImplementedError(msg)

        alter_column_ddl = str(
            self.get_column_alter_ddl(
                table_name=full_table_name,
                column_name=column_name,
                column_type=compatible_sql_type,
            )
        )
        cursor.execute(alter_column_ddl)

    def get_column_alter_ddl(  # type: ignore[override]
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        column_type: TypeEngine,
    ) -> DDL:
        """Get the alter column DDL statement.

        Override this if your database uses a different syntax for altering columns.

        Args:
            schema_name: Schema name.
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to alter.
            column_type: New column type string.

        Returns:
            A sqlalchemy DDL instance.
        """
        column = Column(column_name, column_type)
        return DDL(
            (
                'ALTER TABLE "%(schema_name)s"."%(table_name)s"'
                "ALTER COLUMN %(column_name)s %(column_type)s"
            ),
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": column.compile(dialect=self._engine.dialect),
                "column_type": column.type.compile(dialect=self._engine.dialect),
            },
        )

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generate a SQLAlchemy URL.

        Args:
            config: The configuration for the connector.
        """
        user, password = self.get_credentials()
        sqlalchemy_url = URL.create(
            drivername="redshift+redshift_connector",
            username=user,
            password=password,
            host=config["host"],
            port=config["port"],
            database=config["dbname"],
            query=self.get_sqlalchemy_query(config),
        )
        return cast(str, sqlalchemy_url)

    def get_sqlalchemy_query(self, config: dict) -> dict:
        """Get query values to be used for sqlalchemy URL creation.

        Args:
            config: The configuration for the connector.

        Returns:
            A dictionary with key-value pairs for the sqlalchemy query.
        """
        query = {}

        # ssl_enable is for verifying the server's identity to the client.
        if config["ssl_enable"]:
            ssl_mode = config["ssl_mode"]
            query.update({"sslmode": ssl_mode})
        return query

    def get_credentials(self) -> tuple[str, str]:
        """Use boto3 to get temporary cluster credentials.

        Returns:
        -------
        tuple[str, str]
            username and password
        """
        if self.config.get("enable_iam_authentication"):
            client = boto3.client("redshift", region_name="eu-west-1")
            response = client.get_cluster_credentials(
                DbUser=self.config["user"],
                DbName=self.config["dbname"],
                ClusterIdentifier=self.config["cluster_identifier"],
                DurationSeconds=3600,
                AutoCreate=False,
            )
            return response["DbUser"], response["DbPassword"]
        return self.config["user"], self.config["password"]
