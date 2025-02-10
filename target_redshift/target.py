"""Redshift target class."""

from __future__ import annotations

from typing import TYPE_CHECKING

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_redshift.sinks import RedshiftSink

if TYPE_CHECKING:
    from pathlib import PurePath


class TargetRedshift(SQLTarget):
    """Target for Redshift."""

    _MAX_RECORD_AGE_IN_MINUTES: float = 60

    def __init__(
        self,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,  # noqa: FBT001, FBT002
        validate_config: bool = True,  # noqa: FBT001, FBT002
    ) -> None:
        """Initialize the target.

        Args:
            config: Target configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
        """
        self.max_parallelism = 1
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

        assert self.config.get("add_record_metadata") or not self.config.get(  # noqa: S101
            "activate_version"
        ), (
            "Activate version messages can't be processed unless add_record_metadata "
            "is set to true. To ignore Activate version messages instead, Set the "
            "`activate_version` configuration to False."
        )

        assert self.config.get("s3_bucket") is not None  # noqa: S101

    name = "target-redshift"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            description=(
                "Hostname for redshift instance."
            ),
        ),
        th.Property(
            "port",
            th.StringType,
            default="5432",
            description=(
                "The port on which redshift is awaiting connection."
            ),
        ),
        th.Property(
            "enable_iam_authentication",
            th.BooleanType,
            description=(
                "If true, use temporary credentials "
                "(https://docs.aws.amazon.com/redshift/latest/mgmt/generating-iam-credentials-cli-api.html)."
            ),
            title="Enable IAM Authentication",
        ),
        th.Property(
            "cluster_identifier",
            th.StringType,
            description=(
                "Redshift cluster identifier. Note if sqlalchemy_url is set or "
                "enable_iam_authentication is false this will be ignored."
            ),
        ),
        th.Property(
            "user",
            th.StringType,
            description=(
                "User name used to authenticate. Note if sqlalchemy_url is set this "
                "will be ignored."
            ),
        ),
        th.Property(
            "password",
            th.StringType,
            description=(
                "Password used to authenticate. Note if sqlalchemy_url is set this "
                "will be ignored."
            ),
        ),
        th.Property(
            "dbname",
            th.StringType,
            description=(
                "Database name. Note if sqlalchemy_url is set this will be ignored."
            ),
            title="Database Name",
        ),
        th.Property(
            "aws_redshift_copy_role_arn",
            th.StringType,
            secret=True,  # Flag config as protected.
            required=True,
            description="Redshift copy role arn to use for the COPY command from s3",
            title="AWS Redshift Copy Role ARN",
        ),
        th.Property(
            "s3_bucket",
            th.StringType,
            required=True,
            description="S3 bucket to save staging files before using COPY command",
        ),
        th.Property(
            "s3_region",
            th.StringType,
            description=(
                "AWS region for S3 bucket. If not specified, region will be "
                "detected by boto config resolution. "
                "See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html"
            ),
        ),
        th.Property(
            "s3_key_prefix",
            th.StringType,
            description="S3 key prefix to save staging files before using COPY command",
            default="",
        ),
        th.Property(
            "remove_s3_files",
            th.BooleanType,
            default=False,
            description="If you want to remove staging files in S3",
        ),
        th.Property(
            "temp_dir",
            th.StringType,
            default="temp",
            description="Where you want to store your temp data files.",
        ),
        th.Property(
            "default_target_schema",
            th.StringType,
            description="Redshift schema to send data to, example: tap-clickup",
        ),
        th.Property(
            "activate_version",
            th.BooleanType,
            default=False,
            description=(
                "If set to false, the tap will ignore activate version messages. If "
                "set to true, add_record_metadata must be set to true as well."
            ),
        ),
        th.Property(
            "hard_delete",
            th.BooleanType,
            default=False,
            description=(
                "When activate version is sent from a tap this specefies "
                "if we should delete the records that don't match, or mark "
                "them with a date in the `_sdc_deleted_at` column. This config "
                "option is ignored if `activate_version` is set to false."
            ),
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType,
            default=False,
            description=(
                "Note that this must be enabled for activate_version to work!"
                "This adds _sdc_extracted_at, _sdc_batched_at, and more to every "
                "table. See https://sdk.meltano.com/en/latest/implementation/record_metadata.html "  # noqa: E501
                "for more information."
            ),
        ),
        th.Property(
            "ssl_enable",
            th.BooleanType,
            default=False,
            description=(
                "Whether or not to use ssl to verify the server's identity. Use"
                " ssl_certificate_authority and ssl_mode for further customization."
                " To use a client certificate to authenticate yourself to the server,"
                " use ssl_client_certificate_enable instead."
            ),
        ),
        th.Property(
            "ssl_mode",
            th.StringType,
            default="verify-full",
            description=(
                "SSL Protection method, see [redshift documentation](https://docs.aws.amazon.com/redshift/latest/mgmt/connecting-ssl-support.html"
                " for more information. Must be one of disable, allow, prefer,"
                " require, verify-ca, or verify-full."
            ),
        ),
    ).to_dict()

    default_sink_class = RedshiftSink


if __name__ == "__main__":
    TargetRedshift.cli()
