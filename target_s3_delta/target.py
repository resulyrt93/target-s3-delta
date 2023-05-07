"""s3-delta target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_s3_delta.sinks import (
    S3DeltaSink,
)


class TargetS3Delta(Target):
    """Sample target for s3-delta."""

    name = "target-s3-delta"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "s3_path",
            th.StringType,
            description="The s3 path to the target output file",
            required=True,
        ),
        th.Property("aws_access_key_id", th.StringType, required=True),
        th.Property(
            "aws_secret_access_key",
            th.StringType,
            required=True,
        ),
        th.Property(
            "aws_region",
            th.StringType,
            default="us-east-1"
        ),
    ).to_dict()

    default_sink_class = S3DeltaSink