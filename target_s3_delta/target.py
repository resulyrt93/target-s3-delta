"""s3-delta target class."""

from __future__ import annotations

import copy
import os
from typing import List, Optional, Dict

from deltalake import write_deltalake
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from singer_sdk import typing as th
from singer_sdk.target_base import Target

from target_s3_delta.common import ExtractMode
from target_s3_delta.sinks import (
    S3DeltaSink,
    TEMP_DATA_DIRECTORY,
)


def read_parquet_generator(file_paths):
    for file_path in file_paths:
        df = pd.read_parquet(file_path, engine="pyarrow")
        sorted_columns = sorted(df.columns)
        df = df[sorted_columns]

        record_batch = pa.RecordBatch.from_pandas(df)
        yield record_batch


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
        th.Property("aws_secret_access_key", th.StringType, required=True),
        th.Property("aws_region", th.StringType, default="us-east-1"),
        th.Property("mode", th.StringType, default=ExtractMode.OVERWRITE),
        th.Property("partition_by", th.StringType),
        th.Property("batch_size", th.IntegerType),
        th.Property("max_rows_per_file", th.IntegerType, default=10 * 1024 * 1024),
    ).to_dict()

    default_sink_class = S3DeltaSink

    def get_partition_config(self) -> Optional[List[str]]:
        partition_config: str = self.config.get("partition_by")
        if partition_config is None or partition_config == "":
            return None

        return partition_config.split(",")

    def get_storage_options(self) -> Dict:
        return {
            "AWS_ACCESS_KEY_ID": self.config.get("aws_access_key_id"),
            "AWS_SECRET_ACCESS_KEY": self.config.get("aws_secret_access_key"),
            "REGION": self.config.get("aws_region"),
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

    def write_batches_to_delta(self):
        storage_options = self.get_storage_options()
        path = self.config.get("s3_path")
        mode = self.config.get("mode")
        partition_by = self.get_partition_config()

        files = [file for file in os.listdir(TEMP_DATA_DIRECTORY) if file.endswith(".parquet")]

        if len(files) == 0:
            self.logger.warn(f"Could not create any file.")
            return

        absolute_files = [f"{TEMP_DATA_DIRECTORY}{file}" for file in files]

        data = read_parquet_generator(absolute_files)
        first_batch = pq.read_table(absolute_files[0])

        if len(first_batch) == 0:
            self.logger.info(f"Result doesn't have any record. Not created any transaction in Delta table")
            return

        write_deltalake(
            path,
            data,
            schema=first_batch.schema,
            storage_options=storage_options,
            mode=mode,
            overwrite_schema=True,
            partition_by=partition_by,
            max_rows_per_file=self.config.get("max_rows_per_file"),
        )

        self.logger.info(f"Transaction has created. Mode: {mode}")

    def _process_endofpipe(self):
        state = copy.deepcopy(self._latest_state)
        self._drain_all(self._sinks_to_clear, 1)

        for sink in self._sinks_to_clear:
            sink.clean_up()

        self._sinks_to_clear = []
        self._drain_all(list(self._sinks_active.values()), self.max_parallelism)

        for sink in self._sinks_active.values():
            sink.clean_up()

        self.logger.info(f"All records saved to disk.")
        self.write_batches_to_delta()

        self._write_state_message(state)
        self._reset_max_record_age()
