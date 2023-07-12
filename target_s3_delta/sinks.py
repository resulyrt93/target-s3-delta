"""s3-delta target sink class, which handles writing streams."""

from __future__ import annotations

import os
from typing import Dict, Optional

from singer_sdk import PluginBase
from singer_sdk.helpers._typing import DatetimeErrorTreatmentEnum
from singer_sdk.sinks import BatchSink
import pandas as pd

from target_s3_delta.common import ExtractMode
from target_s3_delta.utils import float_to_decimal

TEMP_DATA_DIRECTORY = "/tmp/meltano_temp_data/"
MAX_SIZE_DEFAULT = 50000


class S3DeltaSink(BatchSink):
    """s3-delta target sink class."""

    replication_configs: Optional[Dict] = None

    @property
    def datetime_error_treatment(self) -> DatetimeErrorTreatmentEnum:
        return DatetimeErrorTreatmentEnum.NULL

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | None,
    ) -> None:
        super().__init__(
            target=target,
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
        )
        self.state = target._latest_state
        self.set_replication_configs()

    def set_replication_configs(self) -> None:
        """
        Checks state info if it exists it sets replication configurations to filter
        duplicate records in process_record
        """
        if self.mode == ExtractMode.APPEND:
            bookmarks: dict = self.state.get("bookmarks", {})
            if len(bookmarks):
                replication_configs = bookmarks.get(list(bookmarks.keys())[0])
                replication_key = replication_configs.get("replication_key")
                replication_key_value = replication_configs.get("replication_key_value")

                if replication_key is not None and replication_key_value is not None:
                    self.replication_configs = {
                        "replication_key": replication_key,
                        "replication_key_value": replication_key_value,
                    }

    @property
    def mode(self) -> ExtractMode:
        """Get extract mode"""
        return self.config.get("mode")

    @property
    def max_size(self) -> int:
        """Get max batch size.

        Returns:
            Max number of records to batch before `is_full=True`
        """
        return self.config.get("batch_size", MAX_SIZE_DEFAULT)

    def _validate_and_parse(self, record: dict) -> dict:
        """Validate or repair the record, parsing to python-native types as needed."""
        record = float_to_decimal(record)
        return super()._validate_and_parse(record=record)

    def is_duplicate_replication(self, record: dict) -> bool:
        """Whether record is duplicate replication"""
        if self.replication_configs is not None:
            replication_key = self.replication_configs.get("replication_key")
            replication_key_value = self.replication_configs.get("replication_key_value")

            return bool(replication_key in record and record[replication_key] == replication_key_value)
        return False

    def process_record(self, record: dict, context: dict) -> None:
        """Load the latest record from the stream.
        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        if "records" not in context:
            context["records"] = []

        if self.is_duplicate_replication(record):
            # Since singer returns at least one last record in incremental cases, we're truncating it.
            # https://www.stitchdata.com/docs/replication/replication-methods/key-based-incremental
            return

        context["records"].append(record)

    def start_batch(self, context: dict) -> None:
        """Start a batch.

        Developers may optionally add additional markers to the `context` dict,
        which is unique to this batch.

        Args:
            context: Stream partition or context dictionary.
        """
        batch_key = context["batch_id"]
        context["file_path"] = f"{batch_key}.parquet"

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        self.logger.info("Batch processing has started.")
        is_exist = os.path.exists(TEMP_DATA_DIRECTORY)
        if not is_exist:
            os.makedirs(TEMP_DATA_DIRECTORY)

        df = pd.DataFrame(context["records"])
        df.to_parquet(f"{TEMP_DATA_DIRECTORY}{context.get('file_path')}", engine="pyarrow")

        context["records"] = []
        self.logger.info("Batch processing has finished.")
