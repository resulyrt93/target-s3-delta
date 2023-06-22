"""s3-delta target sink class, which handles writing streams."""

from __future__ import annotations

import os
import time
from typing import Dict, Optional

from dateutil import parser
from singer_sdk import PluginBase
from singer_sdk.helpers._typing import (
    DatetimeErrorTreatmentEnum,
    get_datelike_property_type,
    handle_invalid_timestamp_in_record,
)
from singer_sdk.sinks import BatchSink
import pandas as pd

from target_s3_delta.common import ExtractMode

NOT_PROPER_DATETIME_FORMAT = "0000-00-00 00:00:00"
TEMP_DATA_DIRECTORY = "/tmp/meltano_temp_data/"
MAX_SIZE_DEFAULT = 50000


class S3DeltaSink(BatchSink):
    """s3-delta target sink class."""

    replication_configs: Optional[Dict] = None

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

        for key in record:
            date_val = record[key]
            if date_val == NOT_PROPER_DATETIME_FORMAT:
                record[key] = None

        context["records"].append(record)

    def _parse_timestamps_in_record(
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
            schema:
            treatment:
        """
        for key in record:
            datelike_type = get_datelike_property_type(schema["properties"][key])
            if datelike_type:
                date_val = record[key]
                try:
                    if record[key] is not None:
                        if date_val == NOT_PROPER_DATETIME_FORMAT:
                            record[key] = None
                        else:
                            date_val = parser.parse(date_val)
                except parser.ParserError as ex:
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
        is_exist = os.path.exists(TEMP_DATA_DIRECTORY)
        if not is_exist:
            os.makedirs(TEMP_DATA_DIRECTORY)

        df = pd.DataFrame(context["records"])
        df.to_parquet(f"{TEMP_DATA_DIRECTORY}{context.get('file_path')}", engine="pyarrow")

        context["records"] = []
