"""s3-delta target sink class, which handles writing streams."""

from __future__ import annotations

from dateutil import parser
from deltalake import write_deltalake
from singer_sdk.helpers._typing import (
    DatetimeErrorTreatmentEnum,
    get_datelike_property_type,
    handle_invalid_timestamp_in_record,
)
from singer_sdk.sinks import BatchSink
import pandas as pd

NOT_PROPER_DATETIME_FORMAT = "0000-00-00 00:00:00"


class S3DeltaSink(BatchSink):
    """s3-delta target sink class."""

    max_size = 10000  # Max records to write in one batch

    def process_record(self, record: dict, context: dict) -> None:
        """Load the latest record from the stream.
        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.
        """
        for key in record:
            date_val = record[key]
            if date_val == NOT_PROPER_DATETIME_FORMAT:
                record[key] = None

        if "records" not in context:
            context["records"] = []

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
        # Sample:
        # ------
        # batch_key = context["batch_id"]
        # context["file_path"] = f"{batch_key}.csv"

    def process_batch(self, context: dict) -> None:
        """Write out any prepped records and return once fully written.

        Args:
            context: Stream partition or context dictionary.
        """
        df = pd.DataFrame(context["records"])

        storage_options = {
            "AWS_ACCESS_KEY_ID": self.config.get("aws_access_key_id"),
            "AWS_SECRET_ACCESS_KEY": self.config.get("aws_secret_access_key"),
            "REGION": self.config.get("aws_region"),
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }

        path = self.config.get("s3_path")

        write_deltalake(
            path,
            df,
            storage_options=storage_options,
            mode="overwrite",
            overwrite_schema=True,
        )

        self.logger.info(f"Uploaded {len(context['records'])} rows.")

        context["records"] = []
