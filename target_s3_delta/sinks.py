"""s3-delta target sink class, which handles writing streams."""

from __future__ import annotations

from deltalake import write_deltalake
from singer_sdk.sinks import BatchSink
import pandas as pd


class S3DeltaSink(BatchSink):
    """s3-delta target sink class."""

    max_size = 10000  # Max records to write in one batch

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
