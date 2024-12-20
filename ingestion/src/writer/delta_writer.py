import pandas as pd
import logging
from deltalake import write_deltalake

logger = logging.getLogger(__name__)


class DeltaWriter:
    def __init__(self, path):
        self.path = path

    def write(self, data, partition_by=None, mode="overwrite"):
        if data.empty:
            logger.warning("No data to write to Delta Table. Skipping.")
            return

        if "timestamp" not in data.columns:
            logger.error(
                f"Data missing required 'timestamp' field. Available fields: {list(data.columns)}"
            )
            raise ValueError(
                f"Unable to write DeltaTable without 'timestamp' field. Current fields: {list(data.columns)}"
            )

        try:
            logger.info(f"Writing DeltaTable to {self.path} with mode={mode}...")
            write_deltalake(self.path, data, partition_by=partition_by, mode=mode)
            logger.info("DeltaTable written successfully.")
        except Exception as e:
            logger.error(f"Failed to write DeltaTable to {self.path}. Error: {e}")
            raise
