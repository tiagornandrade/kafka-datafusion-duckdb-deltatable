from deltalake.writer import write_deltalake
import logging

logger = logging.getLogger(__name__)

def write_delta_table(local_path, data, mode='overwrite'):
    if data.empty:
        logger.warning("No data to write to Delta Table. Skipping.")
        return
    logger.info(f"Writing DeltaTable to {local_path}...")
    write_deltalake(local_path, data, mode=mode)
    logger.info("DeltaTable written successfully.")
