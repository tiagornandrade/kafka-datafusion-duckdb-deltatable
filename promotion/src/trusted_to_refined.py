import duckdb
import pandas as pd
import tabulate
import logging
from datafusion import SessionContext

logger = logging.getLogger(__name__)
ctx = SessionContext()
