import duckdb


duckdb.sql(
    """
CREATE TABLE raw_user_events AS
SELECT * FROM delta_scan('./database/delta-lake/raw/user-events');
"""
)

print(
    duckdb.sql(
        """
SELECT * FROM raw_user_events;
"""
    )
)
