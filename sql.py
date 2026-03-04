import sqlite3

conn = sqlite3.connect(":memory:")
processed_data.to_sql("trips", conn, index=False)
