import pytest
import pandas as pd
import sqlite3

# Connect to database
conn = sqlite3.connect("translink_test.db")
print(pd.read_sql("SELECT * FROM trips_raw", conn))

# Create raw dataset
raw_data = pd.DataFrame({
    "trip_id": [1, 2, 3, 4, 5],
    "passenger_id": [101, 102, 103, 104, 105],
    "trip_distance_km": [5.2, 3.1, 4.0, 8.5, 2.0],
    "fare": [3.25, 3.25, 3.25, 3.25, 3.25]
})

# Load into SQL
raw_data.to_sql("trips_raw", conn, if_exists="replace", index=False)

print(pd.read_sql("SELECT * FROM trips_raw", conn))
# -----------------------------
# 1. Row Count Validation
# -----------------------------


def test_row_count():
    sql_count = pd.read_sql(
        "SELECT COUNT(*) AS count FROM trips_raw", conn
    )["count"][0]

    assert sql_count == len(raw_data)


# -----------------------------
# 2. NULL Value Detection
# -----------------------------
def test_null_values():
    df = pd.read_sql("""
    SELECT *
    FROM trips_raw
    WHERE trip_distance_km IS NULL
       OR fare IS NULL
    """, conn)

    assert len(df) == 0


# -----------------------------
# 3. Duplicate Detection
# -----------------------------
def test_duplicates():
    df = pd.read_sql("""
    SELECT trip_id, COUNT(*) AS count
    FROM trips_raw
    GROUP BY trip_id
    HAVING COUNT(*) > 1
    """, conn)

    assert len(df) == 0


# -----------------------------
# 4. Range Validation
# -----------------------------
def test_invalid_ranges():
    df = pd.read_sql("""
    SELECT *
    FROM trips_raw
    WHERE trip_distance_km <= 0
       OR fare < 0
    """, conn)

    assert len(df) == 0


# -----------------------------
# 5. Business Logic Validation
# -----------------------------
def test_cost_per_km():
    df = pd.read_sql("""
    SELECT trip_id,
           fare / trip_distance_km AS cost_per_km
    FROM trips_raw
    WHERE trip_distance_km IS NOT NULL
      AND fare IS NOT NULL
    """, conn)

    # cost per km should be reasonable
    assert (df["cost_per_km"] < 10).all()
