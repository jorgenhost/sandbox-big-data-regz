# %%
import ibis
import polars as pl
from datetime import date
import math

def make_lf(n_rows: int = 3) -> pl.LazyFrame:
    base = pl.LazyFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "scores": [[10, 20], [30, 40], [50, 60]],
            "tags": [["x", "y"], ["y", "z"], ["x", "z"]],
            "active": [True, False, True],
        }
    )

    # base has 3 rows; we know this statically
    base_rows = 3
    reps = math.ceil(n_rows / base_rows)

    # Lazy "repetition index" frame: 0..reps-1
    rep_idx = pl.LazyFrame({"rep": list(range(reps))})

    # Cross join by adding a dummy column on both sides
    lf_repeated = (
        base.with_columns(pl.lit(1).alias("_k"))
        .join(rep_idx.with_columns(pl.lit(1).alias("_k")), on="_k", how="inner")
        .drop("_k", "rep")
    )

    # Trim to exactly n_rows, still lazy
    return lf_repeated.head(n_rows)

df = make_lf(50_000_000)
df.sink_parquet("data.pq")

# %%

# 1. Set up connection with resource limits
con = ibis.duckdb.connect()
con.raw_sql('SET memory_limit = "1GB"')
con.raw_sql("SET threads TO 2")

# 2. Read the data
t = con.read_parquet('data.pq')

# 3. Define columns to unnest
nested_cols = ['scores', 'tags']

# 4. Get remaining columns dynamically (everything except nested ones)
remaining_cols = [col for col in t.columns if col not in nested_cols]

# 5. Build and execute the query
result = t.select(
    *[t[col].unnest() for col in nested_cols],  # Unnest the nested columns
    *[t[col] for col in remaining_cols]          # Include remaining columns
)

# 6. Write to parquet
result.to_parquet('data_exploded.pq')

# %%
lf = pl.scan_parquet('data_exploded.pq')
lf.head().collect()

# %%
lf.select(pl.len()).collect()