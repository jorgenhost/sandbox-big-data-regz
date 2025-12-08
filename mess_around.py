# %%
import polars as pl
import polars.selectors as cs

# %%
lf = pl.scan_parquet('nyc-taxi/**/*.pq', hive_partitioning=True)

def ref_level(col: str, level: int) -> pl.Expr:

    out = pl.when(pl.col(col) == level).then(pl.lit(-999)).otherwise(pl.col(col))
    return out


#%%
(lf
    .head(40_000_000)
    .select(
        pl.col("VendorID", "month", "year", "passenger_count", "fare_amount", "tip_amount"), 
        cs.starts_with("week"))
    .with_columns(
        passenger_cat = pl.col("passenger_count").qcut(4, allow_duplicates=True),
        month = ref_level('month', 2),
        weekday = ref_level('weekday', 5)
    )
    .with_columns(temp_col = pl.col("passenger_cat").to_physical()*pl.col("week")).sink_parquet('temp.pq')
)