from __future__ import annotations

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

DATA_PATH = os.environ.get("DATA_PATH", "owid-covid-data.csv")

COL_MAP = {
    "location": "country",
    "date": "date",
    "new_cases": "new_cases",
    "new_deaths": "new_deaths",
    "total_cases": "total_cases",
    "population": "population",
}
DATE_COL = "date"

HASH_PARTITIONS = int(os.environ.get("HASH_PARTITIONS", "8"))
RANGE_PARTITIONS = int(os.environ.get("RANGE_PARTITIONS", "8"))

OUTPUT_DIR = os.path.join(os.getcwd(), "spark_output")


def create_spark(app_name: str = "Lab1_Partitioning") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_csv(spark: SparkSession, path: str) -> DataFrame:
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )
    return df


def standardize_columns(df: DataFrame) -> DataFrame:
    for src, dst in COL_MAP.items():
        if src in df.columns and src != dst:
            df = df.withColumnRenamed(src, dst)

    required = {"country", "date"}
    missing = sorted([c for c in required if c not in df.columns])
    if missing:
        raise ValueError(
            f"Missing required columns: {missing}. "
            f"Update COL_MAP to match your dataset columns."
        )

    df = df.withColumn("date", F.to_date(F.col("date")))

    return df


def transformation_pipeline(df: DataFrame) -> DataFrame:
    """
    Example pipeline on the partitioned dataset:
      - filter by date range
      - handle nulls
      - derive year/month
      - aggregate and sort
    """

    df = df.filter((F.col("date") >= F.lit("2021-01-01")) & (F.col("date") <= F.lit("2021-12-31")))

    keep_cols = [c for c in ["country", "date", "new_cases", "new_deaths", "total_cases", "population"] if c in df.columns]
    df = df.select(*keep_cols)

    numeric_cols = [c for c in ["new_cases", "new_deaths", "total_cases", "population"] if c in df.columns]
    fill_map = {c: 0 for c in numeric_cols}
    df = df.na.fill(fill_map)

    df = (
        df.withColumn("year", F.year("date"))
          .withColumn("month", F.month("date"))
          .withColumn("cases_per_100k",
                      F.when(F.col("population") > 0, (F.col("new_cases") / F.col("population")) * 100000)
                       .otherwise(F.lit(None)))
    )

    agg = (
        df.groupBy("country", "year", "month")
          .agg(
              F.sum("new_cases").alias("sum_new_cases"),
              F.sum("new_deaths").alias("sum_new_deaths"),
              F.avg("cases_per_100k").alias("avg_cases_per_100k"),
          )
          .orderBy(F.desc("sum_new_cases"))
    )
    return agg


def main() -> None:
    spark = create_spark()

    print(f"\nReading dataset: {DATA_PATH}")
    raw = load_csv(spark, DATA_PATH)
    df = standardize_columns(raw)

    print("\n=== Dataset preview ===")
    df.show(5, truncate=False)
    print(f"Columns: {df.columns}")
    print(f"Row count: {df.count()}")

    df_hash = df.repartition(HASH_PARTITIONS, F.col("country"))
    print("\n=== Strategy 1: Hash partitioning ===")
    print("Partitions (hash):", df_hash.rdd.getNumPartitions())

    result_hash = transformation_pipeline(df_hash)
    print("\nTop 20 rows (hash partitioned pipeline result):")
    result_hash.show(20, truncate=False)

    out_hash = os.path.join(OUTPUT_DIR, "hash_country_partitioned_parquet")
    (
        result_hash.write.mode("overwrite")
        .partitionBy("country")
        .parquet(out_hash)
    )
    print(f"Saved Strategy 1 output to: {out_hash}")

    df_range = df.repartitionByRange(RANGE_PARTITIONS, F.col("date"))
    print("\n=== Strategy 2: Range partitioning ===")
    print("Partitions (range):", df_range.rdd.getNumPartitions())

    result_range = transformation_pipeline(df_range)
    print("\nTop 20 rows (range partitioned pipeline result):")
    result_range.show(20, truncate=False)

    out_range = os.path.join(OUTPUT_DIR, "range_date_partitioned_parquet")
    (
        result_range.write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(out_range)
    )
    print(f"Saved Strategy 2 output to: {out_range}")

    print("\n=== Execution plan snippets ===")
    print("Hash plan:")
    result_hash.explain(mode="simple")
    print("\nRange plan:")
    result_range.explain(mode="simple")

    spark.stop()


if __name__ == "__main__":
    main()
