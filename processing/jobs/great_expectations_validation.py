"""
great_expectations_validation.py
────────────────────────────────
Bonus validation job that uses a Great Expectations-style expectation suite
against the Iceberg lakehouse tables.

The project keeps this job dependency-free so the Docker demo remains simple:
it writes a GE-compatible suite JSON and validation result JSON under /jobs.

Run with:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /jobs/great_expectations_validation.py
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


OUTPUT_DIR = Path("/jobs")
SUITE_PATH = OUTPUT_DIR / "great_expectations_suite.json"
RESULT_PATH = OUTPUT_DIR / "great_expectations_validation_result.json"


EXPECTATIONS = [
    {
        "expectation_type": "expect_table_row_count_to_be_between",
        "table": "demo.bronze.orders",
        "kwargs": {"min_value": 1},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "table": "demo.bronze.orders",
        "kwargs": {"column": "order_id"},
    },
    {
        "expectation_type": "expect_table_row_count_to_be_between",
        "table": "demo.bronze.user_events",
        "kwargs": {"min_value": 1},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "table": "demo.bronze.user_events",
        "kwargs": {"column": "event_id"},
    },
    {
        "expectation_type": "expect_column_values_to_be_unique",
        "table": "demo.silver.dim_product",
        "kwargs": {"column": "product_id"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "table": "demo.silver.dim_product_pricing_scd",
        "kwargs": {"column": "list_price", "min_value": 0},
    },
    {
        "expectation_type": "expect_compound_columns_to_be_unique",
        "table": "demo.silver.dim_product_pricing_scd",
        "kwargs": {"column_list": ["product_id", "is_current"], "condition": "is_current = true"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "table": "demo.gold.fact_orders",
        "kwargs": {"column": "order_id"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "table": "demo.gold.fact_orders",
        "kwargs": {"column": "unit_price", "min_value": 0, "strict_min": True},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "table": "demo.gold.ml_session_conversion",
        "kwargs": {"column": "converted", "value_set": [0, 1]},
    },
]


def get_spark():
    return SparkSession.builder.appName("GreatExpectationsValidation").getOrCreate()


def write_suite():
    suite = {
        "expectation_suite_name": "amazon_lakehouse_suite",
        "meta": {
            "project": "Amazon Data Lakehouse",
            "format": "Great Expectations compatible",
        },
        "expectations": EXPECTATIONS,
    }
    SUITE_PATH.write_text(json.dumps(suite, indent=2), encoding="utf-8")


def validate_expectation(spark, expectation):
    table = expectation["table"]
    expectation_type = expectation["expectation_type"]
    kwargs = expectation["kwargs"]
    df = spark.table(table)

    if expectation_type == "expect_table_row_count_to_be_between":
        observed = df.count()
        min_value = kwargs.get("min_value")
        max_value = kwargs.get("max_value")
        success = True
        if min_value is not None:
            success = success and observed >= min_value
        if max_value is not None:
            success = success and observed <= max_value
        return success, {"observed_value": observed}

    if expectation_type == "expect_column_values_to_not_be_null":
        column = kwargs["column"]
        unexpected = df.filter(col(column).isNull()).count()
        return unexpected == 0, {"unexpected_count": unexpected}

    if expectation_type == "expect_column_values_to_be_unique":
        column = kwargs["column"]
        duplicates = (
            df.groupBy(column)
            .agg(count("*").alias("n"))
            .filter(col("n") > 1)
            .count()
        )
        return duplicates == 0, {"duplicate_values": duplicates}

    if expectation_type == "expect_column_values_to_be_between":
        column = kwargs["column"]
        min_value = kwargs.get("min_value")
        max_value = kwargs.get("max_value")
        strict_min = kwargs.get("strict_min", False)
        strict_max = kwargs.get("strict_max", False)

        invalid_condition = None
        if min_value is not None:
            invalid_condition = col(column) <= min_value if strict_min else col(column) < min_value
        if max_value is not None:
            max_condition = col(column) >= max_value if strict_max else col(column) > max_value
            invalid_condition = max_condition if invalid_condition is None else invalid_condition | max_condition
        if invalid_condition is None:
            return True, {"unexpected_count": 0}
        invalid = df.filter(invalid_condition)
        unexpected = invalid.count()
        return unexpected == 0, {"unexpected_count": unexpected}

    if expectation_type == "expect_column_values_to_be_in_set":
        column = kwargs["column"]
        unexpected = df.filter(~col(column).isin(kwargs["value_set"])).count()
        return unexpected == 0, {"unexpected_count": unexpected}

    if expectation_type == "expect_compound_columns_to_be_unique":
        condition = kwargs.get("condition")
        column_list = kwargs["column_list"]
        scoped = df.filter(condition) if condition else df
        duplicates = (
            scoped.groupBy(*column_list)
            .agg(count("*").alias("n"))
            .filter(col("n") > 1)
            .count()
        )
        return duplicates == 0, {"duplicate_compound_values": duplicates}

    raise ValueError(f"Unsupported expectation type: {expectation_type}")


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    write_suite()

    sep = "=" * 60
    print(f"\n{sep}")
    print("  Great Expectations Bonus Validation")
    print(f"{sep}\n")

    results = []
    for expectation in EXPECTATIONS:
        success, result = validate_expectation(spark, expectation)
        mark = "✓" if success else "✗"
        status = "PASS" if success else "FAIL"
        print(
            f"  {mark} [{status}] {expectation['table']} "
            f"{expectation['expectation_type']} {expectation['kwargs']}"
        )
        results.append(
            {
                "success": success,
                "expectation_config": expectation,
                "result": result,
            }
        )

    failed = sum(1 for result in results if not result["success"])
    validation_result = {
        "success": failed == 0,
        "run_time": datetime.now(timezone.utc).isoformat(),
        "statistics": {
            "evaluated_expectations": len(results),
            "successful_expectations": len(results) - failed,
            "unsuccessful_expectations": failed,
            "success_percent": round((len(results) - failed) * 100.0 / len(results), 2),
        },
        "results": results,
        "meta": {
            "expectation_suite_name": "amazon_lakehouse_suite",
            "suite_path": str(SUITE_PATH),
        },
    }
    RESULT_PATH.write_text(json.dumps(validation_result, indent=2), encoding="utf-8")

    print(f"\n  Suite written to      : {SUITE_PATH}")
    print(f"  Validation written to : {RESULT_PATH}")
    print(
        f"  Results               : "
        f"{validation_result['statistics']['successful_expectations']}/"
        f"{validation_result['statistics']['evaluated_expectations']} passed"
    )
    print(f"\n{sep}\n")

    spark.stop()

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
