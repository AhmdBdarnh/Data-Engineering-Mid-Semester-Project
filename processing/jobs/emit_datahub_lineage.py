"""
emit_datahub_lineage.py
───────────────────────
Bonus DataHub lineage job.

Writes a local lineage artifact to /jobs/datahub_lineage.json. If a DataHub
GMS endpoint is configured with DATAHUB_GMS_URL, the same lineage is emitted
as Metadata Change Proposals.

Run with:
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /jobs/emit_datahub_lineage.py

Optional:
    DATAHUB_GMS_URL=http://datahub-gms:8080
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

from pyspark.sql import SparkSession


OUTPUT_PATH = Path("/jobs/datahub_lineage.json")
DATAHUB_GMS_URL = os.getenv("DATAHUB_GMS_URL", "").rstrip("/")
DATA_PLATFORM = "iceberg"
ENV = "PROD"


DATASETS = [
    "demo.bronze.orders",
    "demo.bronze.product_catalog",
    "demo.bronze.product_pricing",
    "demo.bronze.reviews",
    "demo.bronze.user_events",
    "demo.bronze.user_events_stream",
    "demo.silver.dim_product",
    "demo.silver.dim_product_pricing_scd",
    "demo.gold.fact_orders",
    "demo.gold.fact_user_events",
    "demo.gold.ecommerce_summary",
    "demo.gold.ml_session_conversion",
]


LINEAGE = {
    "bronze_ingestion": {
        "inputs": [
            "file.amazon_orders_late_arrivals",
            "file.amazon_product_catalog_static_dimension",
            "file.amazon_product_pricing_scd_type2",
            "file.amazon_reviews_batch_api",
            "file.amazon_user_activity_streaming_events",
        ],
        "outputs": [
            "demo.bronze.orders",
            "demo.bronze.product_catalog",
            "demo.bronze.product_pricing",
            "demo.bronze.reviews",
            "demo.bronze.user_events",
        ],
    },
    "streaming_consumer": {
        "inputs": ["kafka.user_events"],
        "outputs": ["demo.bronze.user_events_stream"],
    },
    "silver_dimensions": {
        "inputs": [
            "demo.bronze.product_catalog",
            "demo.bronze.product_pricing",
        ],
        "outputs": [
            "demo.silver.dim_product",
            "demo.silver.dim_product_pricing_scd",
        ],
    },
    "gold_facts": {
        "inputs": [
            "demo.bronze.orders",
            "demo.bronze.user_events",
            "demo.bronze.reviews",
            "demo.silver.dim_product",
        ],
        "outputs": [
            "demo.gold.fact_orders",
            "demo.gold.fact_user_events",
            "demo.gold.ecommerce_summary",
            "demo.gold.ml_session_conversion",
        ],
    },
}


def get_spark():
    return SparkSession.builder.appName("DataHubLineage").getOrCreate()


def dataset_urn(name):
    if name.startswith("file."):
        return f"urn:li:dataset:(urn:li:dataPlatform:file,{name[5:]},{ENV})"
    if name.startswith("kafka."):
        return f"urn:li:dataset:(urn:li:dataPlatform:kafka,{name[6:]},{ENV})"
    return f"urn:li:dataset:(urn:li:dataPlatform:{DATA_PLATFORM},{name},{ENV})"


def data_flow_urn():
    return "urn:li:dataFlow:(airflow,batch_pipeline,prod)"


def data_job_urn(job_name):
    return f"urn:li:dataJob:({data_flow_urn()},{job_name})"


def mcp(entity_urn, aspect_name, aspect):
    return {
        "proposal": {
            "entityType": entity_urn.split(":")[2],
            "entityUrn": entity_urn,
            "changeType": "UPSERT",
            "aspectName": aspect_name,
            "aspect": {
                "value": json.dumps(aspect),
                "contentType": "application/json",
            },
        }
    }


def build_lineage_artifact(spark):
    tables = {}
    for table in DATASETS:
        try:
            df = spark.table(table)
            tables[table] = {
                "urn": dataset_urn(table),
                "row_count": df.count(),
                "columns": [
                    {"name": field.name, "type": field.dataType.simpleString()}
                    for field in df.schema.fields
                ],
            }
        except Exception as exc:
            tables[table] = {
                "urn": dataset_urn(table),
                "error": str(exc),
            }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "datahub_gms_url": DATAHUB_GMS_URL or None,
        "datasets": tables,
        "jobs": {
            job: {
                "urn": data_job_urn(job),
                "inputs": [dataset_urn(name) for name in spec["inputs"]],
                "outputs": [dataset_urn(name) for name in spec["outputs"]],
            }
            for job, spec in LINEAGE.items()
        },
    }


def build_mcps(artifact):
    proposals = []
    proposals.append(
        mcp(
            data_flow_urn(),
            "dataFlowInfo",
            {
                "name": "batch_pipeline",
                "description": "Amazon Data Lakehouse Airflow batch pipeline",
                "project": "Amazon Data Lakehouse",
            },
        )
    )

    for dataset in artifact["datasets"].values():
        if "columns" not in dataset:
            continue
        proposals.append(
            mcp(
                dataset["urn"],
                "schemaMetadata",
                {
                    "schemaName": dataset["urn"],
                    "platform": f"urn:li:dataPlatform:{DATA_PLATFORM}",
                    "version": 0,
                    "hash": "",
                    "platformSchema": {"com.linkedin.schema.OtherSchema": {"rawSchema": ""}},
                    "fields": [
                        {
                            "fieldPath": column["name"],
                            "type": {"type": {"com.linkedin.schema.StringType": {}}},
                            "nativeDataType": column["type"],
                            "description": "",
                            "recursive": False,
                        }
                        for column in dataset["columns"]
                    ],
                },
            )
        )

    for job_name, job in artifact["jobs"].items():
        proposals.append(
            mcp(
                job["urn"],
                "dataJobInfo",
                {
                    "name": job_name,
                    "type": "COMMAND",
                    "flowUrn": data_flow_urn(),
                    "description": f"Spark job {job_name}",
                },
            )
        )
        proposals.append(
            mcp(
                job["urn"],
                "dataJobInputOutput",
                {
                    "inputDatasets": job["inputs"],
                    "outputDatasets": job["outputs"],
                },
            )
        )

    return proposals


def post_to_datahub(proposals):
    if not DATAHUB_GMS_URL:
        print("  DATAHUB_GMS_URL is not set; wrote local lineage artifact only.")
        return 0

    sent = 0
    endpoint = f"{DATAHUB_GMS_URL}/aspects?action=ingestProposal"
    for proposal in proposals:
        request = Request(
            endpoint,
            data=json.dumps(proposal).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=10) as response:
                if response.status >= 400:
                    raise URLError(f"DataHub returned HTTP {response.status}")
            sent += 1
        except Exception as exc:
            print(f"  WARNING: DataHub emission stopped after {sent} proposals: {exc}")
            return sent
    return sent


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    sep = "=" * 60
    print(f"\n{sep}")
    print("  DataHub Bonus Lineage")
    print(f"{sep}\n")

    artifact = build_lineage_artifact(spark)
    OUTPUT_PATH.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    proposals = build_mcps(artifact)
    sent = post_to_datahub(proposals)

    print(f"  Lineage artifact : {OUTPUT_PATH}")
    print(f"  Jobs described   : {len(artifact['jobs'])}")
    print(f"  Tables described : {len(artifact['datasets'])}")
    print(f"  MCPs prepared    : {len(proposals)}")
    print(f"  MCPs sent        : {sent}")
    print(f"\n{sep}\n")

    spark.stop()
    sys.exit(0)


if __name__ == "__main__":
    main()
