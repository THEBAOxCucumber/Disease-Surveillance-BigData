"""Validate the extracted disease samples and land them in the S3-compatible raw zone."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from airflow import DAG
from airflow.operators.python import PythonOperator


YEARS = ("2568", "2569")
EXPECTED_FIELDS = {
    "_id", "ชื่อกลุ่มโรค", "อายุ (เต็ม) ปี", "อายุ (เต็ม) เดือน", "อาย (เต็ม) วัน",
    "เพศ", "สถานภาพสมรส", "สัญชาติ", "อาชีพ", "จังหวัด", "อำเภอ/เขต",
    "ตำบล/แขวง", "วันที่เริ่มป่วย", "สภาพผู้ป่วย", "ประเภทผู้ป่วย", "สถานที่รักษา",
}


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
    )


def ensure_bucket():
    client = _s3_client()
    bucket = os.environ["DATA_LAKE_BUCKET"]
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        client.create_bucket(Bucket=bucket)


def land_sample(year: str):
    source = Path(os.environ["RAW_DATA_DIR"]) / "disease" / f"disease_cases_{year}_sample.json"
    if not source.is_file():
        raise FileNotFoundError(f"Required raw input is missing: {source.name}")

    payload = source.read_bytes()
    records = json.loads(payload.decode("utf-8"))
    if not isinstance(records, list) or len(records) != 100:
        raise ValueError(f"{source.name}: expected a JSON array of 100 records")
    if not records or set(records[0]) != EXPECTED_FIELDS:
        raise ValueError(f"{source.name}: source fields do not match the 16-field data contract")
    if any(set(record) != EXPECTED_FIELDS for record in records):
        raise ValueError(f"{source.name}: inconsistent fields between records")

    digest = hashlib.sha256(payload).hexdigest()
    bucket = os.environ["DATA_LAKE_BUCKET"]
    key = f"raw/disease/year={year}/{source.name}"
    client = _s3_client()

    try:
        existing = client.head_object(Bucket=bucket, Key=key)
        existing_digest = existing.get("Metadata", {}).get("sha256")
        if existing_digest != digest:
            raise ValueError(
                f"Raw object already exists with different content: s3://{bucket}/{key}. "
                "Use a new versioned filename/key to preserve raw history."
            )
        print(f"Already landed; checksum unchanged: s3://{bucket}/{key}")
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code not in {"404", "NoSuchKey", "NotFound"}:
            raise
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=payload,
            ContentType="application/json",
            Metadata={"sha256": digest, "record-count": str(len(records)), "source-year-be": year},
        )

    manifest = {
        "dataset": "disease_cases",
        "source": "Data.go.th Data API",
        "year_be": year,
        "filename": source.name,
        "object_key": key,
        "format": "json",
        "record_count": len(records),
        "size_bytes": len(payload),
        "sha256": digest,
        "landed_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    client.put_object(
        Bucket=bucket,
        Key=f"raw/disease/year={year}/_metadata/{source.stem}.manifest.json",
        Body=json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"Landed {len(records)} records: s3://{bucket}/{key}; sha256={digest}")


with DAG(
    dag_id="disease_raw_to_lake",
    description="Validate existing Data.go.th disease samples and land immutable raw objects in S3-compatible storage",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2},
    tags=["data-lake", "raw", "disease"],
) as dag:
    create_bucket = PythonOperator(task_id="ensure_raw_bucket", python_callable=ensure_bucket)
    for data_year in YEARS:
        upload = PythonOperator(
            task_id=f"land_disease_{data_year}",
            python_callable=land_sample,
            op_kwargs={"year": data_year},
        )
        create_bucket >> upload
