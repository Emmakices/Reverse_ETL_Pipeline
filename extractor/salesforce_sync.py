"""Salesforce sync module for Reverse ETL customer weekly metrics."""

from __future__ import annotations

import os

from simple_salesforce import Salesforce

from extractor.azure_sql import get_sql_connection
from extractor.config import ExtractorConfig
from extractor.exceptions import ConfigurationError
from extractor.logging_utils import get_logger, log_operation

logger = get_logger(__name__)


class SalesforceConfig:
    """Salesforce connection configuration from environment variables."""

    def __init__(self):
        required = ("SF_USERNAME", "SF_PASSWORD", "SF_SECURITY_TOKEN")
        missing = [k for k in required if k not in os.environ]
        if missing:
            raise ConfigurationError(
                f"Missing Salesforce environment variable(s): {', '.join(missing)}",
                details={"missing_vars": missing},
            )
        self.username = os.environ["SF_USERNAME"]
        self.password = os.environ["SF_PASSWORD"]
        self.security_token = os.environ["SF_SECURITY_TOKEN"]
        self.domain = os.environ.get("SF_DOMAIN", "login")  # "login" or "test"
        self.object_name = os.environ.get("SF_OBJECT_NAME", "Customer_Weekly_Metric__c")


def fetch_export_data(config: ExtractorConfig) -> list[dict]:
    """Query vw_salesforce_customer_weekly_export for the current batch week."""
    with log_operation(logger, "fetch_salesforce_export_data"):
        with get_sql_connection(config) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT external_customer_id, external_signal_id,
                       batch_week_start, batch_week_end,
                       total_spend_week, activity_count_week,
                       last_activity_time, customer_tier
                FROM dbo.vw_salesforce_customer_weekly_export
                WHERE batch_week_start = ? AND batch_week_end = ?
                """,
                config.week_start,
                config.week_end,
            )
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        logger.info(f"Fetched {len(rows)} records for Salesforce sync")
        return rows


def push_to_salesforce(records: list[dict], sf_config: SalesforceConfig) -> dict:
    """Push records to Salesforce using bulk API upsert."""
    with log_operation(logger, "salesforce_bulk_upsert", record_count=len(records)):
        sf = Salesforce(
            username=sf_config.username,
            password=sf_config.password,
            security_token=sf_config.security_token,
            domain=sf_config.domain,
        )

        # Map DB columns to Salesforce field names
        sf_records = []
        for rec in records:
            sf_records.append({
                "External_Customer_Id__c": str(rec["external_customer_id"]),
                "External_Signal_Id__c": str(rec["external_signal_id"]),
                "Batch_Week_Start__c": str(rec["batch_week_start"]),
                "Batch_Week_End__c": str(rec["batch_week_end"]),
                "Total_Spend_Week__c": float(rec["total_spend_week"] or 0),
                "Activity_Count_Week__c": int(rec["activity_count_week"] or 0),
                "Last_Activity_Time__c": str(rec["last_activity_time"]) if rec["last_activity_time"] else None,
                "Customer_Tier__c": rec["customer_tier"],
            })

        # Bulk upsert using external_signal_id as the external ID
        result = sf.bulk.__getattr__(sf_config.object_name).upsert(
            sf_records,
            "External_Signal_Id__c",
            batch_size=10000,
        )

        success_count = sum(1 for r in result if r.get("success"))
        error_count = sum(1 for r in result if not r.get("success"))

        summary = {
            "total_records": len(records),
            "success_count": success_count,
            "error_count": error_count,
        }

        if error_count > 0:
            errors = [r for r in result if not r.get("success")][:5]
            logger.warning("Salesforce upsert errors (sample)", extra={"errors": errors})
            summary["sample_errors"] = errors

        logger.info("Salesforce sync complete", extra=summary)
        return summary


def run_salesforce_sync(config: ExtractorConfig) -> dict:
    """Main entry point: fetch from Synapse view, push to Salesforce."""
    sf_config = SalesforceConfig()
    records = fetch_export_data(config)

    if not records:
        logger.info("No records to sync to Salesforce for this week")
        return {"total_records": 0, "success_count": 0, "error_count": 0}

    return push_to_salesforce(records, sf_config)
