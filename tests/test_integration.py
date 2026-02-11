"""Integration tests -- full pipeline flow with fake HTTP, real file I/O, mocked DB."""

from __future__ import annotations

import json
import urllib.parse
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
import responses

from extractor.config import ExtractorConfig
from extractor.exceptions import DataQualityError, SchemaValidationError
from extractor.pipeline import (
    run_extract_stage,
    run_load_stage,
    run_pipeline,
    run_quality_gate_stage,
    run_transform_stage,
    set_run_tag,
)
from extractor.transformation import OUTPUT_COLUMNS

pytestmark = pytest.mark.integration

API_URL = "https://fake-api.example.com"
EVENTS_ENDPOINT = f"{API_URL}/datasets/ecom_events"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_event(idx: int) -> dict:
    """Generate a single valid event within the 2024-01-08..14 week range."""
    day = 8 + (idx % 7)
    return {
        "event_time": f"2024-01-{day:02d}T{10 + idx % 12}:00:00",
        "event_type": ["purchase", "view", "cart"][idx % 3],
        "user_id": str(1000 + idx),
        "product_id": f"P{idx:04d}",
        "category_id": f"C{idx % 5:03d}",
        "category_code": f"electronics.item{idx}",
        "brand": ["Samsung", "Nike", "Apple", "Sony"][idx % 4],
        "price": round(10.0 + idx * 5.5, 2),
        "user_session": f"sess-{idx:06d}",
    }


def register_api(events: list[dict]) -> None:
    """Register a single-page API response."""
    responses.add(
        responses.GET,
        EVENTS_ENDPOINT,
        json={"data": events, "meta": {"total": len(events)}},
        status=200,
    )


def register_paginated_api(events: list[dict], page_size: int) -> None:
    """Register a callback-based paginated API response."""

    def callback(request):
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(request.url).query))
        page = int(params.get("page", 1))
        start = (page - 1) * page_size
        page_data = events[start : start + page_size]
        body = json.dumps({"data": page_data, "meta": {"total": len(events)}})
        return (200, {}, body)

    responses.add_callback(
        responses.GET,
        EVENTS_ENDPOINT,
        callback=callback,
        content_type="application/json",
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def integration_config(tmp_path):
    """ExtractorConfig pointing at fake API URL with tmp_path dirs."""
    return ExtractorConfig(
        api_base_url=API_URL,
        api_key="integration-test-key",
        week_start=date(2024, 1, 8),
        week_end=date(2024, 1, 14),
        storage_account=None,
        container=None,
        local_data_dir=tmp_path / "data" / "raw_events",
        local_rejects_dir=tmp_path / "data" / "raw_events_rejects",
        state_dir=tmp_path / "state",
        api_page_size=100,
        api_rate_limit_delay=0.0,
        api_max_retries=1,
        reject_threshold_pct=5.0,
    )


@pytest.fixture(autouse=True)
def mock_db():
    """Patch all Azure SQL DB functions so no real connections are attempted."""
    with (
        patch("extractor.pipeline.db_insert_pipeline_run", return_value="integ-run-id") as m_insert,
        patch("extractor.pipeline.db_stage_start", return_value=1) as m_stage_start,
        patch("extractor.pipeline.db_stage_end") as m_stage_end,
        patch("extractor.pipeline.db_finalize_pipeline_run") as m_finalize,
    ):
        yield {
            "insert": m_insert,
            "stage_start": m_stage_start,
            "stage_end": m_stage_end,
            "finalize": m_finalize,
        }


# ---------------------------------------------------------------------------
# Full pipeline integration tests
# ---------------------------------------------------------------------------

class TestFullPipelineIntegration:
    """Tests that exercise run_pipeline() end-to-end."""

    @responses.activate
    def test_happy_path_full_pipeline(self, integration_config):
        """All valid records -> SUCCESS, correct parquet output."""
        events = [_make_event(i) for i in range(20)]
        register_api(events)

        metrics = run_pipeline(
            config=integration_config,
            skip_if_exists=False,
            upload_to_cloud=False,
        )

        assert metrics.success is True
        assert metrics.run_status == "SUCCESS"
        assert metrics.rows_fetched == 20
        assert metrics.rows_output == 20
        assert metrics.rows_rejected == 0

        # Verify parquet output exists and has correct contents
        output_path = integration_config.local_output_file
        assert output_path.exists()

        df = pd.read_parquet(output_path)
        assert len(df) == 20
        assert list(df.columns) == OUTPUT_COLUMNS
        assert df["batch_week_start"].iloc[0] == "2024-01-08"
        assert df["batch_week_end"].iloc[0] == "2024-01-14"
        assert df["event_type"].str.islower().all()
        assert df["price"].notna().all()
        assert (df["price"] > 0).all()

    @responses.activate
    def test_mixed_valid_and_invalid_records(self, integration_config):
        """19 valid + 1 invalid -> SUCCESS_WITH_REJECTS, rejects parquet written."""
        valid = [_make_event(i) for i in range(19)]
        invalid = [{"event_type": "view", "user_id": "456"}]  # missing event_time
        integration_config.reject_threshold_pct = 10.0
        register_api(valid + invalid)

        metrics = run_pipeline(
            config=integration_config,
            skip_if_exists=False,
            upload_to_cloud=False,
        )

        assert metrics.success is True
        assert metrics.run_status == "SUCCESS_WITH_REJECTS"
        assert metrics.rows_fetched == 20
        assert metrics.rows_output == 19
        assert metrics.rows_rejected == 1
        assert metrics.reject_rate_pct == pytest.approx(5.0)

        # Verify valid parquet
        df_valid = pd.read_parquet(integration_config.local_output_file)
        assert len(df_valid) == 19

        # Verify rejects parquet
        rejects_path = Path(metrics.rejects_file_path)
        assert rejects_path.exists()
        df_rej = pd.read_parquet(rejects_path)
        assert len(df_rej) == 1
        assert "row_index" in df_rej.columns
        assert "batch_week_start" in df_rej.columns

    @responses.activate
    def test_quality_gate_rejects_above_threshold(self, integration_config):
        """Reject rate exceeds threshold -> DataQualityError, but files still written."""
        valid = [_make_event(i) for i in range(19)]
        invalid = [{"event_type": "view", "user_id": "456"}]  # missing event_time
        integration_config.reject_threshold_pct = 3.0  # 5% > 3% -> FAILED
        register_api(valid + invalid)

        with pytest.raises(DataQualityError) as exc_info:
            run_pipeline(
                config=integration_config,
                skip_if_exists=False,
                upload_to_cloud=False,
            )

        assert exc_info.value.details["reject_rate_pct"] == pytest.approx(5.0)
        assert exc_info.value.details["reject_threshold_pct"] == 3.0

        # Files should still be written (load runs before quality gate)
        assert integration_config.local_output_file.exists()
        df = pd.read_parquet(integration_config.local_output_file)
        assert len(df) == 19

    @responses.activate
    def test_empty_api_response(self, integration_config):
        """Empty API response -> SchemaValidationError (0% success rate < 95%)."""
        register_api([])

        with pytest.raises(SchemaValidationError, match="Validation success rate too low"):
            run_pipeline(
                config=integration_config,
                skip_if_exists=False,
                upload_to_cloud=False,
            )

    @responses.activate
    def test_api_pagination(self, integration_config):
        """Multi-page fetch works end-to-end."""
        events = [_make_event(i) for i in range(25)]
        integration_config.api_page_size = 10
        register_paginated_api(events, page_size=10)

        metrics = run_pipeline(
            config=integration_config,
            skip_if_exists=False,
            upload_to_cloud=False,
        )

        assert metrics.rows_fetched == 25
        assert metrics.rows_output == 25

        df = pd.read_parquet(integration_config.local_output_file)
        assert len(df) == 25

        # 10 + 10 + 5 events across 3 pages
        assert len(responses.calls) == 3

    @responses.activate
    def test_skip_if_exists(self, integration_config):
        """Pre-existing output file -> pipeline skips, no API calls."""
        output_file = integration_config.local_output_file
        output_file.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"dummy": [1]}).to_parquet(output_file)

        metrics = run_pipeline(
            config=integration_config,
            skip_if_exists=True,
            upload_to_cloud=False,
        )

        assert metrics.success is True
        assert metrics.rows_fetched == 0
        assert len(responses.calls) == 0


# ---------------------------------------------------------------------------
# Stage runner integration tests
# ---------------------------------------------------------------------------

class TestStageRunnersIntegration:
    """Tests that exercise individual stage runners sequentially (Airflow DAG path)."""

    @responses.activate
    def test_stage_runners_end_to_end(self, integration_config):
        """Run extract -> transform -> load -> quality_gate with real inter-stage files."""
        events = [_make_event(i) for i in range(10)]
        register_api(events)
        set_run_tag("integ-test")

        # Stage 1: Extract
        extract_meta = run_extract_stage(integration_config)
        assert extract_meta["rows_fetched"] == 10
        raw_path = Path(extract_meta["raw_events_path"])
        assert raw_path.exists()
        raw_events = json.loads(raw_path.read_text(encoding="utf-8"))
        assert len(raw_events) == 10

        # Stage 2: Transform
        transform_meta = run_transform_stage(integration_config)
        assert transform_meta["rows_output"] == 10
        assert transform_meta["rows_rejected"] == 0
        assert transform_meta["run_status"] == "SUCCESS"
        validated_path = Path(transform_meta["validated_path"])
        assert validated_path.exists()
        df = pd.read_parquet(validated_path)
        assert len(df) == 10

        # Stage 3: Load
        load_meta = run_load_stage(integration_config, upload_to_cloud=False)
        assert integration_config.local_output_file.exists()
        df_final = pd.read_parquet(integration_config.local_output_file)
        assert len(df_final) == 10
        assert list(df_final.columns) == OUTPUT_COLUMNS

        # Stage 4: Quality Gate
        qg_meta = run_quality_gate_stage(integration_config)
        assert qg_meta["quality_gate"] == "PASSED"
        assert qg_meta["run_status"] == "SUCCESS"
