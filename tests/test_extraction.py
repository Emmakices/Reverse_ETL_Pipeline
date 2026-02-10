"""Tests for extractor.extraction — fetch_events with mocked HTTP."""

from unittest.mock import MagicMock, patch

import pytest
import requests

from extractor.extraction import check_api_health, fetch_events, fetch_page


class TestFetchPage:
    @patch("extractor.extraction.requests.Session")
    def test_returns_json_on_success(self, mock_session_cls):
        session = MagicMock()
        response = MagicMock()
        response.json.return_value = {"data": [{"id": 1}]}
        response.raise_for_status.return_value = None
        session.get.return_value = response

        result = fetch_page(session, "http://test.com/api", {}, {}, 30)
        assert result == {"data": [{"id": 1}]}

    @patch("extractor.extraction.requests.Session")
    def test_raises_on_timeout(self, mock_session_cls):
        from extractor.exceptions import APITimeoutError

        session = MagicMock()
        session.get.side_effect = requests.exceptions.Timeout("timed out")

        with pytest.raises(APITimeoutError):
            fetch_page(session, "http://test.com/api", {}, {}, 30)


class TestFetchEvents:
    @patch("extractor.extraction.create_http_session")
    @patch("extractor.extraction.fetch_page")
    def test_single_page(self, mock_fetch_page, mock_create_session, sample_config):
        mock_create_session.return_value = MagicMock()
        mock_fetch_page.return_value = {
            "data": [
                {"event_time": "2024-01-10T14:30:00", "event_type": "view", "user_id": "1"},
                {"event_time": "2024-01-11T14:30:00", "event_type": "purchase", "user_id": "2"},
            ]
        }

        events = fetch_events(sample_config)
        assert len(events) == 2
        assert events[0]["user_id"] == "1"

    @patch("extractor.extraction.time.sleep")
    @patch("extractor.extraction.create_http_session")
    @patch("extractor.extraction.fetch_page")
    def test_multi_page_pagination(self, mock_fetch_page, mock_create_session, mock_sleep, sample_config):
        sample_config.api_page_size = 2
        mock_create_session.return_value = MagicMock()

        # First page returns full page (size 2), second page returns partial (size 1)
        mock_fetch_page.side_effect = [
            {"data": [{"event_type": "v", "user_id": "1", "event_time": "2024-01-10T00:00:00"},
                       {"event_type": "v", "user_id": "2", "event_time": "2024-01-10T00:00:00"}]},
            {"data": [{"event_type": "v", "user_id": "3", "event_time": "2024-01-10T00:00:00"}]},
        ]

        events = fetch_events(sample_config)
        assert len(events) == 3
        assert mock_fetch_page.call_count == 2

    @patch("extractor.extraction.create_http_session")
    @patch("extractor.extraction.fetch_page")
    def test_empty_page_stops_pagination(self, mock_fetch_page, mock_create_session, sample_config):
        mock_create_session.return_value = MagicMock()
        mock_fetch_page.return_value = {"data": []}

        events = fetch_events(sample_config)
        assert len(events) == 0
        assert mock_fetch_page.call_count == 1

    @patch("extractor.extraction.create_http_session")
    @patch("extractor.extraction.fetch_page")
    def test_list_response_format(self, mock_fetch_page, mock_create_session, sample_config):
        mock_create_session.return_value = MagicMock()
        mock_fetch_page.return_value = [
            {"event_type": "v", "user_id": "1", "event_time": "2024-01-10T00:00:00"},
        ]

        events = fetch_events(sample_config)
        assert len(events) == 1


class TestCheckApiHealth:
    @patch("extractor.extraction.create_http_session")
    def test_healthy_api(self, mock_create_session, sample_config):
        session = MagicMock()
        response = MagicMock()
        response.status_code = 200
        session.get.return_value = response
        mock_create_session.return_value = session

        assert check_api_health(sample_config) is True

    @patch("extractor.extraction.create_http_session")
    def test_unhealthy_api(self, mock_create_session, sample_config):
        session = MagicMock()
        response = MagicMock()
        response.status_code = 500
        session.get.return_value = response
        mock_create_session.return_value = session

        assert check_api_health(sample_config) is False

    @patch("extractor.extraction.create_http_session")
    def test_connection_error(self, mock_create_session, sample_config):
        session = MagicMock()
        session.get.side_effect = requests.exceptions.ConnectionError("refused")
        mock_create_session.return_value = session

        assert check_api_health(sample_config) is False
