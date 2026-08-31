"""Tests standard tap features using the built-in SDK tests library."""

import os
from http import HTTPStatus
from unittest.mock import MagicMock

import pendulum
import pytest
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_facebook.streams import AdAccountsStream, AdsStream
from tap_facebook.streams.ad_insights import EXCLUDED_FIELDS, AdsInsightStream
from tap_facebook.tap import TapFacebook

SAMPLE_CONFIG = {
    "start_date": "2021-03-01T00:00:00Z",
    "access_token": os.environ["TAP_FACEBOOK_ACCESS_TOKEN"],
    "account_id": os.environ["TAP_FACEBOOK_ACCOUNT_ID"],
}

OFFLINE_CONFIG = {
    "start_date": "2021-03-01T00:00:00Z",
    "access_token": "test-token",
    "account_id": "123",
    "api_version": "v25.0",
    "page_size": 100,
    "backoff_max_tries": 2,
    "quota_backoff_seconds": 300,
    "max_days_per_sync": 7,
}

AD_DAILY_REPORT = {
    "name": "ad_daily",
    "level": "ad",
    "breakdowns": [],
    "action_breakdowns": [],
    "time_increment_days": 1,
    "action_attribution_windows_view": "1d_view",
    "action_attribution_windows_click": "7d_click",
    "action_report_time": "mixed",
    "lookback_window": 28,
}


def _insights_stream(bookmark: str) -> AdsInsightStream:
    tap = TapFacebook(config=OFFLINE_CONFIG)
    stream = AdsInsightStream(tap=tap, report_definition=AD_DAILY_REPORT)
    stream.get_starting_replication_key_value = lambda _context: bookmark  # type: ignore[method-assign]
    return stream

TestTapFacebook = get_tap_test_class(
    TapFacebook,
    config=SAMPLE_CONFIG,
    suite_config=SuiteConfig(
        max_records_limit=20,
        ignore_no_records_for_streams=[
            "adlabels",
            "customconversions",
            "customaudiences",
        ],
    ),
)


def test_ads_accounts_post_process():
    row = {"amount_spent": "0", "balance": "1", "min_campaign_group_spend_cap": "2"}

    ads_accounts_stream = AdAccountsStream(tap=TapFacebook(config=SAMPLE_CONFIG))

    post_processed_row = ads_accounts_stream.post_process(row)

    assert post_processed_row["spend_cap"] is None
    assert post_processed_row["amount_spent"] == 0
    assert post_processed_row["balance"] == 1
    assert post_processed_row["min_campaign_group_spend_cap"] == 2


def test_page_size_from_config():
    stream = AdsStream(tap=TapFacebook(config=OFFLINE_CONFIG))
    params = stream.get_url_params(context=None, next_page_token=None)
    assert params["limit"] == 100


def test_backoff_max_tries_from_config():
    stream = AdsStream(tap=TapFacebook(config=OFFLINE_CONFIG))
    assert stream.backoff_max_tries() == 2


def test_quota_error_is_retriable():
    stream = AdsStream(tap=TapFacebook(config=OFFLINE_CONFIG))
    response = MagicMock()
    response.status_code = HTTPStatus.BAD_REQUEST
    response.reason = "Bad Request"
    response.url = "https://graph.facebook.com/v25.0/act_123/ads"
    response.content = (
        b'{"error":{"message":"(#17) User request limit reached",'
        b'"code":17,"error_subcode":2446079}}'
    )
    with pytest.raises(RetriableAPIError):
        stream.validate_response(response)


def test_quota_backoff_seconds_from_config():
    stream = AdsStream(tap=TapFacebook(config=OFFLINE_CONFIG))
    assert stream._quota_backoff_seconds() == 300
    assert stream._backoff_seconds_for_exception(
        RetriableAPIError("too many calls from this ad-account"),
    ) == 300


def test_upstream_excluded_marketing_messages_website_purchase_values():
    # Preserve MeltanoLabs #448 — invalid insights field causes API error #100.
    assert "marketing_messages_website_purchase_values" in EXCLUDED_FIELDS


def test_backfill_mode_when_bookmark_far_behind():
    stream = _insights_stream("2024-01-01")
    assert stream._is_backfill_mode(None) is True
    assert stream._max_days_for_run(None) == 7


def test_steady_state_when_bookmark_is_recent():
    recent = pendulum.today().subtract(days=7).to_date_string()
    stream = _insights_stream(recent)
    assert stream._is_backfill_mode(None) is False
    assert stream._max_days_for_run(None) is None


def test_backfill_start_date_skips_lookback():
    stream = _insights_stream("2024-06-01")
    report_start = stream._get_start_date(None)
    assert report_start == pendulum.parse("2024-06-01").date()


def test_adaccounts_omitted_when_account_id_configured():
    tap = TapFacebook(config=OFFLINE_CONFIG)
    stream_names = {stream.name for stream in tap.discover_streams()}
    assert "adaccounts" not in stream_names


def test_adaccounts_included_without_account_id():
    tap = TapFacebook(
        config={
            "start_date": "2021-03-01T00:00:00Z",
            "access_token": "test-token",
            "api_version": "v25.0",
        },
    )
    stream_names = {stream.name for stream in tap.discover_streams()}
    assert "adaccounts" in stream_names


def test_ads_columns_omit_expensive_fields():
    expensive = {
        "recommendations",
        "tracking_specs",
        "conversion_specs",
        "bid_info",
        "source_ad_id",
    }
    assert expensive.isdisjoint(set(AdsStream.columns))
