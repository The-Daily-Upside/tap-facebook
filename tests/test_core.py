"""Tests standard tap features using the built-in SDK tests library."""

import os
from http import HTTPStatus
from unittest.mock import MagicMock

import pytest
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_facebook.streams import AdAccountsStream, AdsStream
from tap_facebook.streams.ad_insights import EXCLUDED_FIELDS
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
}

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


def test_ads_columns_omit_expensive_fields():
    expensive = {
        "recommendations",
        "tracking_specs",
        "conversion_specs",
        "bid_info",
        "source_ad_id",
    }
    assert expensive.isdisjoint(set(AdsStream.columns))
