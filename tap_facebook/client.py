"""REST client handling, including facebookStream base class."""

from __future__ import annotations

import abc
import json
import typing as t
from http import HTTPStatus
from urllib.parse import urlparse

import pendulum
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context


def is_quota_error_text(content: str) -> bool:
    """Return True when Meta response body indicates ad-account API quota exhaustion."""
    content_lower = content.lower()
    return (
        "too many calls" in content_lower
        or "request limit reached" in content_lower
        or "2446079" in content_lower
    )


class FacebookStream(RESTStream):
    """facebook stream class."""

    # add account id in the url
    # path and fields will be added to this url in streams.pys

    def _page_size(self) -> int:
        """Graph API page size from config (default 25)."""
        return int(self.config.get("page_size", 25))

    def _quota_backoff_seconds(self) -> int:
        """Seconds to wait after a Meta quota (code 17) response."""
        return int(self.config.get("quota_backoff_seconds", 300))

    @property
    def url_base(self) -> str:
        version: str = self.config["api_version"]
        account_id: str = self.config["account_id"]
        return f"https://graph.facebook.com/{version}/act_{account_id}"

    records_jsonpath = "$.data[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.paging.cursors.after"  # noqa: S105

    tolerated_http_errors: list[int] = []  # noqa: RUF012

    @property
    def authenticator(self) -> BearerTokenAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BearerTokenAuthenticator.create_for_stream(
            self,
            token=self.config["access_token"],
        )

    def get_next_page_token(
        self,
        response: requests.Response,
        previous_token: t.Any | None,  # noqa: ARG002, ANN401
    ) -> t.Any | None:  # noqa: ANN401
        """Return a token for identifying next page or None if no more pages.

        Args:
            response: The HTTP ``requests.Response`` object.
            previous_token: The previous page token value.

        Returns:
            The next pagination token.
        """
        if not self.next_page_token_jsonpath:
            return response.headers.get("X-Next-Page", None)

        all_matches = extract_jsonpath(
            self.next_page_token_jsonpath,
            response.json(),
        )
        return next(iter(all_matches), None)

    def get_url_params(
        self,
        context: Context | None,  # noqa: ARG002
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {"limit": self._page_size()}
        if next_page_token is not None:
            params["after"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key

        return params

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.
        """
        full_path = urlparse(response.url).path
        if response.status_code in self.tolerated_http_errors:
            msg = (
                f"{response.status_code} Tolerated Status Code "
                f"(Reason: {response.reason}) for path: {full_path}"
            )
            self.logger.info(msg)
            return

        if HTTPStatus.BAD_REQUEST <= response.status_code < HTTPStatus.INTERNAL_SERVER_ERROR:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.content!s} (Reason: {response.reason}) for path: {full_path}"
            )
            # Ad-account quota (code 17 / 2446079) is transient: long sleep + retry.
            if response.status_code == HTTPStatus.BAD_REQUEST and is_quota_error_text(
                str(response.content),
            ):
                raise RetriableAPIError(msg, response)

            raise FatalAPIError(msg)

        if response.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.content!s} (Reason: {response.reason}) for path: {full_path}"
            )
            raise RetriableAPIError(msg, response)

    def backoff_wait_generator(self) -> t.Generator[float, None, None]:
        """Long fixed wait for quota; exponential for other retriable errors."""
        return self.backoff_runtime(value=self._backoff_seconds_for_exception)

    def _backoff_seconds_for_exception(self, exception: BaseException) -> int:
        if is_quota_error_text(str(exception)):
            wait = self._quota_backoff_seconds()
            self.logger.warning(
                "Meta ad-account quota hit — sleeping %ss before retry",
                wait,
            )
            return wait
        # Mild expo-ish floor for 5xx / other RetriableAPIError
        return 30

    def backoff_max_tries(self) -> int:
        """The number of attempts before giving up when retrying requests.

        Configurable via ``backoff_max_tries`` (default 2). Raise for overnight
        runs that should sleep through Meta Development-tier quota windows.

        Returns:
            int: limit
        """
        return int(self.config.get("backoff_max_tries", 2))


class IncrementalFacebookStream(FacebookStream, metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def filter_entity(self) -> str:
        """The entity to filter on."""

    def get_url_params(
        self,
        context: Context | None,
        next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {"limit": self._page_size()}
        if next_page_token is not None:
            params["after"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
            ts = pendulum.parse(self.get_starting_replication_key_value(context))  # type: ignore[arg-type]
            params["filtering"] = json.dumps(
                [
                    {
                        "field": f"{self.filter_entity}.{self.replication_key}",
                        "operator": "GREATER_THAN",
                        "value": int(ts.timestamp()),  # type: ignore[union-attr]
                    },
                ],
            )

        return params
