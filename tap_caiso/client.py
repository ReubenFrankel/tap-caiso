"""REST client handling, including caisoStream base class."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Iterable

import requests
import csv
from io import StringIO
import datetime
from datetime import date
from datetime import timedelta
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class caisoPaginator(BaseAPIPaginator):
    def has_more(self, response):
        
        url = response.request.url

        date = url[:50]
        year = int(date[0:4])
        month = int(date)
        day = int(date[6:8])

        earliest = datetime.datetime(2018, 4, 10)
        current = datetime.datetime(year, month, day)

        if current < earliest:
            return False
        return True

    def get_next(self, response):
        url = response.request.url

        date = url[41:49]
        year = date[0:4]
        month = date[4:6]
        day = date[6:8]

        current = datetime.datetime(year, month, day)
        new = current - timedelta(days=1)
        new = new.strftime("%Y%m%d")

        return new
    
    def __init__(self, *args, **kwargs):
        super().__init__(None, *args, **kwargs)

class caisoStream(RESTStream):
    """caiso stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        # TODO: hardcode a value here, or retrieve it from self.config
        today = date.today()
        yesterday = today - timedelta(days=1)
        y_date = yesterday.strftime("%Y%m%d")
        self.logger.info(url)
        url = f"https://www.caiso.com/outlook/SP/History/{y_date}"
        return url

    records_jsonpath = "$[*]"  # Or override `parse_response`.

    # Set this value or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"  # noqa: S105
    

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")

        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")  # noqa: ERA001
        return headers

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        #today = date.today()
        #yesterday = today - timedelta(days=1)
        #yesterday = yesterday.strftime("%Y%m%d")
        return caisoPaginator()#(date.today()-timedelta(days=1)).strftime("%Y%m%d"))

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def prepare_request(self, context, next_page_token):
        
        prepared_request = super().prepare_request(context, next_page_token)
        
        start_date = self.get_starting_timestamp(context) or self.config["start_date"] or self.default_start_date
        date = next_page_token or start_date.strftime("%Y%m%d")

        base = self.url_base
        no_date = base[:41]
        url = f"{no_date}{date}"

        headers = self.http_headers

        prepared_request.prepare_url(
            url,
            self.get_url_params(context, next_page_token),
        )

        return prepared_request
        
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        data = list(csv.DictReader(StringIO(response.text)))
        yield from extract_jsonpath(self.records_jsonpath, input=data)

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        # TODO: Delete this method if not needed.
        return row
