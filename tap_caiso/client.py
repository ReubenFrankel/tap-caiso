"""REST client handling, including caisoStream base class."""

from __future__ import annotations

import csv
import typing as t
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path

import pendulum
import requests
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream

# we use the same date format in multiple places, so let's make it a constant
DATE_FORMAT = "%Y%m%d"

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class caisoPaginator(BaseAPIPaginator):
    def __init__(self, *args, **kwargs):
        super().__init__(None, *args, **kwargs)

    def has_more(self, response):
        # there should be more records to fetch if the date to be requested next is
        # before the current date

        # we cannot include the current date as there is no guarantee the data will be
        # available or complete until the day is up
        return self.get_next(response) < date.today()

    def get_next(self, response):
        # parse the date from the previously requested url and return the next `date`
        # value

        # increment date by one day
        return caisoStream.get_date_from_request_url(response) + timedelta(days=1)


class caisoStream(RESTStream):
    """caiso stream class."""

    # dynamic fallback start date of the last 4 weeks
    fallback_start_date = date.today() - timedelta(weeks=4)

    # templated url string that we populate in `prepare_request`
    url_base = "https://www.caiso.com/outlook/SP/History/{date}"

    def get_new_paginator(self):
        return caisoPaginator()

    def prepare_request(self, context, next_page_token: date | None):
        prepared_request = super().prepare_request(context, next_page_token)

        # resolve the current date in heirachical order
        start_date_str = self.config.get("start_date")
        start_date = (
            t.cast(date, pendulum.parse(start_date_str)) if start_date_str else None
        )
        current_date = (
            next_page_token  # from paginator
            or start_date  # from config
            or self.fallback_start_date  # fallback
        )
        self.logger.info(f"Current date: {current_date}")

        # populate the base url date in the format we want
        url = self.url_base.format(date=current_date.strftime(DATE_FORMAT))
        self.logger.info(f"Current URL: {url}")

        # set the new url for the prepared request
        prepared_request.prepare_url(
            url,
            self.get_url_params(context, next_page_token),
        )

        return prepared_request

    def parse_response(self, response):
        # invalid date does not cause api to return client error - instead, it
        # returns some html page

        # when we do get some csv data back, the response contains a specific content
        # type header - we can use this to check before trying to parse the response
        # text (which could be the afforementioned html page content)
        if response.headers["Content-Type"] != "application/octet-stream":
            raise RuntimeError(f"No CSV data available: {response.request.path_url}")

        # get the current date from the request url
        current_date = self.get_date_from_request_url(response)

        # for a valid response, data comes back as a csv - let's parse it as a list of
        # records, where each element is a mapping of the header keys to the
        # corresponding row values
        for record in csv.DictReader(StringIO(response.text)):
            record["current_date"] = current_date  # set current date as record property
            yield record

    @staticmethod
    def get_date_from_request_url(response: requests.Response):
        # assumes date value is always second-to-last segment of url path
        url_date_str = response.request.path_url.split("/")[-2]
        url_date = datetime.strptime(url_date_str, DATE_FORMAT).date()

        return url_date
