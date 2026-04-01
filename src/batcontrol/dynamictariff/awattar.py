"""Awattar Class

This module implements the Awattar API to retrieve dynamic electricity prices.
It inherits from the DynamicTariffBaseclass.

Classes:
    Awattar: A class to interact with the Awattar API and process electricity prices.

Methods:
    __init__(self,
                timezone, country: str,
                price_fees: float,
                price_markup: float,
                vat: float,
                min_time_between_API_calls=0):

        Initializes the Awattar class with the specified parameters.

    get_raw_data_from_provider(self):
        Fetches raw data from the Awattar API.

    _get_prices_native(self):
        Processes the raw data to extract and calculate electricity prices.
"""
import datetime
import html
import json
import logging
import math
import re
import requests
from .baseclass import DynamicTariffBaseclass

logger = logging.getLogger(__name__)
_AUSTRIAN_SNAP_MONTHS = {4, 5, 6, 7, 8, 9}
STEKKER_GRAPH_DATA_RE = re.compile(
    r'data-epex-forecast-graph-data-value="([^"]+)"'
)


class Awattar(DynamicTariffBaseclass):
    """ Implement Awattar API to get dynamic electricity prices
        Inherits from DynamicTariffBaseclass

        Native resolution: 60 minutes (hourly)
        API only provides hourly data, baseclass handles 15-min replication if needed.
    """

    def __init__(self, timezone, country: str, min_time_between_API_calls=0,
                 delay_evaluation_by_seconds=0, target_resolution: int = 60):
        """ Initialize Awattar class with parameters """
        # Awattar only provides hourly data (native_resolution=60)
        super().__init__(
            timezone,
            min_time_between_API_calls,
            delay_evaluation_by_seconds,
            target_resolution=target_resolution,
            native_resolution=60
        )
        country = country.lower()
        if country in ['at', 'de']:
            self.url = f'https://api.awattar.{country}/v1/marketdata'
        else:
            raise RuntimeError(f'[Awattar] Country Code {country} not known')

        self.vat = 0
        self.price_fees = 0
        self.price_markup = 0
        self.snap_fees = None
        self._daily_price_cache = {}
        self._stekker_forecast_cache = {}
        self._stekker_window_cache = {}

    def set_price_parameters(
            self,
            vat: float,
            price_fees: float,
            price_markup: float,
            snap_fees: float = None):
        """ Set the extra price parameters for the tariff calculation """
        self.vat = vat
        self.price_fees = price_fees
        self.price_markup = price_markup
        self.snap_fees = snap_fees

    def get_raw_data_from_provider(self):
        """ Get raw data from Awattar API and return parsed json """
        logger.debug('Requesting price forecast from Awattar API')
        return self._fetch_raw_data(self.url)

    def _fetch_raw_data(self, url: str) -> dict:
        """Fetch raw Awattar data from the given URL."""
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            if response.status_code != 200:
                raise ConnectionError(f'[Awattar] API returned {response}')
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f'[Awattar] API request failed: {e}') from e

        return response.json()

    def _get_prices_native(self) -> dict[int, float]:
        """Get hour-aligned prices at native (60-minute) resolution.

        Returns:
            Dict mapping hour index to price value
            Index 0 = start of current hour
        """
        raw_data = self.get_raw_data()
        data = raw_data['data']
        now = datetime.datetime.now().astimezone(self.timezone)
        # Align to start of current hour
        current_hour_start = now.replace(minute=0, second=0, microsecond=0)
        prices = {}

        for item in data:
            timestamp = datetime.datetime.fromtimestamp(
                item['start_timestamp'] / 1000
            ).astimezone(self.timezone)
            diff = timestamp - current_hour_start
            rel_hour = int(diff.total_seconds() / 3600)
            if rel_hour >= 0:
                end_price = (
                    item['marketprice'] / 1000 * (1 + self.price_markup)
                    + self._get_effective_price_fees(timestamp)
                ) * (1 + self.vat)
                prices[rel_hour] = end_price

        logger.debug(
            'Awattar: Retrieved %d hourly prices (hour-aligned)',
            len(prices)
        )
        prices.update(self._get_extended_day_prices(now, current_hour_start))
        prices.update(self._get_stekker_extension_prices(prices, current_hour_start))
        logger.debug(
            'Awattar: Returning %d hourly prices after extensions',
            len(prices)
        )
        return prices

    def _calculate_end_price(
            self,
            market_price: float,
            timestamp: datetime.datetime = None) -> float:
        """Apply markup, fees and VAT to the Awattar market price."""
        return (
            market_price / 1000 * (1 + self.price_markup)
            + self._get_effective_price_fees(timestamp)
        ) * (1 + self.vat)

    def _get_effective_price_fees(self, timestamp: datetime.datetime = None) -> float:
        """Apply Austrian SNAP fees during the configured summer daytime window."""
        if timestamp is None or self.url.endswith('.de/v1/marketdata') or self.snap_fees is None:
            return self.price_fees

        local_timestamp = timestamp.astimezone(self.timezone)
        if (
            local_timestamp.month not in _AUSTRIAN_SNAP_MONTHS
            or local_timestamp.hour < 10
            or local_timestamp.hour >= 16
        ):
            return self.price_fees

        return self.snap_fees

    def _get_prices_for_date(self, day: datetime.date) -> dict[int, float]:
        """Get all Awattar prices for a specific local day."""
        cached = self._daily_price_cache.get(day)
        now_ts = datetime.datetime.now().timestamp()
        if cached and now_ts - cached['fetched_at_ts'] < self.min_time_between_updates:
            return cached['prices']

        naive_day_start = datetime.datetime.combine(day, datetime.time(0, 0, 0))
        if hasattr(self.timezone, 'localize'):
            day_start = self.timezone.localize(naive_day_start)
        else:
            day_start = naive_day_start.replace(tzinfo=self.timezone)
        start_ts = int(day_start.timestamp() * 1000)
        raw_data = self._fetch_raw_data(f'{self.url}?start={start_ts}')
        prices = {}
        for hour_index, item in enumerate(raw_data.get('data', [])):
            timestamp = day_start + datetime.timedelta(hours=hour_index)
            prices[hour_index] = self._calculate_end_price(
                item['marketprice'],
                timestamp=timestamp,
            )
        self._daily_price_cache[day] = {
            'fetched_at_ts': now_ts,
            'prices': prices,
        }
        return prices

    def _get_extended_day_prices(
            self,
            now: datetime.datetime,
            current_hour_start: datetime.datetime) -> dict[int, float]:
        """Supplement the rolling feed with full local-day price data."""
        extended_prices = {}
        for day_offset in [0, 1]:
            day = now.date() + datetime.timedelta(days=day_offset)
            for hour_index, price in self._get_prices_for_date(day).items():
                timestamp = datetime.datetime.combine(day, datetime.time(hour_index, 0, 0))
                if hasattr(self.timezone, 'localize'):
                    timestamp = self.timezone.localize(timestamp)
                else:
                    timestamp = timestamp.replace(tzinfo=self.timezone)
                rel_hour = int((timestamp - current_hour_start).total_seconds() / 3600)
                if rel_hour >= 0:
                    extended_prices[rel_hour] = price
        return extended_prices

    def _get_stekker_region(self) -> str:
        """Map current Awattar country endpoint to the matching Stekker region."""
        if self.url.endswith('.at/v1/marketdata'):
            return 'AT-3600'
        if self.url.endswith('.de/v1/marketdata'):
            return 'DE-LU-3600'
        raise RuntimeError('No Stekker region mapping configured for current Awattar URL')

    def _fetch_stekker_forecast_page(
            self,
            filter_from: datetime.date,
            filter_to: datetime.date) -> str:
        """Fetch the Stekker HTML page that embeds the forecast payload."""
        params = {
            'advanced_view': '',
            'region': self._get_stekker_region(),
            'filter_from': filter_from.isoformat(),
            'filter_to': filter_to.isoformat(),
            'show_historic_forecasts': 0,
            'unit': 'MWh',
            'commit': 'Save',
        }
        response = requests.get(
            'https://stekker.app/epex-forecast',
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.text

    def _extract_stekker_forecast_traces(self, html_text: str) -> list[dict]:
        """Extract Plotly traces from the embedded Stekker graph payload."""
        match = STEKKER_GRAPH_DATA_RE.search(html_text)
        if match is None:
            raise ValueError('Stekker graph payload not found in HTML response')

        raw_payload = html.unescape(match.group(1))
        traces = json.loads(raw_payload)
        if not isinstance(traces, list):
            raise ValueError('Stekker graph payload is not a JSON list')
        return traces

    def _get_stekker_forecast_for_date(
            self,
            day: datetime.date) -> dict[datetime.datetime, float]:
        """Return Stekker forecast prices for a specific local day as final end prices."""
        cached = self._stekker_forecast_cache.get(day)
        now_ts = datetime.datetime.now().timestamp()
        if cached and now_ts - cached['fetched_at_ts'] < self.min_time_between_updates:
            return cached['prices']

        prices = {}
        all_prices = self._get_stekker_forecast_window(
            filter_from=day - datetime.timedelta(days=1),
            filter_to=day + datetime.timedelta(days=2),
        )
        for local_timestamp, price in all_prices.items():
            if local_timestamp.date() == day:
                prices[local_timestamp] = price

        self._stekker_forecast_cache[day] = {
            'fetched_at_ts': now_ts,
            'prices': prices,
        }
        logger.debug(
            'Awattar: Retrieved %d Stekker forecast prices for %s',
            len(prices),
            day.isoformat(),
        )
        return prices

    def _get_stekker_forecast_window(
            self,
            filter_from: datetime.date,
            filter_to: datetime.date) -> dict[datetime.datetime, float]:
        """Return all hourly Stekker forecast prices in the requested window."""
        cache_key = (filter_from, filter_to)
        cached = self._stekker_window_cache.get(cache_key)
        now_ts = datetime.datetime.now().timestamp()
        if cached and now_ts - cached['fetched_at_ts'] < self.min_time_between_updates:
            return cached['prices']

        html_text = self._fetch_stekker_forecast_page(
            filter_from=filter_from,
            filter_to=filter_to,
        )
        traces = self._extract_stekker_forecast_traces(html_text)
        forecast_trace = next(
            (trace for trace in traces if trace.get('name') == 'Forecast price'),
            None,
        )
        if forecast_trace is None:
            raise ValueError('Stekker forecast trace not found in HTML payload')

        prices = {}
        for timestamp_str, price_eur_per_mwh in zip(
                forecast_trace.get('x', []),
                forecast_trace.get('y', [])):
            if price_eur_per_mwh is None:
                continue
            timestamp = datetime.datetime.fromisoformat(timestamp_str)
            local_timestamp = timestamp.astimezone(self.timezone)
            if local_timestamp.minute != 0 or local_timestamp.second != 0:
                continue
            prices[local_timestamp] = self._calculate_end_price(price_eur_per_mwh)

        self._stekker_window_cache[cache_key] = {
            'fetched_at_ts': now_ts,
            'prices': prices,
        }
        return prices

    def _get_stekker_extension_prices(
            self,
            prices: dict[int, float],
            current_hour_start: datetime.datetime) -> dict[int, float]:
        """Extend the known spot curve with Stekker forecast data after the last hour."""
        if not prices:
            return {}

        last_rel_hour = max(prices.keys())
        last_known_timestamp = current_hour_start + datetime.timedelta(hours=last_rel_hour)

        try:
            stekker_prices = self._get_stekker_forecast_window(
                filter_from=last_known_timestamp.date() - datetime.timedelta(days=1),
                filter_to=last_known_timestamp.date() + datetime.timedelta(days=2),
            )
        except (
                requests.exceptions.RequestException,
                ValueError,
                json.JSONDecodeError) as exc:
            logger.warning('Awattar: Stekker extension unavailable: %s', exc)
            return {}

        extension_prices = {}
        for timestamp, price in stekker_prices.items():
            rel_hour = int((timestamp - current_hour_start).total_seconds() / 3600)
            if rel_hour > last_rel_hour:
                extension_prices[rel_hour] = price

        logger.debug(
            'Awattar: Added %d Stekker extension prices after rel_hour=%d',
            len(extension_prices),
            last_rel_hour,
        )
        return extension_prices

    def get_prices_for_today(self) -> dict[int, float]:
        """Get all available hourly prices for the current local day."""
        return self._get_prices_for_date(
            datetime.datetime.now().astimezone(self.timezone).date()
        )

    def get_prices_for_tomorrow(self) -> dict[int, float]:
        """Get all available hourly prices for the next local day."""
        today = datetime.datetime.now().astimezone(self.timezone).date()
        return self._get_prices_for_date(today + datetime.timedelta(days=1))
