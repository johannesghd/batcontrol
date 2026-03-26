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
import logging
import math
import requests
from .baseclass import DynamicTariffBaseclass

logger = logging.getLogger(__name__)


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
        self._daily_price_cache = {}

    def set_price_parameters(self, vat: float, price_fees: float, price_markup: float):
        """ Set the extra price parameters for the tariff calculation """
        self.vat = vat
        self.price_fees = price_fees
        self.price_markup = price_markup

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
                    item['marketprice'] / 1000 * (1 + self.price_markup) + self.price_fees
                ) * (1 + self.vat)
                prices[rel_hour] = end_price

        logger.debug(
            'Awattar: Retrieved %d hourly prices (hour-aligned)',
            len(prices)
        )
        prices.update(self._get_extended_day_prices(now, current_hour_start))
        logger.debug(
            'Awattar: Returning %d hourly prices after today/tomorrow extension',
            len(prices)
        )
        return prices

    def _calculate_end_price(self, market_price: float) -> float:
        """Apply markup, fees and VAT to the Awattar market price."""
        return (
            market_price / 1000 * (1 + self.price_markup) + self.price_fees
        ) * (1 + self.vat)

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
            prices[hour_index] = self._calculate_end_price(item['marketprice'])
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

    def get_prices_for_today(self) -> dict[int, float]:
        """Get all available hourly prices for the current local day."""
        return self._get_prices_for_date(
            datetime.datetime.now().astimezone(self.timezone).date()
        )

    def get_prices_for_tomorrow(self) -> dict[int, float]:
        """Get all available hourly prices for the next local day."""
        today = datetime.datetime.now().astimezone(self.timezone).date()
        return self._get_prices_for_date(today + datetime.timedelta(days=1))
