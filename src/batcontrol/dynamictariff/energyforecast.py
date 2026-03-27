"""Energyforecast.de Class

This module implements the energyforecast.de API to retrieve dynamic electricity prices.
It inherits from the DynamicTariffBaseclass.

Classes:
    Energyforecast: A class to interact with the energyforecast.de API
                    and process electricity prices.

Methods:
    __init__(self,
                timezone,
                price_fees: float,
                price_markup: float,
                vat: float,
                min_time_between_API_calls=0):

        Initializes the Energyforecast class with the specified parameters.

    get_raw_data_from_provider(self):
        Fetches raw data from the energyforecast.de API.

    _get_prices_native(self):
        Processes the raw data to extract and calculate electricity prices.
"""
import datetime
import logging
import requests
from .baseclass import DynamicTariffBaseclass

logger = logging.getLogger(__name__)


class Energyforecast(DynamicTariffBaseclass):
    """ Implement energyforecast.de API to get dynamic electricity prices
        Inherits from DynamicTariffBaseclass

        Uses 48-hour forecast window for better day-ahead planning.

        Energyforecast API supports both resolutions:
        - hourly: Hourly prices (60-minute intervals)
        - quarter_hourly: 15-minute prices

        The native resolution is set based on target_resolution to fetch
        data at the optimal granularity from the API.

        Accepted market zones when explicitly configured:
        - DE-LU (default)
        - AT
        - FR
        - NL
        - BE
        - PL
        - DK1
        - DK2
    """

    SUPPORTED_MARKET_ZONES = {
        'DE-LU',
        'AT',
        'FR',
        'NL',
        'BE',
        'PL',
        'DK1',
        'DK2',
    }

    def __init__(self, timezone, token, min_time_between_API_calls=0,
                 delay_evaluation_by_seconds=0, target_resolution: int = 60,
                 market_zone: str = ''):
        """ Initialize Energyforecast class with parameters """
        # Energyforecast API supports both resolutions
        if target_resolution == 15:
            native_resolution = 15
            self.api_resolution = "quarter_hourly"
        else:
            native_resolution = 60
            self.api_resolution = "hourly"

        super().__init__(
            timezone,
            min_time_between_API_calls,
            delay_evaluation_by_seconds,
            target_resolution=target_resolution,
            native_resolution=native_resolution
        )
        self.url = 'https://www.energyforecast.de/api/v1/predictions/next_48_hours'
        self.token = token
        self.market_zone = self._normalize_market_zone(market_zone)
        self.vat = 0
        self.price_fees = 0
        self.price_markup = 0

        logger.info(
            'Energyforecast: Configured to fetch %s data (resolution=%d min, market_zone=%s)',
            self.api_resolution,
            self.native_resolution,
            self.market_zone or '<api-default>',
        )

    @classmethod
    def _normalize_market_zone(cls, market_zone: str) -> str:
        """Normalize and validate the configured Energyforecast market zone."""
        normalized = str(market_zone).strip().upper()
        if normalized == '':
            return ''
        if normalized not in cls.SUPPORTED_MARKET_ZONES:
            supported_zones = ', '.join(sorted(cls.SUPPORTED_MARKET_ZONES))
            raise ValueError(
                f'[Energyforecast] Unsupported market_zone {market_zone!r}. '
                f'Supported zones: {supported_zones}'
            )
        return normalized

    def upgrade_48h_to_96h(self):
        """ During initialization, we can upgrade the forecast if user wants 96h horizon """
        self.url = 'https://www.energyforecast.de/api/v1/predictions/next_96_hours'

    def set_price_parameters(self, vat: float, price_fees: float, price_markup: float):
        """ Set the extra price parameters for the tariff calculation """
        self.vat = vat
        self.price_fees = price_fees
        self.price_markup = price_markup

    def get_raw_data_from_provider(self):
        """ Get raw data from energyforecast.de API and return parsed json """
        logger.debug('Requesting price forecast from energyforecast.de API (resolution=%s)',
                     self.api_resolution)
        if not self.token:
            raise RuntimeError('[Energyforecast] API token is required')
        try:
            # Request base prices without provider-side calculations
            # We apply vat, fees, and markup locally
            params = {
                'resolution': self.api_resolution,
                'token': self.token,
                'vat': 0,
                'fixed_cost_cent': 0
            }
            if self.market_zone:
                params['market_zone'] = self.market_zone
            response = requests.get(self.url, params=params, timeout=30)
            response.raise_for_status()
            if response.status_code != 200:
                raise ConnectionError(f'[Energyforecast] API returned {response}')
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f'[Energyforecast] API request failed: {e}') from e

        response_json = response.json()
        return {'data': response_json}

    def _get_prices_native(self) -> dict[int, float]:
        """Get hour-aligned prices at native resolution.

        Expected API response format:
           data: [
              {
                "start": "2025-11-11T06:00:35.531Z",
                "end": "2025-11-11T06:00:35.531Z",
                "price": 0,
                "price_origin": "string"
              }
            ]

        Returns:
            Dict mapping interval index to price value
            Index 0 = start of current hour
            For 15-min resolution: indices 0-3 represent the current hour
        """
        raw_data = self.get_raw_data()
        data = raw_data.get('data', [])
        now = datetime.datetime.now(self.timezone)
        # Align to start of current hour
        current_hour_start = now.replace(minute=0, second=0, microsecond=0)
        prices = {}

        # Determine interval duration in seconds
        interval_seconds = self.native_resolution * 60

        for item in data:
            # Parse ISO format timestamp
            # Python <3.11 does not support 'Z' (UTC) in fromisoformat(),
            # so we replace it with '+00:00'.
            # Remove this workaround if only supporting Python 3.11+.
            timestamp = datetime.datetime.fromisoformat(
                item['start'].replace('Z', '+00:00')
            ).astimezone(self.timezone)

            diff = timestamp - current_hour_start
            rel_interval = int(diff.total_seconds() / interval_seconds)

            if rel_interval >= 0:
                # Apply fees/markup/vat to the base price
                # The price field should already be in the correct unit (EUR/kWh)
                base_price = item['price']
                end_price = ((base_price * (1 + self.price_markup) + self.price_fees)
                             * (1 + self.vat))
                prices[rel_interval] = end_price

        logger.debug(
            'Energyforecast: Retrieved %d prices at %d-min resolution (hour-aligned)',
            len(prices),
            self.native_resolution
        )
        return prices
