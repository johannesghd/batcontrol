"""Module to get forecast from the Solcast hobbyist rooftop sites API."""

import datetime
import logging
import math
import re

import requests

from .baseclass import ForecastSolarBaseclass, ProviderError, RateLimitException

logger = logging.getLogger(__name__)

_PERIOD_RE = re.compile(r'^PT(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?$')
_SOLCAST_HOBBYIST_DAILY_REQUEST_BUDGET = 8


class SolcastHobbyist(ForecastSolarBaseclass):
    """Provider for Solcast hobbyist rooftop site forecasts."""

    def __init__(self, pvinstallations, timezone, min_time_between_api_calls,
                 delay_evaluation_by_seconds, target_resolution=60) -> None:
        resource_count = max(1, len(pvinstallations))
        refresh_cycles_per_day = max(
            1,
            _SOLCAST_HOBBYIST_DAILY_REQUEST_BUDGET // resource_count,
        )
        recommended_refresh_interval = math.ceil(86400 / refresh_cycles_per_day)
        effective_refresh_interval = max(
            min_time_between_api_calls,
            recommended_refresh_interval,
        )
        if effective_refresh_interval != min_time_between_api_calls:
            logger.info(
                'Solcast hobbyist refresh interval raised from %d to %d seconds '
                'to stay within hobbyist request limits for %d resource(s)',
                min_time_between_api_calls,
                effective_refresh_interval,
                resource_count,
            )

        super().__init__(
            pvinstallations,
            timezone,
            effective_refresh_interval,
            delay_evaluation_by_seconds,
            target_resolution=target_resolution,
            native_resolution=15,
        )

    def get_forecast_from_raw_data(self) -> dict[int, float]:
        """Convert Solcast rooftop site forecasts into 15-minute Wh intervals."""
        results = self.get_all_raw_data()
        prediction = {}

        now = datetime.datetime.now().astimezone(self.timezone)
        current_hour = now.replace(minute=0, second=0, microsecond=0)

        for _, result in results.items():
            previous_interval_energy_wh = None
            for entry in sorted(
                    result.get('forecasts', []),
                    key=lambda item: item.get('period_end', '')):
                pv_estimate_kw = entry.get('pv_estimate')
                period_end_str = entry.get('period_end')
                if pv_estimate_kw is None or period_end_str is None:
                    continue

                period_end = self._parse_timestamp(period_end_str).astimezone(self.timezone)
                period = self._parse_period(entry.get('period', 'PT30M'))
                period_start = period_end - period
                period_energy_wh = pv_estimate_kw * 1000 * (period.total_seconds() / 3600)
                interval_energies_wh = self._split_period_energy_to_quarters(
                    period_energy_wh,
                    period,
                    previous_interval_energy_wh,
                )

                for interval_index, interval_energy_wh in enumerate(interval_energies_wh):
                    interval_start = period_start + datetime.timedelta(minutes=15 * interval_index)
                    rel_interval = int((interval_start - current_hour).total_seconds() / 900)
                    if rel_interval < 0:
                        continue
                    prediction[rel_interval] = prediction.get(rel_interval, 0) + interval_energy_wh

                previous_interval_energy_wh = period_energy_wh

        if not prediction:
            return {}

        max_interval = max(prediction.keys())
        for interval in range(max_interval + 1):
            prediction.setdefault(interval, 0)

        return dict(sorted(prediction.items()))

    def get_raw_data_from_provider(self, pvinstallation_name) -> dict:
        """Get raw rooftop site forecast data from Solcast."""
        unit = None
        for installation in self.pvinstallations:
            if installation['name'] == pvinstallation_name:
                unit = installation
                break

        if unit is None:
            raise RuntimeError(
                f'[SolcastHobbyist] PV Installation {pvinstallation_name} not found'
            )

        resource_id = unit.get('resource_id')
        api_key = unit.get('api_key', unit.get('apikey'))
        hours = unit.get('hours', 168)

        if not resource_id:
            raise ValueError(
                f'No Solcast resource_id provided for installation {pvinstallation_name}'
            )
        if not api_key:
            raise ValueError(
                f'No Solcast api_key provided for installation {pvinstallation_name}'
            )

        url = f'https://api.solcast.com.au/rooftop_sites/{resource_id}/forecasts'
        params = {
            'format': 'json',
            'api_key': api_key,
            'hours': hours,
        }

        logger.info('Requesting Solcast forecast for PV installation %s', pvinstallation_name)
        response = requests.get(url, params=params, timeout=60)

        if response.status_code == 200:
            return response.json()

        if response.status_code == 429:
            retry_after = response.headers.get('Retry-After')
            if retry_after is not None:
                try:
                    retry_seconds = int(retry_after)
                    self.rate_limit_blackout_window_ts = time_now = datetime.datetime.now().timestamp() + retry_seconds
                    logger.warning(
                        'Solcast hobbyist API rate limit exceeded. Retry after %d seconds at %s',
                        retry_seconds,
                        datetime.datetime.fromtimestamp(time_now).astimezone(self.timezone),
                    )
                except ValueError:
                    logger.warning('Solcast Retry-After header was not an integer: %s', retry_after)
            raise RateLimitException('Solcast hobbyist API rate limit exceeded')

        raise ProviderError(
            f'Solcast hobbyist API returned {response.status_code} - {response.text}'
        )

    @staticmethod
    def _parse_timestamp(value: str) -> datetime.datetime:
        """Parse Solcast ISO timestamps including Z suffix."""
        if value.endswith('Z'):
            value = value[:-1] + '+00:00'
        return datetime.datetime.fromisoformat(value)

    @staticmethod
    def _parse_period(value: str) -> datetime.timedelta:
        """Parse simple ISO-8601 duration values used by Solcast."""
        match = _PERIOD_RE.match(value)
        if match is None:
            raise ValueError(f'Unsupported Solcast period value: {value}')
        hours = int(match.group('hours') or 0)
        minutes = int(match.group('minutes') or 0)
        return datetime.timedelta(hours=hours, minutes=minutes)

    @staticmethod
    def _split_period_energy_to_quarters(
            period_energy_wh: float,
            period: datetime.timedelta,
            previous_period_energy_wh: float = None) -> list[float]:
        """Split Solcast period energy into 15-minute buckets.

        For 30-minute periods, use trapezoidal disaggregation with the previous
        period energy as the reference. This biases rising production later in
        the half-hour and falling production earlier, while preserving the
        current half-hour total.
        """
        minutes = int(period.total_seconds() // 60)
        if minutes == 15:
            return [period_energy_wh]
        if minutes != 30:
            quarter_count = max(1, int(period.total_seconds() // 900))
            return [period_energy_wh / quarter_count] * quarter_count

        if previous_period_energy_wh is None:
            previous_period_energy_wh = 0

        first_quarter_wh = max(
            0,
            min(period_energy_wh, (previous_period_energy_wh + period_energy_wh) / 4),
        )
        second_quarter_wh = period_energy_wh - first_quarter_wh
        return [first_quarter_wh, second_quarter_wh]
