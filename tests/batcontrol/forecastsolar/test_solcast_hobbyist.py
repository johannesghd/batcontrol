"""Tests for the Solcast hobbyist provider."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
import pytz

from batcontrol.forecastsolar.solcast_hobbyist import SolcastHobbyist


@patch('batcontrol.forecastsolar.solcast_hobbyist.requests.get')
def test_solcast_hobbyist_builds_rooftop_site_request(mock_get):
    """Solcast hobbyist provider should call the rooftop sites endpoint with api_key."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {'forecasts': []}
    mock_get.return_value = mock_response

    provider = SolcastHobbyist(
        pvinstallations=[{
            'name': 'Roof East',
            'resource_id': 'abc123',
            'api_key': 'secret',
            'hours': 72,
        }],
        timezone=pytz.timezone('Europe/Berlin'),
        min_time_between_api_calls=900,
        target_resolution=60,
        delay_evaluation_by_seconds=0,
    )

    provider.get_raw_data_from_provider('Roof East')

    assert mock_get.call_args.args[0] == (
        'https://api.solcast.com.au/rooftop_sites/abc123/forecasts'
    )
    assert mock_get.call_args.kwargs['params'] == {
        'format': 'json',
        'api_key': 'secret',
        'hours': 72,
    }


def test_solcast_hobbyist_aggregates_two_resources_to_quarter_hourly():
    """Two rooftop resources should be summed into native 15-minute intervals."""
    timezone = pytz.timezone('Europe/Berlin')
    provider = SolcastHobbyist(
        pvinstallations=[
            {'name': 'Roof East', 'resource_id': 'east', 'api_key': 'secret'},
            {'name': 'Roof West', 'resource_id': 'west', 'api_key': 'secret'},
        ],
        timezone=timezone,
        min_time_between_api_calls=900,
        target_resolution=15,
        delay_evaluation_by_seconds=0,
    )

    provider.cache_list['Roof East'].store_new_entry({
        'forecasts': [
            {'period_end': '2026-03-26T20:30:00+01:00', 'period': 'PT30M', 'pv_estimate': 0.5},
            {'period_end': '2026-03-26T21:00:00+01:00', 'period': 'PT30M', 'pv_estimate': 0.7},
            {'period_end': '2026-03-26T21:30:00+01:00', 'period': 'PT30M', 'pv_estimate': 1.0},
            {'period_end': '2026-03-26T22:00:00+01:00', 'period': 'PT30M', 'pv_estimate': 1.2},
        ]
    })
    provider.cache_list['Roof West'].store_new_entry({
        'forecasts': [
            {'period_end': '2026-03-26T20:30:00+01:00', 'period': 'PT30M', 'pv_estimate': 0.3},
            {'period_end': '2026-03-26T21:00:00+01:00', 'period': 'PT30M', 'pv_estimate': 0.5},
            {'period_end': '2026-03-26T21:30:00+01:00', 'period': 'PT30M', 'pv_estimate': 0.2},
            {'period_end': '2026-03-26T22:00:00+01:00', 'period': 'PT30M', 'pv_estimate': 0.4},
        ]
    })

    with patch('batcontrol.forecastsolar.solcast_hobbyist.datetime') as mock_datetime:
        now = timezone.localize(datetime(2026, 3, 26, 20, 15, 0))
        mock_datetime.datetime.now.return_value = now
        mock_datetime.datetime.fromisoformat = datetime.fromisoformat
        mock_datetime.datetime.fromtimestamp = datetime.fromtimestamp
        mock_datetime.datetime.combine = datetime.combine
        mock_datetime.timedelta = __import__('datetime').timedelta
        mock_datetime.timezone = __import__('datetime').timezone

        forecast = provider.get_forecast_from_raw_data()

    assert forecast[0] == pytest.approx(162.5)
    assert forecast[1] == pytest.approx(237.5)
    assert forecast[2] == pytest.approx(287.5)
    assert forecast[3] == pytest.approx(312.5)
    assert forecast[4] == pytest.approx(287.5)
    assert forecast[5] == pytest.approx(312.5)
    assert forecast[6] == pytest.approx(437.5)
    assert forecast[7] == pytest.approx(362.5)


def test_solcast_hobbyist_centered_split_biases_rising_production_later():
    """30-minute intervals should use both neighbors to produce a smooth rising ramp."""
    split = SolcastHobbyist._split_period_energy_to_quarters(
        period_energy_wh=100,
        period=__import__('datetime').timedelta(minutes=30),
        previous_period_energy_wh=0,
        next_period_energy_wh=200,
    )

    assert split == [37.5, 62.5]


def test_solcast_hobbyist_centered_split_defaults_missing_neighbors_to_zero():
    """Missing neighboring intervals should be treated as 0 Wh."""
    split = SolcastHobbyist._split_period_energy_to_quarters(
        period_energy_wh=100,
        period=__import__('datetime').timedelta(minutes=30),
        previous_period_energy_wh=None,
        next_period_energy_wh=None,
    )

    assert split == [50, 50]


def test_solcast_hobbyist_centered_split_supports_sixty_minute_periods():
    """60-minute periods should be integrated into four 15-minute buckets."""
    split = SolcastHobbyist._split_period_energy_to_quarters(
        period_energy_wh=400,
        period=__import__('datetime').timedelta(minutes=60),
        previous_period_energy_wh=200,
        next_period_energy_wh=600,
    )

    assert split == [81.25, 93.75, 106.25, 118.75]
    assert sum(split) == pytest.approx(400)


def test_solcast_hobbyist_unsupported_periods_fall_back_to_flat_split():
    """Unsupported source periods should be split flat instead of failing."""
    split = SolcastHobbyist._split_period_energy_to_quarters(
        period_energy_wh=180,
        period=__import__('datetime').timedelta(minutes=45),
        previous_period_energy_wh=120,
        next_period_energy_wh=240,
    )

    assert split == [60, 60, 60]


def test_solcast_hobbyist_raises_for_missing_resource_id():
    """resource_id is required for Solcast hobbyist resources."""
    provider = SolcastHobbyist(
        pvinstallations=[{'name': 'Roof East', 'api_key': 'secret'}],
        timezone=pytz.timezone('Europe/Berlin'),
        min_time_between_api_calls=900,
        target_resolution=60,
        delay_evaluation_by_seconds=0,
    )

    try:
        provider.get_raw_data_from_provider('Roof East')
        assert False, 'Expected ValueError for missing resource_id'
    except ValueError as exc:
        assert 'resource_id' in str(exc)


@patch('batcontrol.core.tariff_factory.create_tarif_provider')
@patch('batcontrol.core.inverter_factory.create_inverter')
@patch('batcontrol.core.solar_factory.create_solar_provider')
@patch('batcontrol.core.consumption_factory.create_consumption')
def test_core_promotes_time_resolution_for_subhourly_solar_provider(
        mock_consumption, mock_solar, mock_inverter_factory, mock_tariff):
    """Core should switch to 15-minute logic when the solar provider is sub-hourly."""
    from batcontrol.core import Batcontrol

    mock_inverter = MagicMock()
    mock_inverter.get_max_capacity.return_value = 10000
    mock_inverter_factory.return_value = mock_inverter
    mock_tariff.return_value = MagicMock()
    mock_solar.return_value = MagicMock()
    mock_consumption.return_value = MagicMock()

    config = {
        'timezone': 'Europe/Berlin',
        'time_resolution_minutes': 60,
        'solar_forecast_provider': 'solcast-hobbyist',
        'inverter': {
            'type': 'dummy',
            'max_grid_charge_rate': 5000,
            'max_pv_charge_rate': 3000,
            'min_pv_charge_rate': 100,
        },
        'utility': {
            'type': 'tibber',
            'token': 'test_token',
        },
        'pvinstallations': [],
        'consumption_forecast': {
            'type': 'simple',
            'value': 500,
        },
        'battery_control': {
            'max_charging_from_grid_limit': 0.8,
            'min_price_difference': 0.05,
        },
        'mqtt': {
            'enabled': False,
        }
    }

    bc = Batcontrol(config)

    assert bc.time_resolution == 15
    assert bc.intervals_per_hour == 4
