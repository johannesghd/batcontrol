"""Tests for the Solcast hobbyist provider."""

from datetime import datetime
from unittest.mock import MagicMock, patch

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


def test_solcast_hobbyist_aggregates_two_resources_to_hourly():
    """Two rooftop resources should be summed into one hourly W forecast."""
    timezone = pytz.timezone('Europe/Berlin')
    provider = SolcastHobbyist(
        pvinstallations=[
            {'name': 'Roof East', 'resource_id': 'east', 'api_key': 'secret'},
            {'name': 'Roof West', 'resource_id': 'west', 'api_key': 'secret'},
        ],
        timezone=timezone,
        min_time_between_api_calls=900,
        target_resolution=60,
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

    assert forecast[0] == 1000
    assert forecast[1] == 1400


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
