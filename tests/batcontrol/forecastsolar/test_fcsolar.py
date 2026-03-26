"""Tests for Forecast.Solar provider-specific URL handling."""

from unittest.mock import MagicMock, patch

import pytz

from batcontrol.forecastsolar.fcsolar import FCSolar


@patch('batcontrol.forecastsolar.fcsolar.requests.get')
def test_fcsolar_builds_query_with_horizon_and_damping(mock_get):
    """Forecast.Solar URL should include all supported query modifiers."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = '{"message":{"info":{"time":"2026-03-26T12:00:00+00:00"}},"result":{}}'
    mock_get.return_value = mock_response

    provider = FCSolar(
        pvinstallations=[{
            'name': 'Roof',
            'lat': 47.0,
            'lon': 15.0,
            'declination': 10,
            'azimuth': -1,
            'kWp': 12,
            'horizon': '1,2,3',
            'damping': '0.5',
            'damping_morning': '0.7',
            'damping_evening': '0.9',
        }],
        timezone=pytz.timezone('Europe/Berlin'),
        min_time_between_api_calls=900,
        target_resolution=60,
    )

    provider.get_raw_data_from_provider('Roof')

    requested_url = mock_get.call_args.args[0]
    assert 'horizon=1%2C2%2C3' in requested_url
    assert 'damping=0.5' in requested_url
    assert 'damping_morning=0.7' in requested_url
    assert 'damping_evening=0.9' in requested_url


@patch('batcontrol.forecastsolar.fcsolar.requests.get')
def test_fcsolar_accepts_api_key_alias(mock_get):
    """Shared config should work with api_key alias."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = '{"message":{"info":{"time":"2026-03-26T12:00:00+00:00"}},"result":{}}'
    mock_get.return_value = mock_response

    provider = FCSolar(
        pvinstallations=[{
            'name': 'Roof',
            'lat': 47.0,
            'lon': 15.0,
            'declination': 10,
            'azimuth': -1,
            'kWp': 12,
            'api_key': 'abc123',
        }],
        timezone=pytz.timezone('Europe/Berlin'),
        min_time_between_api_calls=900,
        target_resolution=60,
    )

    provider.get_raw_data_from_provider('Roof')

    requested_url = mock_get.call_args.args[0]
    assert requested_url.startswith(
        'https://api.forecast.solar/abc123/estimate/'
    )
