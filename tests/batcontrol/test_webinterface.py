"""Tests for the built-in web dashboard helpers."""

import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pytz

from batcontrol.core import Batcontrol, MODE_FORCE_CHARGING
from batcontrol.webinterface import DashboardHistory, align_timestamp, build_forecast_series


def test_align_timestamp_uses_interval_start():
    """Timestamps should snap to the beginning of the active interval."""
    timestamp = 1711367425  # arbitrary
    aligned = align_timestamp(timestamp, 15)

    assert aligned % (15 * 60) == 0
    assert aligned <= timestamp
    assert timestamp - aligned < 15 * 60


def test_dashboard_history_replaces_same_interval_and_prunes(tmp_path):
    """History should upsert within one interval and prune old points."""
    history_file = tmp_path / "history.jsonl"
    history = DashboardHistory(str(history_file), interval_minutes=60, retention_days=1)

    history.record(timestamp=3600, soc=60, production=1000, consumption=700)
    history.record(timestamp=4200, soc=61, production=1100, consumption=650)
    history.record(timestamp=3600 + 2 * 24 * 3600, soc=55, production=900, consumption=800)

    entries = history.get_entries()

    assert len(entries) == 1
    assert entries[0]["timestamp"] == 3600 + 2 * 24 * 3600
    assert entries[0]["soc"] == 55.0


def test_build_forecast_series_aligns_points_to_interval():
    """Forecast series should start at the aligned interval boundary."""
    timezone = pytz.timezone("Europe/Berlin")
    timestamp = timezone.localize(
        datetime.datetime(2026, 3, 25, 10, 23)
    ).timestamp()

    series = build_forecast_series(np.array([100.0, 200.0]), timestamp, 15, timezone)

    assert len(series) == 2
    assert series[0]["timestamp"] == align_timestamp(timestamp, 15)
    assert series[1]["timestamp"] - series[0]["timestamp"] == 15 * 60


@patch('batcontrol.core.tariff_factory.create_tarif_provider')
@patch('batcontrol.core.inverter_factory.create_inverter')
@patch('batcontrol.core.solar_factory.create_solar_provider')
@patch('batcontrol.core.consumption_factory.create_consumption')
def test_batcontrol_dashboard_snapshot(
        mock_consumption,
        mock_solar,
        mock_inverter_factory,
        mock_tariff,
        tmp_path):
    """Snapshot payload should expose forecast and history series."""
    mock_inverter = MagicMock()
    mock_inverter.get_max_capacity.return_value = 10000
    mock_inverter_factory.return_value = mock_inverter
    mock_tariff.return_value = MagicMock()
    mock_solar.return_value = MagicMock()
    mock_consumption.return_value = MagicMock()

    config = {
        'timezone': 'Europe/Berlin',
        'time_resolution_minutes': 60,
        'inverter': {
            'type': 'dummy',
            'max_grid_charge_rate': 5000,
        },
        'utility': {
            'type': 'tibber',
            'token': 'test_token'
        },
        'pvinstallations': [{'name': 'Test PV'}],
        'consumption_forecast': {'type': 'csv', 'csv': {}},
        'battery_control': {
            'max_charging_from_grid_limit': 0.8,
            'min_price_difference': 0.05,
        },
        'mqtt': {'enabled': False},
        'webinterface': {'enabled': False},
    }

    bc = Batcontrol(config)
    try:
        bc.last_run_time = datetime.datetime(2026, 3, 25, 10, 23, tzinfo=pytz.UTC).timestamp()
        bc.last_production = np.array([1200.0, 1500.0])
        bc.last_consumption = np.array([800.0, 900.0])
        bc.last_prices = np.array([0.21, 0.24])
        bc.last_SOC = 67.5
        bc.last_stored_energy = 6700
        bc.last_reserved_energy = 2100
        bc.last_charge_rate = 1800
        bc.last_mode = MODE_FORCE_CHARGING
        bc.dashboard_history = DashboardHistory(
            str(tmp_path / 'dashboard-history.jsonl'),
            interval_minutes=60,
            retention_days=7,
        )
        bc.dashboard_history.record(
            bc.last_run_time,
            soc=bc.last_SOC,
            production=bc.last_production[0],
            consumption=bc.last_consumption[0],
        )

        snapshot = bc.get_dashboard_snapshot()

        assert snapshot['status']['mode_label'] == 'Force charging'
        assert len(snapshot['today']['load_profile']) == 2
        assert len(snapshot['today']['pv_forecast']) == 2
        assert len(snapshot['today']['prices']) == 2
        assert len(snapshot['history']['soc']) == 1
        assert snapshot['history']['production'][0]['value'] == 1200.0
    finally:
        bc.shutdown()
