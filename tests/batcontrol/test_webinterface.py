"""Tests for the built-in web dashboard helpers."""

import datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pytz

from batcontrol.core import Batcontrol, MODE_FORCE_CHARGING, MODE_LIMIT_BATTERY_CHARGE_RATE
from batcontrol.datastore import DataRecorder
from batcontrol.webinterface import align_timestamp, build_forecast_series


def test_align_timestamp_uses_interval_start():
    """Timestamps should snap to the beginning of the active interval."""
    timestamp = 1711367425  # arbitrary
    aligned = align_timestamp(timestamp, 15)

    assert aligned % (15 * 60) == 0
    assert aligned <= timestamp
    assert timestamp - aligned < 15 * 60


def test_data_recorder_history_series_uses_calculation_rows(tmp_path):
    """Dashboard history should be derived from stored calculation runs."""
    db_path = tmp_path / "history.sqlite3"
    recorder = DataRecorder(str(db_path))

    recorder.record_calculation(
        created_at_ts=3600,
        mode=10,
        charge_rate_w=0,
        soc_percent=60,
        stored_energy_wh=6000,
        reserved_energy_wh=1000,
        free_capacity_wh=3000,
        prices=[0.2],
        production=[1000],
        consumption=[700],
        net_consumption=[-300],
        history_forecast_metrics={
            'predicted_production_w': 1000,
            'predicted_consumption_w': 700,
        },
    )

    entries = recorder.get_history_series()

    assert len(entries) == 1
    assert entries[0]["created_at_ts"] == 3600
    assert entries[0]["soc_percent"] == 60
    assert entries[0]["predicted_production"] == 1000
    assert entries[0]["actual_production"] is None


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


def test_energy_flow_projection_handles_signed_grid_power():
    """Projection should produce SOC and signed projected grid power."""
    bc = object.__new__(Batcontrol)
    bc.time_resolution = 60
    projection = Batcontrol._build_energy_flow_projection(
        bc,
        {
            'stored_energy_wh': 800.0,
            'free_capacity_wh': 200.0,
            'reserved_energy_wh': 200.0,
            'mode': 10,
            'charge_rate_w': 0,
            'net_consumption': [-300.0, -100.0, 500.0, 500.0],
        }
    )

    assert projection['soc'] == [80.0, 100.0, 100.0, 50.0]
    assert projection['grid'] == [-100.0, -100.0, 0.0, 200.0]


def test_energy_flow_projection_respects_pv_charge_limit_mode():
    """Projection should cap battery charging when mode 8 stores a PV charge limit."""
    bc = object.__new__(Batcontrol)
    bc.time_resolution = 60
    projection = Batcontrol._build_energy_flow_projection(
        bc,
        {
            'stored_energy_wh': 800.0,
            'free_capacity_wh': 200.0,
            'reserved_energy_wh': 200.0,
            'mode': MODE_LIMIT_BATTERY_CHARGE_RATE,
            'charge_rate_w': 0,
            'net_consumption': [-300.0, -300.0],
            'metadata': {'limit_battery_charge_rate_w': 150.0},
        }
    )

    assert projection['soc'] == [80.0, 95.0]
    assert projection['grid'] == [-150.0, -250.0]


def test_energy_flow_projection_uses_export_target_for_mode_8():
    """Mode 8 export flattening should still charge the battery up to full over time."""
    bc = object.__new__(Batcontrol)
    bc.time_resolution = 60
    bc.config = {'battery_control_expert': {'max_future_grid_export_power': 650}}
    projection = Batcontrol._build_energy_flow_projection(
        bc,
        {
            'stored_energy_wh': 200.0,
            'free_capacity_wh': 800.0,
            'reserved_energy_wh': 0.0,
            'mode': MODE_LIMIT_BATTERY_CHARGE_RATE,
            'charge_rate_w': 0,
            'net_consumption': [-1000.0, -1000.0, -1000.0],
            'metadata': {'limit_battery_charge_rate_w': 0.0},
        }
    )

    assert np.allclose(projection['soc'], [20.0, 55.0, 90.0])
    assert projection['grid'] == [-650.0, -650.0, -900.0]


def test_dashboard_query_limit_scales_with_history_days():
    """Dashboard queries should not truncate multi-day history to 500 rows."""
    bc = object.__new__(Batcontrol)
    bc.dashboard_history_days = 7

    assert bc._get_dashboard_query_limit() > 500
    assert bc._get_dashboard_query_limit() == 3361


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
    mock_inverter.max_pv_charge_rate = 0
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
        bc.data_recorder = DataRecorder(str(tmp_path / 'dashboard.sqlite3'))
        bc.data_recorder.record_source_update(
            source_type='prices',
            provider='DummyTariff',
            raw_data={'prices': [0.21, 0.24]},
            normalized_data={0: 0.21, 1: 0.24},
            created_at_ts=bc.last_run_time,
        )
        bc.data_recorder.record_source_update(
            source_type='solar_forecast',
            provider='DummySolar',
            raw_data={'production': [1200.0, 1500.0]},
            normalized_data={0: 1200.0, 1: 1500.0},
            created_at_ts=bc.last_run_time,
        )
        bc.data_recorder.record_calculation(
            created_at_ts=bc.last_run_time,
            mode=bc.last_mode,
            charge_rate_w=bc.last_charge_rate,
            soc_percent=bc.last_SOC,
            stored_energy_wh=bc.last_stored_energy,
            reserved_energy_wh=bc.last_reserved_energy,
            free_capacity_wh=3200,
            prices=bc.last_prices,
            production=bc.last_production,
            consumption=bc.last_consumption,
            net_consumption=bc.last_consumption - bc.last_production,
            history_forecast_metrics={
                'predicted_production_w': 1200.0,
                'predicted_consumption_w': 800.0,
            },
            actual_metrics={
                'actual_production_w': 1180.0,
                'actual_consumption_w': 760.0,
                'actual_grid_w': -420.0,
            },
        )

        snapshot = bc.get_dashboard_snapshot()

        assert snapshot['status']['mode_label'] == 'Force charging'
        assert snapshot['status']['display_power_label'] == 'Grid charge rate'
        assert snapshot['status']['display_power_w'] == 1800
        assert len(snapshot['today']['load_profile']) == 2
        assert len(snapshot['today']['pv_forecast']) == 2
        assert len(snapshot['today']['prices']) == 2
        assert len(snapshot['today']['predicted_grid_power']) == 2
        assert len(snapshot['timeline']) == 1
        assert len(snapshot['history']['soc']) == 1
        assert snapshot['history']['actual_production'][0]['value'] == 1180.0
        assert snapshot['history']['predicted_production'][0]['value'] == 1200.0
        assert snapshot['history']['actual_grid'][0]['value'] == -420.0
        assert snapshot['sources']['prices']['provider'] == 'DummyTariff'
    finally:
        bc.shutdown()


@patch('batcontrol.core.tariff_factory.create_tarif_provider')
@patch('batcontrol.core.inverter_factory.create_inverter')
@patch('batcontrol.core.solar_factory.create_solar_provider')
@patch('batcontrol.core.consumption_factory.create_consumption')
def test_dashboard_uses_full_price_source_series(
        mock_consumption,
        mock_solar,
        mock_inverter_factory,
        mock_tariff,
        tmp_path):
    """Dashboard prices should come from persisted source updates when longer than the run snapshot."""
    mock_inverter = MagicMock()
    mock_inverter.get_max_capacity.return_value = 10000
    mock_inverter.max_pv_charge_rate = 0
    mock_inverter_factory.return_value = mock_inverter
    mock_tariff.return_value = MagicMock()
    mock_solar.return_value = MagicMock()
    mock_consumption.return_value = MagicMock()

    config = {
        'timezone': 'Europe/Berlin',
        'time_resolution_minutes': 60,
        'inverter': {'type': 'dummy', 'max_grid_charge_rate': 5000},
        'utility': {'type': 'tibber', 'token': 'test_token'},
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
        bc.data_recorder = DataRecorder(str(tmp_path / 'dashboard-prices.sqlite3'))
        bc.data_recorder.record_source_update(
            source_type='prices',
            provider='DummyTariff',
            raw_data={'prices': [0.21, 0.24, 0.27, 0.29]},
            normalized_data={0: 0.21, 1: 0.24, 2: 0.27, 3: 0.29},
            created_at_ts=bc.last_run_time,
        )
        bc.data_recorder.record_calculation(
            created_at_ts=bc.last_run_time,
            mode=bc.last_mode,
            charge_rate_w=bc.last_charge_rate,
            soc_percent=bc.last_SOC,
            stored_energy_wh=bc.last_stored_energy,
            reserved_energy_wh=bc.last_reserved_energy,
            free_capacity_wh=3200,
            prices=bc.last_prices,
            production=bc.last_production,
            consumption=bc.last_consumption,
            net_consumption=bc.last_consumption - bc.last_production,
        )

        snapshot = bc.get_dashboard_snapshot()

        assert len(snapshot['today']['prices']) == 4
        assert snapshot['today']['prices'][3]['value'] == 0.29
    finally:
        bc.shutdown()
