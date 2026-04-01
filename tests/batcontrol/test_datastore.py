"""Tests for SQLite-backed persistence."""

import sqlite3
import time

from batcontrol.datastore import DataRecorder
from batcontrol.dynamictariff.baseclass import DynamicTariffBaseclass
from batcontrol.forecastsolar.baseclass import ForecastSolarBaseclass


class DummyTariff(DynamicTariffBaseclass):
    """Minimal tariff provider for persistence tests."""

    def schedule_next_refresh(self) -> None:
        """Disable scheduler side effects for tests."""

    def get_raw_data_from_provider(self) -> dict:
        return {'prices': [0.21, 0.24]}

    def _get_prices_native(self) -> dict[int, float]:
        return {0: 0.21, 1: 0.24}


class DummySolar(ForecastSolarBaseclass):
    """Minimal solar provider for persistence tests."""

    def schedule_next_refresh(self) -> None:
        """Disable scheduler side effects for tests."""

    def get_raw_data_from_provider(self, pvinstallation_name) -> dict:
        return {'name': pvinstallation_name, 'power': [500, 800]}

    def get_forecast_from_raw_data(self) -> dict[int, float]:
        return {0: 500.0, 1: 800.0}


class CountingTariff(DummyTariff):
    """Tariff provider that counts external fetches."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fetch_count = 0

    def get_raw_data_from_provider(self) -> dict:
        self.fetch_count += 1
        return super().get_raw_data_from_provider()


class FailingTariff(CountingTariff):
    """Tariff provider that simulates a fetch failure."""

    def get_raw_data_from_provider(self) -> dict:
        self.fetch_count += 1
        raise ConnectionError('simulated fetch failure')


class CountingSolar(DummySolar):
    """Solar provider that counts external fetches."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fetch_count = 0

    def get_raw_data_from_provider(self, pvinstallation_name) -> dict:
        self.fetch_count += 1
        return super().get_raw_data_from_provider(pvinstallation_name)


def test_data_recorder_persists_rows(tmp_path):
    """Recorder should store source updates and calculation runs."""
    db_path = tmp_path / 'batcontrol.sqlite3'
    recorder = DataRecorder(str(db_path))

    recorder.record_source_update(
        source_type='prices',
        provider='DummyTariff',
        raw_data={'prices': [0.21]},
        normalized_data={0: 0.21},
    )
    recorder.record_calculation(
        created_at_ts=1234.0,
        mode=10,
        charge_rate_w=0,
        soc_percent=66.5,
        stored_energy_wh=6500,
        reserved_energy_wh=2100,
        free_capacity_wh=3000,
        prices=[0.21],
        production=[400],
        consumption=[600],
        net_consumption=[200],
        history_forecast_metrics={
            'predicted_production_w': 400,
            'predicted_consumption_w': 600,
        },
        actual_metrics={
            'actual_production_w': 500,
            'actual_consumption_w': 450,
            'actual_grid_w': -150,
        },
    )

    with sqlite3.connect(str(db_path)) as connection:
        source_count = connection.execute(
            "SELECT COUNT(*) FROM source_updates"
        ).fetchone()[0]
        calc_count = connection.execute(
            "SELECT COUNT(*) FROM calculation_runs"
        ).fetchone()[0]

    assert source_count == 1
    assert calc_count == 1


def test_data_recorder_can_query_snapshot_and_timeline(tmp_path):
    """Recorder should return timeline and historical snapshots for the dashboard."""
    db_path = tmp_path / 'timeline.sqlite3'
    recorder = DataRecorder(str(db_path))

    recorder.record_calculation(
        created_at_ts=1000.0,
        mode=10,
        charge_rate_w=0,
        soc_percent=50.0,
        stored_energy_wh=5000,
        reserved_energy_wh=2000,
        free_capacity_wh=3000,
        prices=[0.21],
        production=[400],
        consumption=[600],
        net_consumption=[200],
        history_forecast_metrics={
            'predicted_production_w': 400,
            'predicted_consumption_w': 600,
        },
        actual_metrics={
            'actual_production_w': 900,
            'actual_consumption_w': 300,
            'actual_grid_w': 120,
        },
    )
    recorder.record_calculation(
        created_at_ts=2000.0,
        mode=-1,
        charge_rate_w=1000,
        soc_percent=60.0,
        stored_energy_wh=6000,
        reserved_energy_wh=2500,
        free_capacity_wh=2500,
        prices=[0.18],
        production=[500],
        consumption=[550],
        net_consumption=[50],
        history_forecast_metrics={
            'predicted_production_w': 500,
            'predicted_consumption_w': 550,
        },
    )

    timeline = recorder.get_calculation_timeline()
    snapshot = recorder.get_calculation_snapshot(1500.0)
    history = recorder.get_history_series()

    assert len(timeline) == 2
    assert snapshot['created_at_ts'] == 1000.0
    assert snapshot['prices'] == [0.21]
    assert snapshot['actual_production_w'] == 900
    assert len(history) == 2
    assert history[0]['actual_production'] == 900
    assert history[0]['actual_consumption'] == 300
    assert history[0]['actual_grid'] == 120
    assert history[0]['predicted_production'] == 400
    assert history[0]['predicted_consumption'] == 600
    assert history[1]['actual_production'] is None
    assert history[1]['predicted_production'] == 500


def test_history_series_converts_interval_wh_to_watts(tmp_path):
    """History fallback should convert stored interval energy to W."""
    db_path = tmp_path / 'history-conversion.sqlite3'
    recorder = DataRecorder(str(db_path))

    recorder.record_calculation(
        created_at_ts=1000.0,
        mode=10,
        charge_rate_w=0,
        soc_percent=50.0,
        stored_energy_wh=5000,
        reserved_energy_wh=2000,
        free_capacity_wh=3000,
        prices=[0.21],
        production=[250],
        consumption=[125],
        net_consumption=[-125],
        metadata={'interval_minutes': 15},
    )

    history = recorder.get_history_series()

    assert history[0]['predicted_production'] == 1000.0
    assert history[0]['predicted_consumption'] == 500.0


def test_tariff_refresh_records_source_update(tmp_path):
    """Tariff refresh should write one source update when a recorder is attached."""
    db_path = tmp_path / 'tariff.sqlite3'
    recorder = DataRecorder(str(db_path))
    tariff = DummyTariff(
        timezone=None,
        min_time_between_API_calls=900,
        delay_evaluation_by_seconds=0,
        target_resolution=60,
        native_resolution=60,
    )
    tariff.set_data_recorder(recorder)

    tariff.refresh_data()

    with sqlite3.connect(str(db_path)) as connection:
        row = connection.execute(
            "SELECT source_type, provider FROM source_updates"
        ).fetchone()

    assert row == ('prices', 'DummyTariff')


def test_solar_refresh_records_source_update(tmp_path):
    """Solar refresh should write one source update when a recorder is attached."""
    db_path = tmp_path / 'solar.sqlite3'
    recorder = DataRecorder(str(db_path))
    solar = DummySolar(
        pvinstallations=[{'name': 'Roof'}],
        timezone=None,
        min_time_between_API_calls=900,
        delay_evaluation_by_seconds=0,
        target_resolution=60,
        native_resolution=60,
    )
    solar.set_data_recorder(recorder)

    solar.refresh_data()

    with sqlite3.connect(str(db_path)) as connection:
        row = connection.execute(
            "SELECT source_type, provider FROM source_updates"
        ).fetchone()

    assert row == ('solar_forecast', 'DummySolar')


def test_tariff_restores_fresh_persisted_source_update(tmp_path):
    """Fresh persisted price data should be restored into the runtime cache."""
    db_path = tmp_path / 'tariff-restore.sqlite3'
    recorder = DataRecorder(str(db_path))
    created_at_ts = time.time()
    recorder.record_source_update(
        source_type='prices',
        provider='CountingTariff',
        raw_data={'prices': [0.21, 0.24]},
        normalized_data={0: 0.21, 1: 0.24},
        created_at_ts=created_at_ts,
    )

    tariff = CountingTariff(
        timezone=None,
        min_time_between_API_calls=900,
        delay_evaluation_by_seconds=0,
        target_resolution=60,
        native_resolution=60,
    )

    tariff.set_data_recorder(recorder)

    assert tariff.get_raw_data() == {'prices': [0.21, 0.24]}
    assert tariff.fetch_count == 0
    assert tariff.next_update_ts == created_at_ts + 900

    tariff.refresh_data()

    assert tariff.fetch_count == 0


def test_solar_restores_fresh_persisted_source_update(tmp_path):
    """Fresh persisted solar data should be restored into all runtime caches."""
    db_path = tmp_path / 'solar-restore.sqlite3'
    recorder = DataRecorder(str(db_path))
    created_at_ts = time.time()
    recorder.record_source_update(
        source_type='solar_forecast',
        provider='CountingSolar',
        raw_data={'Roof': {'name': 'Roof', 'power': [500, 800]}},
        normalized_data={0: 500.0, 1: 800.0},
        metadata={'installations': ['Roof']},
        created_at_ts=created_at_ts,
    )

    solar = CountingSolar(
        pvinstallations=[{'name': 'Roof'}],
        timezone=None,
        min_time_between_api_calls=900,
        delay_evaluation_by_seconds=0,
        target_resolution=60,
        native_resolution=60,
    )

    solar.set_data_recorder(recorder)

    assert solar.get_raw_data('Roof') == {'name': 'Roof', 'power': [500, 800]}
    assert solar.fetch_count == 0
    assert solar.next_update_ts == created_at_ts + 900

    solar.refresh_data()

    assert solar.fetch_count == 0


def test_tariff_ignores_stale_persisted_source_update(tmp_path):
    """Stale persisted price data should not suppress a fresh provider fetch."""
    db_path = tmp_path / 'tariff-stale.sqlite3'
    recorder = DataRecorder(str(db_path))
    recorder.record_source_update(
        source_type='prices',
        provider='CountingTariff',
        raw_data={'prices': [0.21, 0.24]},
        normalized_data={0: 0.21, 1: 0.24},
        created_at_ts=time.time() - 3600,
    )

    tariff = CountingTariff(
        timezone=None,
        min_time_between_API_calls=900,
        delay_evaluation_by_seconds=0,
        target_resolution=60,
        native_resolution=60,
    )

    tariff.set_data_recorder(recorder)
    tariff.refresh_data()

    assert tariff.fetch_count == 1


def test_tariff_uses_stale_persisted_source_update_as_fetch_failure_fallback(tmp_path):
    """Fetch failures should fall back to the latest persisted tariff snapshot."""
    db_path = tmp_path / 'tariff-fallback.sqlite3'
    recorder = DataRecorder(str(db_path))
    stale_created_at_ts = time.time() - 3600
    recorder.record_source_update(
        source_type='prices',
        provider='FailingTariff',
        raw_data={'prices': [0.21, 0.24]},
        normalized_data={0: 0.21, 1: 0.24},
        created_at_ts=stale_created_at_ts,
    )

    tariff = FailingTariff(
        timezone=None,
        min_time_between_API_calls=900,
        delay_evaluation_by_seconds=0,
        target_resolution=60,
        native_resolution=60,
    )

    tariff.set_data_recorder(recorder)
    assert tariff.fetch_count == 0

    tariff.refresh_data()

    assert tariff.fetch_count == 1
    assert tariff.get_raw_data() == {'prices': [0.21, 0.24]}
    assert tariff.next_update_ts > time.time()
