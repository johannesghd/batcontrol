"""SQLite-backed persistence for source updates and calculation snapshots."""

import json
import logging
import os
import sqlite3
import threading
import time

logger = logging.getLogger(__name__)


class DataRecorder:
    """Persist batcontrol source updates and calculations to SQLite."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._init_db()

    def _connect(self):
        connection = sqlite3.connect(self.db_path, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_db(self) -> None:
        directory = os.path.dirname(self.db_path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS source_updates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at_ts REAL NOT NULL,
                    source_type TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    source_name TEXT,
                    raw_data_json TEXT,
                    normalized_data_json TEXT,
                    metadata_json TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS calculation_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at_ts REAL NOT NULL,
                    mode INTEGER,
                    charge_rate_w INTEGER,
                    soc_percent REAL,
                    stored_energy_wh REAL,
                    reserved_energy_wh REAL,
                    free_capacity_wh REAL,
                    predicted_production_w REAL,
                    predicted_consumption_w REAL,
                    actual_production_w REAL,
                    actual_consumption_w REAL,
                    actual_battery_w REAL,
                    actual_grid_w REAL,
                    actual_inverter_w REAL,
                    actual_secondary_wr_w REAL,
                    prices_json TEXT,
                    production_json TEXT,
                    consumption_json TEXT,
                    net_consumption_json TEXT,
                    metadata_json TEXT
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_source_updates_ts "
                "ON source_updates(created_at_ts)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_calculation_runs_ts "
                "ON calculation_runs(created_at_ts)"
            )
            self._ensure_column(connection, 'calculation_runs', 'predicted_production_w', 'REAL')
            self._ensure_column(connection, 'calculation_runs', 'predicted_consumption_w', 'REAL')
            self._ensure_column(connection, 'calculation_runs', 'actual_production_w', 'REAL')
            self._ensure_column(connection, 'calculation_runs', 'actual_consumption_w', 'REAL')
            self._ensure_column(connection, 'calculation_runs', 'actual_battery_w', 'REAL')
            self._ensure_column(connection, 'calculation_runs', 'actual_grid_w', 'REAL')
            self._ensure_column(connection, 'calculation_runs', 'actual_inverter_w', 'REAL')
            self._ensure_column(connection, 'calculation_runs', 'actual_secondary_wr_w', 'REAL')
            connection.commit()

    @staticmethod
    def _to_json(value) -> str:
        return json.dumps(value, sort_keys=True, default=_json_default)

    @staticmethod
    def _ensure_column(connection, table_name: str, column_name: str, column_type: str) -> None:
        """Add a missing column for forward-compatible schema upgrades."""
        columns = [
            row["name"]
            for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        ]
        if column_name not in columns:
            connection.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
            )

    def record_source_update(
            self,
            source_type: str,
            provider: str,
            raw_data,
            normalized_data=None,
            source_name: str = None,
            metadata: dict = None,
            created_at_ts: float = None) -> None:
        """Persist one source update event."""
        timestamp = created_at_ts if created_at_ts is not None else time.time()
        try:
            with self._lock:
                with self._connect() as connection:
                    connection.execute(
                        """
                        INSERT INTO source_updates (
                            created_at_ts,
                            source_type,
                            provider,
                            source_name,
                            raw_data_json,
                            normalized_data_json,
                            metadata_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            timestamp,
                            source_type,
                            provider,
                            source_name,
                            self._to_json(raw_data),
                            self._to_json(normalized_data),
                            self._to_json(metadata or {}),
                        )
                    )
                    connection.commit()
        except (sqlite3.Error, TypeError) as exc:
            logger.error('Failed to record source update: %s', exc)

    def record_calculation(
            self,
            created_at_ts: float,
            mode: int,
            charge_rate_w: int,
            soc_percent: float,
            stored_energy_wh: float,
            reserved_energy_wh: float,
            free_capacity_wh: float,
            prices,
            production,
            consumption,
            net_consumption,
            history_forecast_metrics: dict = None,
            actual_metrics: dict = None,
            metadata: dict = None) -> None:
        """Persist one completed calculation snapshot."""
        history_forecast_metrics = history_forecast_metrics or {}
        actual_metrics = actual_metrics or {}
        try:
            with self._lock:
                with self._connect() as connection:
                    connection.execute(
                        """
                        INSERT INTO calculation_runs (
                            created_at_ts,
                            mode,
                            charge_rate_w,
                            soc_percent,
                            stored_energy_wh,
                            reserved_energy_wh,
                            free_capacity_wh,
                            predicted_production_w,
                            predicted_consumption_w,
                            actual_production_w,
                            actual_consumption_w,
                            actual_battery_w,
                            actual_grid_w,
                            actual_inverter_w,
                            actual_secondary_wr_w,
                            prices_json,
                            production_json,
                            consumption_json,
                            net_consumption_json,
                            metadata_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            created_at_ts,
                            mode,
                            charge_rate_w,
                            soc_percent,
                            stored_energy_wh,
                            reserved_energy_wh,
                            free_capacity_wh,
                            history_forecast_metrics.get('predicted_production_w'),
                            history_forecast_metrics.get('predicted_consumption_w'),
                            actual_metrics.get('actual_production_w'),
                            actual_metrics.get('actual_consumption_w'),
                            actual_metrics.get('actual_battery_w'),
                            actual_metrics.get('actual_grid_w'),
                            actual_metrics.get('actual_inverter_w'),
                            actual_metrics.get('actual_secondary_wr_w'),
                            self._to_json(prices),
                            self._to_json(production),
                            self._to_json(consumption),
                            self._to_json(net_consumption),
                            self._to_json(metadata or {}),
                        )
                    )
                    connection.commit()
        except (sqlite3.Error, TypeError) as exc:
            logger.error('Failed to record calculation snapshot: %s', exc)

    def get_calculation_timeline(self, since_ts: float = None, limit: int = 500):
        """Return recent calculation runs for dashboard time selection."""
        query = [
            "SELECT created_at_ts, mode, charge_rate_w, soc_percent",
            "FROM calculation_runs",
        ]
        params = []
        if since_ts is not None:
            query.append("WHERE created_at_ts >= ?")
            params.append(since_ts)
        query.append("ORDER BY created_at_ts ASC")
        if limit is not None:
            query.append("LIMIT ?")
            params.append(limit)

        with self._lock:
            with self._connect() as connection:
                rows = connection.execute(" ".join(query), params).fetchall()

        return [dict(row) for row in rows]

    def get_calculation_snapshot(self, at_ts: float = None):
        """Return the latest calculation row or the latest row at/before at_ts."""
        query = [
            "SELECT * FROM calculation_runs",
        ]
        params = []
        if at_ts is not None:
            query.append("WHERE created_at_ts <= ?")
            params.append(at_ts)
        query.append("ORDER BY created_at_ts DESC LIMIT 1")

        with self._lock:
            with self._connect() as connection:
                row = connection.execute(" ".join(query), params).fetchone()

        return self._deserialize_calculation_row(row)

    def get_history_series(self, since_ts: float = None, limit: int = 500):
        """Return compact history series derived from calculation runs."""
        query = [
            """
            SELECT
                created_at_ts,
                soc_percent,
                predicted_production_w,
                predicted_consumption_w,
                actual_production_w,
                actual_grid_w,
                actual_consumption_w,
                metadata_json,
                production_json,
                consumption_json
            FROM calculation_runs
            """
        ]
        params = []
        if since_ts is not None:
            query.append("WHERE created_at_ts >= ?")
            params.append(since_ts)
        query.append("ORDER BY created_at_ts ASC")
        if limit is not None:
            query.append("LIMIT ?")
            params.append(limit)

        with self._lock:
            with self._connect() as connection:
                rows = connection.execute(" ".join(query), params).fetchall()

        entries = []
        for row in rows:
            metadata = self._from_json(row["metadata_json"], {})
            interval_minutes = metadata.get("interval_minutes", 60)
            production = self._from_json(row["production_json"], [])
            consumption = self._from_json(row["consumption_json"], [])
            entries.append({
                "created_at_ts": row["created_at_ts"],
                "soc_percent": row["soc_percent"],
                "predicted_production": (
                    row["predicted_production_w"]
                    if row["predicted_production_w"] is not None
                    else _interval_energy_to_power(
                        production[0] if production else None,
                        interval_minutes,
                    )
                ),
                "predicted_consumption": (
                    row["predicted_consumption_w"]
                    if row["predicted_consumption_w"] is not None
                    else _interval_energy_to_power(
                        consumption[0] if consumption else None,
                        interval_minutes,
                    )
                ),
                "actual_production": row["actual_production_w"],
                "actual_grid": row["actual_grid_w"],
                "actual_consumption": row["actual_consumption_w"],
            })
        return entries

    def get_source_update_snapshot(self, source_type: str, at_ts: float = None):
        """Return latest source update for a source type, optionally at/before a timestamp."""
        query = [
            "SELECT * FROM source_updates WHERE source_type = ?",
        ]
        params = [source_type]
        if at_ts is not None:
            query.append("AND created_at_ts <= ?")
            params.append(at_ts)
        query.append("ORDER BY created_at_ts DESC LIMIT 1")

        with self._lock:
            with self._connect() as connection:
                row = connection.execute(" ".join(query), params).fetchone()

        if row is None:
            return None

        return {
            "created_at_ts": row["created_at_ts"],
            "source_type": row["source_type"],
            "provider": row["provider"],
            "source_name": row["source_name"],
            "raw_data": self._from_json(row["raw_data_json"], {}),
            "normalized_data": self._from_json(row["normalized_data_json"], {}),
            "metadata": self._from_json(row["metadata_json"], {}),
        }

    @staticmethod
    def _from_json(value, default):
        if value is None:
            return default
        return json.loads(value)

    def _deserialize_calculation_row(self, row):
        """Convert a SQLite row into a JSON-friendly snapshot."""
        if row is None:
            return None

        return {
            "created_at_ts": row["created_at_ts"],
            "mode": row["mode"],
            "charge_rate_w": row["charge_rate_w"],
            "soc_percent": row["soc_percent"],
            "stored_energy_wh": row["stored_energy_wh"],
            "reserved_energy_wh": row["reserved_energy_wh"],
            "free_capacity_wh": row["free_capacity_wh"],
            "predicted_production_w": row["predicted_production_w"],
            "predicted_consumption_w": row["predicted_consumption_w"],
            "actual_production_w": row["actual_production_w"],
            "actual_consumption_w": row["actual_consumption_w"],
            "actual_battery_w": row["actual_battery_w"],
            "actual_grid_w": row["actual_grid_w"],
            "actual_inverter_w": row["actual_inverter_w"],
            "actual_secondary_wr_w": row["actual_secondary_wr_w"],
            "prices": self._from_json(row["prices_json"], []),
            "production": self._from_json(row["production_json"], []),
            "consumption": self._from_json(row["consumption_json"], []),
            "net_consumption": self._from_json(row["net_consumption_json"], []),
            "metadata": self._from_json(row["metadata_json"], {}),
        }


def _json_default(value):
    """Serialize numpy and datetime-like values without depending on numpy."""
    if hasattr(value, "tolist"):
        return value.tolist()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _interval_energy_to_power(value, interval_minutes):
    """Convert interval energy in Wh to mean power in W for charting."""
    if value is None:
        return None
    if not interval_minutes:
        return float(value)
    return float(value) * 60.0 / float(interval_minutes)
