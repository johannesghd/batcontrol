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
            connection.commit()

    @staticmethod
    def _to_json(value) -> str:
        return json.dumps(value, sort_keys=True, default=_json_default)

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
            metadata: dict = None) -> None:
        """Persist one completed calculation snapshot."""
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
                            prices_json,
                            production_json,
                            consumption_json,
                            net_consumption_json,
                            metadata_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            created_at_ts,
                            mode,
                            charge_rate_w,
                            soc_percent,
                            stored_energy_wh,
                            reserved_energy_wh,
                            free_capacity_wh,
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


def _json_default(value):
    """Serialize numpy and datetime-like values without depending on numpy."""
    if hasattr(value, "tolist"):
        return value.tolist()
    if hasattr(value, "item"):
        return value.item()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
