import logging
import os
import time
from typing import Any, Dict, Iterable, List
from supabase import Client, create_client
from config import DatabaseCredentials


_http_logger = logging.getLogger("httpx")
_httpcore_logger = logging.getLogger("httpcore")

class SupabaseService:
    """High-level wrapper for Supabase interactions (auth + table ops).
       Adheres to DIP: callers depend on this abstraction rather than raw client.
    """
    def __init__(self, creds: DatabaseCredentials):
        # Supabase uses httpx under the hood; its INFO-level request logs are very noisy.
        _http_logger.setLevel(logging.WARNING)
        _httpcore_logger.setLevel(logging.WARNING)
        self._creds = creds
        self._client: Client = create_client(creds.url, creds.api_key)
        self._auth(creds.email, creds.password)

        # Retry config for transient HTTP transport issues.
        self._max_retries = self._read_int_env("SUPABASE_MAX_RETRIES", 5, min_value=0)
        self._base_sleep = self._read_float_env("SUPABASE_RETRY_BASE_SLEEP", 1.0, min_value=0.0)
        self._insert_many_chunk_size = self._read_int_env("SUPABASE_INSERT_MANY_CHUNK", 500, min_value=1)
        self._fetch_all_chunk_size = self._read_int_env("SUPABASE_FETCH_ALL_CHUNK", 1000, min_value=1)

    @staticmethod
    def _read_int_env(name: str, default: int, *, min_value: int | None = None) -> int:
        try:
            v = int(os.getenv(name, str(default)))
        except Exception:
            v = default
        if min_value is not None:
            v = max(min_value, v)
        return v

    @staticmethod
    def _read_float_env(name: str, default: float, *, min_value: float | None = None) -> float:
        try:
            v = float(os.getenv(name, str(default)))
        except Exception:
            v = default
        if min_value is not None:
            v = max(min_value, v)
        return v

    def _should_retry_exception(self, err: Exception) -> bool:
        # Supabase uses httpx/httpcore underneath; HTTP/2 streams can be terminated mid-run.
        mod = type(err).__module__
        name = type(err).__name__
        return (mod.startswith("httpx") or mod.startswith("httpcore")) and name in {
            "RemoteProtocolError",
            "ConnectError",
            "ReadTimeout",
            "WriteError",
            "NetworkError",
            "ProtocolError",
        }

    def _reset_client(self) -> None:
        # Best-effort close existing sessions; supabase-py uses httpx under the hood.
        try:
            postgrest = getattr(self._client, "postgrest", None)
            session = getattr(postgrest, "session", None)
            close = getattr(session, "close", None)
            if callable(close):
                close()
        except Exception:
            pass

        self._client = create_client(self._creds.url, self._creds.api_key)
        self._auth(self._creds.email, self._creds.password)

    def _with_retries(self, op_name: str, fn):
        attempt = 0
        while True:
            try:
                return fn()
            except Exception as err:
                attempt += 1
                if attempt > self._max_retries or not self._should_retry_exception(err):
                    raise

                # HTTP/2 connections may be terminated by the server mid-run; recreating the
                # client gives us a fresh connection pool.
                err_name = type(err).__name__
                if err_name == "RemoteProtocolError":
                    self._reset_client()

                sleep_time = self._base_sleep * (2 ** min(attempt, 6))
                logging.getLogger(__name__).warning(
                    "Supabase %s failed (%s: %s). Retrying %s/%s in %.1fs",
                    op_name,
                    type(err).__name__,
                    err,
                    attempt,
                    self._max_retries,
                    sleep_time,
                )
                time.sleep(sleep_time)

    def _auth(self, email: str, password: str) -> None:
        self._client.auth.sign_in_with_password({"email": email, "password": password})

    @property
    def client(self) -> Client:
        return self._client

    # Generic helpers
    def fetch_all(self, table: str) -> List[Dict[str, Any]]:
        # PostgREST applies a default row limit unless a range is specified.
        # Paginate until exhaustion so callers truly get *all* rows.
        chunk_size = max(1, int(self._fetch_all_chunk_size))
        all_rows: List[Dict[str, Any]] = []
        offset = 0
        while True:
            resp = self._client.table(table).select("*").range(offset, offset + chunk_size - 1).execute()
            rows = getattr(resp, "data", None)
            if not rows:
                break
            all_rows.extend(rows)
            if len(rows) < chunk_size:
                break
            offset += chunk_size
        return all_rows

    def upsert(self, table: str, rows: Iterable[Dict[str, Any]]) -> None:
        rows_list = list(rows)
        if not rows_list:
            return
        self._with_retries("upsert", lambda: self._client.table(table).upsert(rows_list).execute())

    def select_eq(self, table: str, column: str, value: Any) -> List[Dict[str, Any]]:
        return self._client.table(table).select("*").eq(column, value).execute().data  # type: ignore

    def update_eq(self, table: str, column: str, value: Any, fields: Dict[str, Any]) -> None:
        """Update rows where `column == value` with `fields`."""
        if not fields:
            return
        self._with_retries(
            "update",
            lambda: self._client.table(table).update(fields).eq(column, value).execute(),
        )

    def insert(self, table: str, row: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a single row and return inserted row (empty dict if none)."""
        resp = self._with_retries("insert", lambda: self._client.table(table).insert(row).execute())
        return resp.data[0] if getattr(resp, "data", None) else {}

    def insert_many(self, table: str, rows: Iterable[Dict[str, Any]]) -> int:
        """Insert many rows. Returns number of rows acknowledged by the client."""
        rows_list = list(rows)
        if not rows_list:
            return 0

        # Chunk large inserts to avoid oversized payloads and to reduce the chance
        # of long-lived HTTP/2 streams being terminated.
        inserted = 0
        chunk_size = max(1, int(self._insert_many_chunk_size))
        for i in range(0, len(rows_list), chunk_size):
            chunk = rows_list[i : i + chunk_size]
            resp = self._with_retries("insert_many", lambda: self._client.table(table).insert(chunk).execute())
            data = getattr(resp, "data", None)
            inserted += len(data) if isinstance(data, list) else 0
        return inserted

    # Maintenance helpers
    def delete_all_tables(self) -> Dict[str, int]:
        """Delete all rows from known application tables (non-TRUNCATE).

        Returns a mapping of table name to number of rows deleted.
        NOTE: Row Level Security policies must allow DELETE for this to work.
        This issues per-table bulk deletes using primary key lists (safe but slower
        than TRUNCATE). For large datasets prefer a SQL function with TRUNCATE.
        """
        # Explicit list to avoid wiping unintended system/auth tables.
        table_pk = {
            "history": "id",
            "ranking": "player_id",
            "player_table": "player_id",
            "videogame_mapping": "id",
        }
        deleted_counts: Dict[str, int] = {}
        for table, pk in table_pk.items():
            rows = self.fetch_all(table)
            if not rows:
                deleted_counts[table] = 0
                continue
            # Use batched primary key deletion to avoid RLS issues with broad filters.
            # PostgREST requires a filter on DELETE; we loop individual rows.
            count = 0
            for r in rows:
                if pk in r:
                    self._client.table(table).delete().eq(pk, r[pk]).execute()
                    count += 1
            deleted_counts[table] = count
        return deleted_counts

    @staticmethod
    def generate_truncate_sql(include_identity_reset: bool = True) -> str:
        """Generate SQL to TRUNCATE all application tables.

        Execute this in the Supabase SQL editor (requires appropriate role).
        If Row Level Security is enabled, TRUNCATE bypass requires service role.
        """
        tables = ["history", "ranking", "player_table", "videogame_mapping"]
        opts = " RESTART IDENTITY CASCADE" if include_identity_reset else ""
        return f"TRUNCATE TABLE {', '.join('public.' + t for t in tables)}{opts};"
