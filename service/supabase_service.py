from typing import Any, Dict, Iterable, List
from supabase import Client, create_client
from config import DatabaseCredentials

class SupabaseService:
    """High-level wrapper for Supabase interactions (auth + table ops).
       Adheres to DIP: callers depend on this abstraction rather than raw client.
    """
    def __init__(self, creds: DatabaseCredentials):
        self._client: Client = create_client(creds.url, creds.api_key)
        self._auth(creds.email, creds.password)

    def _auth(self, email: str, password: str) -> None:
        self._client.auth.sign_in_with_password({"email": email, "password": password})

    @property
    def client(self) -> Client:
        return self._client

    # Generic helpers
    def fetch_all(self, table: str) -> List[Dict[str, Any]]:
        return self._client.table(table).select("*").execute().data  # type: ignore

    def upsert(self, table: str, rows: Iterable[Dict[str, Any]]) -> None:
        rows_list = list(rows)
        if not rows_list:
            return
        self._client.table(table).upsert(rows_list).execute()

    def select_eq(self, table: str, column: str, value: Any) -> List[Dict[str, Any]]:
        return self._client.table(table).select("*").eq(column, value).execute().data  # type: ignore

    def insert(self, table: str, row: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a single row and return inserted row (empty dict if none)."""
        resp = self._client.table(table).insert(row).execute()
        return resp.data[0] if getattr(resp, "data", None) else {}

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
