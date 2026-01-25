from typing import Optional

from service.supabase_service import SupabaseService


class LastUpdatedRepository:
    """Stores and retrieves timestamps from the `last_updated` table."""

    def __init__(self, supabase: SupabaseService):
        self._db = supabase

    def get_timestamp(self, key: str) -> Optional[int]:
        rows = self._db.select_eq("last_updated", "last_updated", key)
        if not rows:
            return None
        ts = rows[0].get("timestamp")
        if ts is None:
            return None
        return int(ts)

    def set_timestamp(self, key: str, timestamp: int) -> None:
        self._db.upsert(
            "last_updated",
            [{"last_updated": key, "timestamp": int(timestamp)}],
        )
