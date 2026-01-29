from typing import Dict, Any, Iterable, Optional
from datetime import datetime
from service.supabase_service import SupabaseService

class HistoryRepository:
    """Persists individual match outcomes into the lean public.history table."""
    def __init__(self, supabase: SupabaseService):
        self._supabase = supabase

    def record(
        self,
        *,
        event_slug: str,
        winner_id: str,
        loser_id: str,
        played_time: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        payload = {
            "eventSlug": event_slug,
            "winnerId": winner_id,
            "loserId": loser_id
        }
        if played_time is not None:
            payload["time"] = played_time.isoformat()
        return self._supabase.insert("history", payload)

    def record_many(
        self,
        *,
        event_slug: str,
        pairs: Iterable[tuple[str, str]],
        played_time: Optional[datetime] = None,
    ) -> int:
        rows = []
        for winner_id, loser_id in pairs:
            payload: Dict[str, Any] = {
                "eventSlug": event_slug,
                "winnerId": winner_id,
                "loserId": loser_id,
            }
            if played_time is not None:
                payload["time"] = played_time.isoformat()
            rows.append(payload)
        return self._supabase.insert_many("history", rows)
