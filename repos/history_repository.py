from typing import Dict, Any, Optional
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
