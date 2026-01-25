from typing import List, Literal, overload

from service.supabase_service import SupabaseService


class VideogameRepository:
    """Read helpers for videogame metadata stored in Supabase."""

    def __init__(self, supabase: SupabaseService):
        self._db = supabase

    @overload
    def load_videogame_ids(self, as_strings: Literal[True] = True) -> List[str]: ...

    @overload
    def load_videogame_ids(self, as_strings: Literal[False]) -> List[int]: ...

    def load_videogame_ids(self, as_strings: bool = True) -> List[int] | List[str]:
        """Return all videogame IDs from `videogame_mapping`.

        start.gg's GraphQL `ID` scalar is typically passed as strings, so
        `as_strings=True` is the most convenient for `$id: [ID]` variables.
        """
        rows = self._db.fetch_all("videogame_mapping")
        ids: List[int] = [int(r["id"]) for r in rows if r.get("id") is not None]
        ids.sort()
        if as_strings:
            return [str(i) for i in ids]
        return ids
