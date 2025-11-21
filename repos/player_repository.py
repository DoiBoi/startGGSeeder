from typing import Dict, List
from glicko2 import Player
from service.supabase_service import SupabaseService

class PlayerRepository:
    """Handles persistence of player ratings, names, and rankings (SRP)."""
    def __init__(self, supabase: SupabaseService):
        self._db = supabase

    def load_players(self, game_id: int) -> Dict[int, Player]:
        rows = self._db.select_eq("ranking", "game_id", game_id)
        players: Dict[int, Player] = {}
        for row in rows:
            player_id = int(row['player_id'])
            # type: ignore comments silence strict type expectations from glicko2 implementation
            p = Player(
                rating=float(row['rating']),  # type: ignore
                rd=float(row['rd']),          # type: ignore
                vol=float(row['vol'])
            )
            setattr(p, "appearances", int(row.get('appearances', 1)))
            players[player_id] = p
        return players

    def load_names(self) -> Dict[int, str]:
        rows = self._db.fetch_all("player_table")
        return {int(r['player_id']): r['name'] for r in rows}

    def save_players(self, game_id: int, players: Dict[int, Player], name_map: Dict[int, str]) -> None:
        sorted_players = sorted(players.items(), key=lambda x: x[1].rating, reverse=True)
        rankings = {pid: rank for rank, (pid, _) in enumerate(sorted_players, start=1)}
        data: List[Dict] = []
        for pid, player in sorted_players:
            appearances = getattr(player, "appearances", 1)
            data.append({
                "player_id": pid,
                "game_id": game_id,
                "name": name_map.get(pid, "Unknown"),
                "rating": player.rating,
                "rd": player.rd,
                "vol": player.vol,
                "ranking": rankings[pid],
                "appearances": appearances,
            })
        self._db.upsert("ranking", data)

    def save_names(self, name_map: Dict[int, str]) -> None:
        data = [{"player_id": pid, "name": name} for pid, name in name_map.items()]
        self._db.upsert("player_table", data)
