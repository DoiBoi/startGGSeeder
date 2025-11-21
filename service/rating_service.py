from typing import Dict, List, Tuple
from glicko2 import Player

Match = Tuple[int, int]  # (winner_id, loser_id)

class RatingService:
    """Encapsulates batch rating updates (SRP + Open/Closed)."""
    def apply_matches(self, players: Dict[int, Player], matches: List[Match]) -> None:
        results: Dict[int, List[Tuple[float, float, int]]] = {pid: [] for pid in players}
        for winner, loser in matches:
            if winner not in players or loser not in players:
                continue
            w = players[winner]
            l = players[loser]
            results[winner].append((l.rating, l.rd, 1))
            results[loser].append((w.rating, w.rd, 0))

        for player_id, games in results.items():
            if games:
                r_list = [r for r, rd, score in games]
                rd_list = [rd for r, rd, score in games]
                outcome = [score for r, rd, score in games]
                players[player_id].update_player(r_list, rd_list, outcome)
