from typing import Dict, List, Tuple, Set, Optional
from datetime import datetime, timezone
import query as q
from repos.player_repository import PlayerRepository
from service.rating_service import RatingService
from service.supabase_service import SupabaseService
from repos.history_repository import HistoryRepository
from update import update_with_discriminator

class EventProcessor:
    def __init__(self, supabase: SupabaseService, players_repo: PlayerRepository, rating_service: RatingService, history_repo: Optional[HistoryRepository] = None):
        self.supabase = supabase
        self.players_repo = players_repo
        self.rating_service = rating_service
        self.history_repo = history_repo
        self.max_batch_size = 25

    def process_event(self, event: Dict, videogame_id: int, event_slug: str, event_time: Optional[datetime]) -> None:
        if not videogame_id:
            return
        entrants = event['entrants']['nodes']
        entrant_to_player: Dict[int, int] = {}
        entrant_ids: List[int] = []
        name_map = self.players_repo.load_names()
        players = self.players_repo.load_players(videogame_id)

        for entrant in entrants:
            player_id = entrant['participants'][0]['player']['id']
            player_name = entrant['participants'][0]['player']['gamerTag']
            entrant_id = entrant['id']
            if player_id not in players:
                from glicko2 import Player
                players[player_id] = Player()
                setattr(players[player_id], "appearances", 1)
                name_map[player_id] = player_name
            else:
                current_apps = getattr(players[player_id], "appearances", 0)
                setattr(players[player_id], "appearances", current_apps + 1)
            entrant_to_player[entrant_id] = player_id
            entrant_ids.append(entrant_id)

        seen_sets: Set[str] = set()
        for i in range(0, len(entrant_ids), self.max_batch_size):
            batch = entrant_ids[i:i + self.max_batch_size]
            print(f"Processing batch {i // self.max_batch_size + 1} of {len(entrants) // self.max_batch_size + 1} for game {event['videogame']['name']}")
            matches = self._fetch_matches_for_batch(batch, entrant_to_player, seen_sets)
            # Record raw match outcomes before rating update
            if self.history_repo:
                for winner, loser in matches:
                    self.history_repo.record(event_slug=event_slug, winner_id=str(winner), loser_id=str(loser), played_time=event_time)
            self.rating_service.apply_matches(players, matches)

        self.players_repo.save_players(videogame_id, players, name_map)
        self.players_repo.save_names(name_map)

    def _fetch_matches_for_batch(self, entrant_ids: List[int], entrant_to_player: Dict[int, int], seen_sets: Set[str]) -> List[Tuple[int, int]]:
        query = """query EntrantsWithSets("""
        for i in range(len(entrant_ids)):
            query += f"$entrantId{i}: ID!, "
        query += "$page: Int!, $perPage: Int!) {\n"
        for i in range(len(entrant_ids)):
            query += f"""E{i}: entrant(id: $entrantId{i})""" + """{  
            paginatedSets (page: $page, perPage: $perPage) {
                nodes {
                    id
                    winnerId
                    slots {
                        entrant {
                            id
                        }
                    }
                }
            }
        }\n"""
        query += """}"""
        sets = q.run_query(query, {**{f"entrantId{i}": entrant_ids[i] for i in range(len(entrant_ids))}, "page": 0, "perPage": 20})
        if "errors" in sets:
            print(f"Error retrieving sets: {sets['errors']}")
            return []
        matches: List[Tuple[int, int]] = []
        for i in range(len(sets['data'])):
            entrant_sets = sets['data'][f"E{i}"]['paginatedSets']['nodes']
            entrant_sets = list(reversed(entrant_sets))
            for game_set in entrant_sets:
                set_id = game_set['id']
                if set_id in seen_sets:
                    continue
                seen_sets.add(set_id)
                winner_id = game_set['winnerId']
                e1 = game_set['slots'][0]['entrant']['id']
                e2 = game_set['slots'][1]['entrant']['id']
                if e1 not in entrant_to_player or e2 not in entrant_to_player:
                    continue
                p1 = entrant_to_player[e1]
                p2 = entrant_to_player[e2]
                if winner_id == e1:
                    matches.append((p1, p2))
                elif winner_id == e2:
                    matches.append((p2, p1))
        return matches

class TournamentProcessor:
    def __init__(self, supabase: SupabaseService, players_repo: PlayerRepository, rating_service: RatingService, history_repo: Optional[HistoryRepository] = None):
        self.supabase = supabase
        self.players_repo = players_repo
        self.rating_service = rating_service
        self.history_repo = history_repo
        self.event_processor = EventProcessor(supabase, players_repo, rating_service, history_repo)

    def process_tournament(self, event_slug: str, saved_games: bool = False):
        print(f"Processing tournament {event_slug}...")
        events = q.run_query(q.resultsQuery, {"slug": event_slug})['data']['tournament']['events']
        existing_map_rows = self.supabase.fetch_all("videogame_mapping")
        videogame_map = {int(row['id']): row['name'] for row in existing_map_rows}

        if saved_games:
            events = [e for e in events if e['videogame']['id'] in videogame_map]

        query = """query EventsQuery("""
        for i in range(len(events)):
            query += f"$id{i}: ID!, "
        query += "$page: Int!, $perPage: Int!) {\n"
        for i in range(len(events)):
            query += f"""E{i}: event(id: $id{i})""" + """{
            slug
            numEntrants
            startAt 
            videogame { name id }
            entrants (query: {page: $page, perPage: $perPage}) {
                nodes {
                    id
                    participants { player { gamerTag id } }
                }
            } 
        }\n"""
        query += """}"""

        events_entrants = q.run_query(query, {**{f"id{i}": events[i]['id'] for i in range(len(events))}, "page": 0, "perPage": 512})
        if "errors" in events_entrants:
            print(f"Error retrieving events for {event_slug}: {events_entrants['errors']}")
            print(f"Previous query: {query}")
            return

        for i in range(len(events_entrants['data'])):
            e_key = f"E{i}"
            videogame_id = events_entrants['data'][e_key]['videogame']['id']
            videogame_name = events_entrants['data'][e_key]['videogame']['name']
            start_at_raw = events_entrants['data'][e_key].get('startAt')
            event_time = None
            if isinstance(start_at_raw, int):
                try:
                    event_time = datetime.fromtimestamp(start_at_raw, tz=timezone.utc)
                except Exception:
                    event_time = None
            if not saved_games and videogame_id not in videogame_map:
                videogame_map[videogame_id] = videogame_name
            if saved_games and videogame_id not in videogame_map:
                continue
            ev_slug = events_entrants['data'][e_key]['slug']
            self.event_processor.process_event(events_entrants['data'][e_key], videogame_id=videogame_id, event_slug=ev_slug, event_time=event_time)

        # Upsert videogame mapping
        self.supabase.upsert("videogame_mapping", [{"id": vid, "name": name} for vid, name in videogame_map.items()])
        update_with_discriminator()
