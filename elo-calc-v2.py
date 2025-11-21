"""Orchestrator script refactored to use SOLID components.

Responsibilities moved into dedicated modules:
 - config.EnvironmentConfig: loads environment variables (SRP)
 - supabase_service.SupabaseService: data access abstraction (DIP)
 - player_repository.PlayerRepository: persistence of players & names (SRP)
 - rating_service.RatingService: rating updates (SRP, Open/Closed for adding new systems)
 - tournament_processor.TournamentProcessor: high-level tournament workflow

This file now exposes a simple function `process_tournament` for backwards compatibility.
"""

from config import EnvironmentConfig
from service.supabase_service import SupabaseService
from repos.player_repository import PlayerRepository
from service.rating_service import RatingService
from repos.tournament_processor import TournamentProcessor
from repos.history_repository import HistoryRepository

_creds = EnvironmentConfig.load()
_supabase_service = SupabaseService(_creds)
_player_repo = PlayerRepository(_supabase_service)
_rating_service = RatingService()
_history_repo = HistoryRepository(_supabase_service)
_tournament_processor = TournamentProcessor(_supabase_service, _player_repo, _rating_service, history_repo=_history_repo)

def process_tournament(event_slug: str, saved_games: bool = False):
    """Facade for processing a tournament slug (keeps original API)."""
    _tournament_processor.process_tournament(event_slug, saved_games=saved_games)

# Example usage retained below (commented)
process_tournament("ubc-summer-slam")
process_tournament("okizeme-46")
process_tournament("ubc-summer-slam-2")
process_tournament("ubc-summer-slam-3")
process_tournament("sunset-series-2024", saved_games=True)
process_tournament("ubc-sunset-showdown")
process_tournament("okizeme-47")
process_tournament("ubc-fgc-frenzy-friday-1")
process_tournament("ubc-fgc-frenzy-friday-2")
process_tournament("burnaby-boo-rawl", saved_games=True)
process_tournament("ubc-fgc-frenzy-friday-3")
process_tournament("pataka-esports-festival", saved_games=True)
process_tournament("ubc-fgc-autumn-assault-1")
process_tournament("ubc-fgc-frenzy-friday-4")
process_tournament("ubc-fgc-frenzy-friday-5")
process_tournament("goin-up", saved_games=True)
process_tournament("ubc-fgc-frenzy-friday-6")
process_tournament("okizeme-48")
process_tournament("ubc-fgc-winter-wavedash-1")
process_tournament("okizeme-49")
process_tournament("ubc-fgc-frenzy-friday-7")
process_tournament("ubc-fgc-frenzy-friday-8")
process_tournament("ubc-fgc-frenzy-friday-9")
process_tournament("end-of-heights-3")
process_tournament("ubc-fgc-frenzy-friday-10")
process_tournament("ubc-fgc-frenzy-friday-11")
process_tournament("ubc-fgc-winter-wavedash-2")
process_tournament("okizeme-50")
process_tournament("ubc-fgc-frenzy-friday-12")
process_tournament("cascadia-cup-road-to-bobc")
process_tournament("battle-of-bc-7-6", saved_games=True)
process_tournament("ubc-fgc-summer-slam-5")
process_tournament("ubc-fgc-summer-slam-6")
process_tournament('ubc-sunset-showdown-2')
process_tournament('okizeme-51')
process_tournament('okizeme-52', saved_games=True)

# Example usage:
# process_tournament("ubc-summer-slam")
# process_tournament("okizeme-46")
# process_tournament("ubc-summer-slam-2")
# process_tournament("ubc-summer-slam-3")
# process_tournament("sunset-series-2024", saved_games=True)
# process_tournament("ubc-sunset-showdown")
# process_tournament("okizeme-47")
# process_tournament("ubc-fgc-frenzy-friday-1")
# process_tournament("ubc-fgc-frenzy-friday-2")
# process_tournament("burnaby-boo-rawl", saved_games=True)
# process_tournament("ubc-fgc-frenzy-friday-3")
# process_tournament("pataka-esports-festival", saved_games=True)
# process_tournament("ubc-fgc-autumn-assault-1")
# process_tournament("ubc-fgc-frenzy-friday-4")
# process_tournament("ubc-fgc-frenzy-friday-5")
# process_tournament("goin-up", saved_games=True)
# process_tournament("ubc-fgc-frenzy-friday-6")
# process_tournament("okizeme-48")
# process_tournament("ubc-fgc-winter-wavedash-1")
# process_tournament("okizeme-49")
# process_tournament("ubc-fgc-frenzy-friday-7")
# process_tournament("ubc-fgc-frenzy-friday-8")
# process_tournament("ubc-fgc-frenzy-friday-9")
# process_tournament("end-of-heights-3")
# process_tournament("ubc-fgc-frenzy-friday-10")
# process_tournament("ubc-fgc-frenzy-friday-11")
# process_tournament("ubc-fgc-winter-wavedash-2")
# process_tournament("okizeme-50")
# process_tournament("ubc-fgc-frenzy-friday-12")
# process_tournament("cascadia-cup-road-to-bobc")
# process_tournament("battle-of-bc-7-6", saved_games=True)
# process_tournament("ubc-fgc-summer-slam-5")
# process_tournament("ubc-fgc-summer-slam-6")
# process_tournament('ubc-sunset-showdown-2')
# process_tournament('okizeme-51')
# process_tournament('okizeme-52')
