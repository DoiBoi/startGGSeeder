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
from repos.videogame_repository import VideogameRepository
from repos.last_updated_repository import LastUpdatedRepository
from service.rating_service import RatingService
from repos.tournament_processor import TournamentProcessor
from repos.history_repository import HistoryRepository
import query as q

_creds = EnvironmentConfig.load()
_supabase_service = SupabaseService(_creds)
_player_repo = PlayerRepository(_supabase_service)
_rating_service = RatingService()
_history_repo = HistoryRepository(_supabase_service)
_tournament_processor = TournamentProcessor(_supabase_service, _player_repo, _rating_service, history_repo=_history_repo)

def process_tournament(event_slug: str, saved_games: bool = False):
    """Facade for processing a tournament slug (keeps original API)."""
    _tournament_processor.process_tournament(event_slug, saved_games=saved_games)


def process_tournaments_from_mapping(
    country: str | None,
    state: str | None,
    per_page: int = 50,
    before_date: int | None = None,
    saved_games: bool = True,
    last_updated_key: str = "tournaments_endAt",
):
    """Bulk workflow:

    - reads videogame IDs from `videogame_mapping`
    - paginates tournaments query by (country,state,ids,after/before)
    - processes each tournament by `slug`
    - stores max `endAt` in `last_updated` under `last_updated_key`
    """
    vg_repo = VideogameRepository(_supabase_service)
    last_repo = LastUpdatedRepository(_supabase_service)

    videogame_ids = vg_repo.load_videogame_ids(as_strings=True)
    after_date = last_repo.get_timestamp(last_updated_key) or 0

    max_end_at = after_date
    processed = 0

    for node in q.fetch_tournaments_paginated(
        country=country,
        state=state,
        videogame_ids=videogame_ids,
        after_date=after_date,
        before_date=before_date,
        per_page=per_page,
    ):
        slug = node.get("slug")
        end_at = node.get("endAt")
        if not slug:
            continue
        if isinstance(end_at, int) and end_at > max_end_at:
            max_end_at = end_at
        print(f"Processing tournament slug={slug} endAt={end_at}")
        _tournament_processor.process_tournament(slug, saved_games=saved_games, update_discriminator=False)
        processed += 1

    if processed:
        # Run discriminator enrichment once at the end (much faster than per-tournament)
        from update import update_with_discriminator

        update_with_discriminator()
        last_repo.set_timestamp(last_updated_key, max_end_at)
        print(f"Updated last_updated[{last_updated_key}] = {max_end_at}")
    else:
        print("No tournaments found to process.")



# Example usage retained below (commented)
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
# process_tournament('okizeme-52', saved_games=True)

# process_tournament("pataka-2026", saved_games=True)

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

