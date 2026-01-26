"""CLI entry-point to bulk process tournaments from start.gg.

Workflow:
- Read videogame IDs from Supabase `videogame_mapping`
- Page through start.gg tournaments using those IDs
- Process each tournament by slug
- Store latest processed endAt in Supabase `last_updated`

Example:
  py seed_tournaments.py --country CA --state BC --per-page 50
"""

from __future__ import annotations

import argparse

import query as q
from config import EnvironmentConfig
from repos.history_repository import HistoryRepository
from repos.last_updated_repository import LastUpdatedRepository
from repos.player_repository import PlayerRepository
from repos.tournament_processor import TournamentProcessor
from repos.videogame_repository import VideogameRepository
from service.rating_service import RatingService
from service.supabase_service import SupabaseService
from update import update_with_discriminator


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Bulk process tournaments by querying start.gg and persisting progress.")
    p.add_argument("--country", default=None, help="Country code (e.g. CA, US).")
    p.add_argument("--state", default=None, help="State/province code (e.g. BC, WA).")
    p.add_argument("--per-page", type=int, default=50, help="start.gg pagination size.")
    p.add_argument("--before-date", type=int, default=None, help="Unix timestamp upper bound (endAt filter).")
    p.add_argument(
        "--after-date",
        type=int,
        default=None,
        help="Unix timestamp lower bound override (if omitted, uses last_updated).",
    )
    p.add_argument(
        "--last-updated-key",
        default="tournaments_endAt",
        help="Primary key value used in Supabase last_updated table.",
    )
    p.add_argument(
        "--saved-games",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="If true, only process events whose game IDs exist in videogame_mapping.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="List matching tournaments and compute max endAt without processing.",
    )

    p.add_argument(
        "--sort",
        default="endAt",
        choices=["startAt", "endAt", "eventRegistrationClosesAt", "computedUpdatedAt"],
        help="Server-side sort field used by start.gg (direction is API-defined).",
    )
    p.add_argument(
        "--sort-ascending",
        action="store_true",
        help="Client-side ascending sort by the chosen --sort field (fetches all pages first).",
    )
    return p


def main() -> int:
    args = build_parser().parse_args()

    creds = EnvironmentConfig.load()
    supabase = SupabaseService(creds)
    player_repo = PlayerRepository(supabase)
    rating_service = RatingService()
    history_repo = HistoryRepository(supabase)
    processor = TournamentProcessor(supabase, player_repo, rating_service, history_repo=history_repo)

    vg_repo = VideogameRepository(supabase)
    last_repo = LastUpdatedRepository(supabase)

    videogame_ids = vg_repo.load_videogame_ids(as_strings=True)
    stored_after = last_repo.get_timestamp(args.last_updated_key) or 0
    after_date = args.after_date if args.after_date is not None else stored_after

    max_end_at = after_date
    processed = 0

    if args.sort_ascending:
        nodes_iter = q.fetch_tournaments_all(
            country=args.country,
            state=args.state,
            videogame_ids=videogame_ids,
            after_date=after_date,
            before_date=args.before_date,
            per_page=args.per_page,
            sort=args.sort,
            client_sort_ascending=True,
        )
    else:
        nodes_iter = q.fetch_tournaments_paginated(
            country=args.country,
            state=args.state,
            videogame_ids=videogame_ids,
            after_date=after_date,
            before_date=args.before_date,
            per_page=args.per_page,
            sort=args.sort,
        )

    for node in nodes_iter:
        slug = node.get("slug")
        end_at = node.get("endAt")

        if not slug:
            continue
        if isinstance(end_at, int) and end_at > max_end_at:
            max_end_at = end_at

        print(f"Found tournament slug={slug} endAt={end_at}")
        if args.dry_run:
            continue

        processor.process_tournament(slug, saved_games=args.saved_games, update_discriminator=False)
        processed += 1

    if args.dry_run:
        print(f"Dry run complete. max_endAt={max_end_at}")
        return 0

    if processed:
        update_with_discriminator()
        last_repo.set_timestamp(args.last_updated_key, max_end_at)
        print(f"Updated last_updated[{args.last_updated_key}] = {max_end_at}")
    else:
        print("No tournaments found to process.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
