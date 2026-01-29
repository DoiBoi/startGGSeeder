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
import logging

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


logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Bulk process tournaments by querying start.gg and persisting progress.")
    p.add_argument(
        "--tournament",
        action="append",
        default=None,
        help=(
            "Tournament slug to process (e.g. 'tournament/exp-2015'). "
            "Can be passed multiple times. If provided, bypasses tournament search/pagination."
        ),
    )
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
        "--save-history",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="If true, write match outcomes to the Supabase history table.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="List matching tournaments and compute max endAt without processing.",
    )

    p.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue processing next tournament when one fails (logs exception and moves on).",
    )
    p.add_argument(
        "--max-errors",
        type=int,
        default=25,
        help="When --continue-on-error is set, stop after this many tournament failures.",
    )

    p.add_argument(
        "--max-tournaments",
        type=int,
        default=None,
        help="Stop after processing this many tournaments (useful for smoke tests).",
    )

    p.add_argument(
        "--sort",
        default="startAt",
        choices=["startAt", "endAt", "eventRegistrationClosesAt", "computedUpdatedAt"],
        help="Server-side sort field used by start.gg (direction is API-defined).",
    )
    p.add_argument(
        "--sort-ascending",
        action="store_true",
        help="Client-side ascending sort by the chosen --sort field (fetches all pages first).",
    )

    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Logging verbosity.",
    )
    return p


def main() -> int:
    args = build_parser().parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Keep output focused: httpx request logs (used by Supabase) are very noisy at INFO.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logger.info(
        "Starting tournament slug retrieval country=%s state=%s after=%s before=%s per_page=%s sort=%s sort_ascending=%s",
        args.country,
        args.state,
        args.after_date,
        args.before_date,
        args.per_page,
        args.sort,
        args.sort_ascending,
    )

    creds = EnvironmentConfig.load()
    supabase = SupabaseService(creds)
    player_repo = PlayerRepository(supabase)
    rating_service = RatingService()

    history_repo = HistoryRepository(supabase) if args.save_history else None
    processor = TournamentProcessor(supabase, player_repo, rating_service, history_repo=history_repo)

    vg_repo = VideogameRepository(supabase)
    last_repo = LastUpdatedRepository(supabase)

    videogame_ids = vg_repo.load_videogame_ids(as_strings=True)
    stored_after = last_repo.get_timestamp(args.last_updated_key) or 0
    after_date = args.after_date if args.after_date is not None else stored_after

    max_end_at = after_date
    processed_max_end_at = after_date
    processed = 0
    failed = 0
    failed_slugs: list[str] = []
    seen = 0

    if args.tournament:
        nodes_iter = []
        for raw_slug in args.tournament:
            slug = (raw_slug or "").strip()
            if not slug:
                continue
            info = q.fetch_tournament_times(slug)
            nodes_iter.append(
                {
                    "slug": slug,
                    "startAt": info.get("startAt"),
                    "endAt": info.get("endAt"),
                    "name": info.get("name"),
                }
            )
    else:
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
        start_at = node.get("startAt")
        end_at = node.get("endAt")

        if not slug:
            continue

        if args.max_tournaments is not None and seen >= args.max_tournaments:
            logger.info("Reached max tournaments limit=%s; stopping.", args.max_tournaments)
            break
        seen += 1
        if isinstance(end_at, int) and end_at > max_end_at:
            max_end_at = end_at

        logger.info("Retrieved tournament slug=%s startAt=%s endAt=%s", slug, start_at, end_at)
        print(f"Found tournament slug={slug} startAt={start_at} endAt={end_at}")
        if args.dry_run:
            continue

        try:
            processor.process_tournament(slug, saved_games=args.saved_games, update_discriminator=False)
            processed += 1
            if isinstance(end_at, int) and end_at > processed_max_end_at:
                processed_max_end_at = end_at
        except Exception:
            failed += 1
            failed_slugs.append(slug)
            logger.exception("Failed processing tournament slug=%s (%s failures so far)", slug, failed)
            print(f"ERROR processing tournament slug={slug} (failure {failed}). See logs for details.")

            if not args.continue_on_error:
                raise
            if failed >= max(1, int(args.max_errors)):
                raise SystemExit(f"Stopping: reached max failures ({failed}/{args.max_errors}).")
            continue

    if args.dry_run:
        print(f"Dry run complete. max_endAt={max_end_at}")
        return 0

    if processed:
        update_with_discriminator()
        last_repo.set_timestamp(args.last_updated_key, processed_max_end_at)
        print(f"Updated last_updated[{args.last_updated_key}] = {processed_max_end_at}")
    else:
        print("No tournaments found to process.")

    if failed:
        logger.warning("Completed with failures=%s. Failed slugs (first 25): %s", failed, failed_slugs[:25])
        print(f"Completed with failures={failed}. See logs for details.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
