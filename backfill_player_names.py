"""Backfill player names (gamerTags) in Supabase.

Why:
- `ranking.name` and `player_table.name` can contain many "Unknown" values.
- This helper finds those player_ids and queries start.gg for the correct gamerTag,
  then updates Supabase.

Usage examples:
  py backfill_player_names.py --dry-run
  py backfill_player_names.py --limit 200 --batch-size 100
  py backfill_player_names.py --only-unknown --sleep 1.0 --log-level INFO

Env:
- Requires the same DATABASE_* env vars as seed_tournaments.py.
- Uses start.gg env vars used by query.run_query (SGG_API_URL/SGG_API_KEY).
"""

from __future__ import annotations

import argparse
import logging
import time

import query as q
from config import EnvironmentConfig
from service.supabase_service import SupabaseService

logger = logging.getLogger(__name__)


UNKNOWN_VALUES = {"unknown", "unkown", "unkowns", ""}


def _is_unknown_name(name: object) -> bool:
    if name is None:
        return True
    if not isinstance(name, str):
        return False
    return name.strip().lower() in UNKNOWN_VALUES


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Backfill player_table/ranking names from start.gg player IDs")
    p.add_argument(
        "--only-unknown",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Only update rows whose current name is Unknown/blank.",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=150,
        help="How many start.gg player IDs to query per GraphQL request.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of player IDs to process (for testing).",
    )
    p.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Sleep seconds between start.gg batches (helps avoid rate limits).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute what would change, but don't write to Supabase.",
    )
    p.add_argument(
        "--sync-ranking-from-player-table",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Also fix ranking.name using player_table.name when available "
            "(no start.gg calls needed)."
        ),
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

    creds = EnvironmentConfig.load()
    supabase = SupabaseService(creds)

    ranking_rows = supabase.fetch_all("ranking")
    player_rows = supabase.fetch_all("player_table")

    player_name_by_id: dict[int, str] = {}
    discriminator_by_id: dict[int, object] = {}
    for r in player_rows:
        try:
            pid = int(r.get("player_id"))
        except Exception:
            continue
        player_name_by_id[pid] = r.get("name")
        if "discriminator" in r:
            discriminator_by_id[pid] = r.get("discriminator")

    # Collect player IDs to fix.
    to_fix: list[int] = []
    seen: set[int] = set()

    for row in ranking_rows:
        try:
            pid = int(row.get("player_id"))
        except Exception:
            continue

        current_ranking_name = row.get("name")
        current_table_name = player_name_by_id.get(pid)

        if args.only_unknown:
            needs = _is_unknown_name(current_ranking_name) or _is_unknown_name(current_table_name)
        else:
            needs = True

        if not needs:
            continue

        if pid in seen:
            continue
        seen.add(pid)
        to_fix.append(pid)

    if args.limit is not None:
        to_fix = to_fix[: max(0, int(args.limit))]

    logger.info(
        "Loaded rows: ranking=%s player_table=%s",
        len(ranking_rows),
        len(player_rows),
    )
    logger.info("Found player_ids needing start.gg backfill=%s (only_unknown=%s)", len(to_fix), args.only_unknown)

    updated_player_table = 0
    updated_ranking_from_startgg = 0
    updated_ranking_from_player_table = 0

    if to_fix:
        batch_size = max(1, int(args.batch_size))
        for start in range(0, len(to_fix), batch_size):
            batch = to_fix[start : start + batch_size]
            logger.info("Fetching start.gg gamerTags batch %s-%s", start + 1, start + len(batch))

            id_to_tag = q.fetch_player_gamertags(batch, batch_size=batch_size)
            if not id_to_tag:
                logger.warning("No gamerTags resolved for this batch")
            else:
                logger.info("Resolved gamerTags=%s/%s", len(id_to_tag), len(batch))

            if args.dry_run:
                continue

            # Update player_table in bulk (keep discriminator if we already have one).
            upsert_rows = []
            for pid, tag in id_to_tag.items():
                row = {"player_id": pid, "name": tag}
                if pid in discriminator_by_id:
                    row["discriminator"] = discriminator_by_id[pid]
                upsert_rows.append(row)

            if upsert_rows:
                supabase.upsert("player_table", upsert_rows)
                updated_player_table += len(upsert_rows)

            # Update ranking.name for each player_id (updates all game rows for that player).
            for pid, tag in id_to_tag.items():
                supabase.update_eq("ranking", "player_id", pid, {"name": tag})
                updated_ranking_from_startgg += 1

            if args.sleep and args.sleep > 0:
                time.sleep(float(args.sleep))

    # Optionally: sync ranking.name from player_table for any remaining Unknowns.
    if args.sync_ranking_from_player_table:
        # Refresh latest player_table names (includes any start.gg updates above).
        player_rows_latest = player_rows if args.dry_run else supabase.fetch_all("player_table")
        latest_name_by_id: dict[int, str] = {}
        for r in player_rows_latest:
            try:
                pid = int(r.get("player_id"))
            except Exception:
                continue
            latest_name_by_id[pid] = r.get("name")

        # Only update players where ranking currently has Unknown and player_table has a real name.
        pids_to_sync: set[int] = set()
        for row in ranking_rows:
            try:
                pid = int(row.get("player_id"))
            except Exception:
                continue
            if not _is_unknown_name(row.get("name")):
                continue
            mapped = latest_name_by_id.get(pid)
            if not mapped or _is_unknown_name(mapped):
                continue
            pids_to_sync.add(pid)

        logger.info(
            "Ranking sync candidates from player_table=%s (dry_run=%s)",
            len(pids_to_sync),
            args.dry_run,
        )

        if not args.dry_run:
            for pid in pids_to_sync:
                supabase.update_eq("ranking", "player_id", pid, {"name": latest_name_by_id[pid]})
                updated_ranking_from_player_table += 1

    if args.dry_run:
        print(
            "Dry run complete. "
            f"candidates_startgg={len(to_fix)} "
            f"sync_ranking_from_player_table={args.sync_ranking_from_player_table}"
        )
        return 0

    print(
        "Backfill complete. "
        f"player_table updated={updated_player_table} rows; "
        f"ranking updated via start.gg={updated_ranking_from_startgg} player_ids; "
        f"ranking updated via player_table sync={updated_ranking_from_player_table} player_ids."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
