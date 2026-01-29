import argparse
import os
import time

import query as q
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()


def _get_supabase_client() -> Client:
    database_url = os.getenv("DATABASE_API_URL")
    database_api = os.getenv("DATABASE_API_KEY")
    if not database_url or not database_api:
        raise ValueError("DATABASE_API_URL and DATABASE_API_KEY must be set in the environment variables.")

    supabase: Client = create_client(database_url, database_api)

    email = os.getenv("DATABASE_LOGIN_EMAIL")
    password = os.getenv("DATABASE_LOGIN_PASSWORD")
    if not email or not password:
        raise ValueError("DATABASE_LOGIN_EMAIL and DATABASE_LOGIN_PASSWORD must be set in the environment variables.")

    supabase.auth.sign_in_with_password({"email": email, "password": password})
    return supabase


def _fetch_player_table_rows(supabase: Client, *, chunk_size: int = 1000) -> list[dict]:
    chunk_size = max(1, int(chunk_size))
    all_rows: list[dict] = []
    offset = 0
    while True:
        resp = (
            supabase.table("player_table")
            .select("player_id,discriminator")
            .range(offset, offset + chunk_size - 1)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < chunk_size:
            break
        offset += chunk_size
    return all_rows

def update_with_discriminator(
    supabase: Client,
    *,
    batch_size: int = 250,
    sleep_seconds: float = 0.0,
    log_every: int = 50,
    dry_run: bool = False,
    only_missing: bool = True,
    debug_missing: int = 0,
) -> int:
    result = _fetch_player_table_rows(supabase)
    print(f"Loaded player_table rows={len(result)}")

    if only_missing:
        result = [r for r in result if r.get("discriminator") in (None, "")]
        print(f"Filtered to missing discriminator rows={len(result)}")

    merged = []
    batch_size = max(1, int(batch_size))
    for index in range(0, len(result), batch_size):
        batch = result[index : index + batch_size]
        print(f"Processing batch of {len(batch)} items")

        ids = [p["player_id"] for p in batch if "player_id" in p and p["player_id"] is not None]
        id_to_disc = q.fetch_player_discriminators(
            ids,
            sleep_seconds=sleep_seconds,
            log_every=log_every,
        )

        missing = [pid for pid in ids if int(pid) not in id_to_disc]
        print(f"Resolved discriminators={len(id_to_disc)}/{len(ids)}")
        if debug_missing and missing:
            sample = missing[: max(0, int(debug_missing))]
            print(f"Sample missing discriminator player_ids={sample}")

        for pid in ids:
            merged.append({"player_id": pid, "discriminator": id_to_disc.get(int(pid))})

        if sleep_seconds and sleep_seconds > 0:
            time.sleep(float(sleep_seconds))

    if dry_run:
        print(f"Dry run: would upsert discriminator rows={len(merged)}")
        return 0

    supabase.table("player_table").upsert(merged).execute()
    print(f"Updated player_table discriminator rows={len(merged)}")
    return len(merged)
        

def process_batch(batch):
    # Backwards-compatible wrapper (kept for any external callers).
    ids = [p['player_id'] for p in batch if 'player_id' in p]
    return {"data": q.fetch_player_discriminators(ids)}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Update player_table with start.gg discriminators")
    p.add_argument("--batch-size", type=int, default=250, help="Number of player_ids to process per batch.")
    p.add_argument("--sleep", type=float, default=0.0, help="Sleep seconds between requests/batches.")
    p.add_argument("--log-every", type=int, default=50, help="Log progress every N players.")
    p.add_argument(
        "--only-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Only update rows where discriminator is currently NULL/blank.",
    )
    p.add_argument(
        "--debug-missing",
        type=int,
        default=0,
        help="Print up to N sample player_ids that have no discriminator returned by start.gg.",
    )
    p.add_argument("--dry-run", action="store_true", help="Don't write to Supabase.")
    return p


def main() -> int:
    args = build_parser().parse_args()
    supabase = _get_supabase_client()
    update_with_discriminator(
        supabase,
        batch_size=args.batch_size,
        sleep_seconds=args.sleep,
        log_every=args.log_every,
        dry_run=args.dry_run,
        only_missing=args.only_missing,
        debug_missing=args.debug_missing,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())