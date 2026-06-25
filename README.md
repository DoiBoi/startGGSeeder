# Player Rankings using glicko 2 system

Currently in heavy development.

To collaborators, I'm working on processing start.gg's return data, an example of how it would looks like is in items.json

Edit: It is now able to serialize and deserialize the data, what is now needed is to separate them by games

## Bulk tournament seeding (CLI)

This project includes a CLI that:
- Reads videogame IDs from the Supabase table `videogame_mapping`
- Queries start.gg for tournaments matching `(country/state/game IDs)` with pagination
- Processes each tournament by `slug`
- Stores the latest processed tournament `endAt` (unix timestamp) in Supabase table `last_updated`

### Usage

```powershell
py seed_tournaments.py --country CA --state BC --per-page 50
```

### Flags

- `--tournament` Tournament slug to process (can be passed multiple times). If provided, bypasses search/pagination.
- `--country` Country code filter (e.g. `CA`, `US`).
- `--state` State/province code filter (e.g. `BC`, `WA`).
- `--per-page` Page size for start.gg queries (default: `50`).
- `--before-date` Upper bound unix timestamp (e.g. `1769320800`).
- `--after-date` Lower bound unix timestamp override. If omitted, the tool uses the stored timestamp from `last_updated`.
- `--last-updated-key` The primary key string used in the `last_updated` table (default: `tournaments_endAt`).
- `--saved-games` / `--no-saved-games` If enabled (default), only processes events whose game IDs exist in `videogame_mapping`.
- `--save-history` / `--no-save-history` If enabled (default), writes match outcomes to the `history` table.
- `--dry-run` Lists matching tournaments and computes the max `endAt` without processing.

Failure handling:
- `--continue-on-error` Keep going when a tournament fails (logs exception and continues).
- `--max-errors` When `--continue-on-error` is set, stop after this many failures (default: `25`).

Limits:
- `--max-tournaments` Stop after processing this many tournaments (useful for smoke tests).

Sorting:
- `--sort` One of `startAt|endAt|eventRegistrationClosesAt|computedUpdatedAt` (default: `startAt`).
- `--sort-ascending` Client-side ascending sort by `--sort` (fetches all pages first).

Logging:
- `--log-level` One of `CRITICAL|ERROR|WARNING|INFO|DEBUG` (default: `INFO`).

Post-processing:
- `--update-discriminator` / `--no-update-discriminator` If enabled (default), runs a final discriminator enrichment pass on `player_table`.

### Examples

Dry-run to see what would be processed:

```powershell
py seed_tournaments.py --country CA --state BC --dry-run
```

Backfill up to a specific timestamp:

```powershell
py seed_tournaments.py --country CA --state BC --before-date 1769320800
```

Process specific tournaments by slug (bypasses search/pagination):

```powershell
py seed_tournaments.py --tournament "tournament/exp-2015" --tournament "tournament/another-event"
```

Continue past failures (stop after 10 failed tournaments):

```powershell
py seed_tournaments.py --country CA --state BC --continue-on-error --max-errors 10
```

Example (fast run, no history, skip discriminator enrichment):

```powershell
py seed_tournaments.py --country CA --state BC --before-date 1770025597 --per-page 150 --sort-ascending --continue-on-error --no-save-history --no-update-discriminator
```

## Other scripts

### Elo / rating workflow (script)

Runs the refactored tournament + rating workflow.

```powershell
py elo-calc-v2.py
```

Notes:
- This file currently executes work immediately when run (it upserts `videogame_mapping` based on a hardcoded list of tournament slugs).
- If you want a safer, parameterized CLI for tournament seeding, use `seed_tournaments.py`.

### Clean Supabase tables (DANGER)

Deletes all rows from the project tables.

```powershell
py clean_table.py
```

Notes:
- This is destructive. Run only against the intended database.
- It uses the same env vars / credentials as the other Supabase scripts.

### Backfill player names (CLI)

Backfills `player_table.name` and `ranking.name` using start.gg gamerTags.

```powershell
py backfill_player_names.py --dry-run
```

Common flags:
- `--only-unknown` / `--no-only-unknown` Only update Unknown/blank names (default: enabled).
- `--batch-size` How many player IDs to query per start.gg request (default: `150`).
- `--limit` Max number of player IDs to process (useful for testing).
- `--sleep` Seconds to sleep between start.gg batches (helps avoid rate limits).
- `--dry-run` Compute changes without writing to Supabase.
- `--sync-ranking-from-player-table` / `--no-sync-ranking-from-player-table` Also fix `ranking.name` from `player_table.name` (default: enabled).
- `--log-level` One of `CRITICAL|ERROR|WARNING|INFO|DEBUG`.

Examples:

```powershell
# Backfill up to 200 player IDs, 100 IDs per start.gg request
py backfill_player_names.py --limit 200 --batch-size 100

# Only update Unknowns, slow down to avoid rate limits
py backfill_player_names.py --only-unknown --sleep 1.0 --log-level INFO
```


