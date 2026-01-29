import logging
import os
import time
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

tournament_query = """
query TournamentEvents($tourneySlug:String!) {
  tournament(slug: $tourneySlug) {
    id
    name
    events {
      id
      name
      slug
    }
  }
}
"""

tournament_times_query = """
query TournamentTimes($tourneySlug:String!) {
  tournament(slug: $tourneySlug) {
    name
    slug
    startAt
    endAt
  }
}
"""

participant_query = """
query TournamentParticipants($tourneySlug: String!, $page: Int!, $perPage: Int!) {
  tournament(slug: $tourneySlug) {
    participants (query: {
      perPage: $perPage, 
      page: $page
      }) {
      pageInfo {
        totalPages
      }
      nodes {
        player {
          id
          gamerTag
        }
        entrants {
          isDisqualified
          id
          name
          event {
            name
            videogame {
              id
              displayName
            }
          }
        }
      } 
    }
  }
}
"""

entrant_query = """
query EntrantSets($entrantId: ID!) {
  entrant(id: $entrantId) {
    event {
      id
      name
      videogame {
        id
        displayName
      }
    }
  }
}
"""

event_entrant_query = """
query Entrants($entrantId: ID!) {
  entrant(id: $entrantId) {
    standing {
      placement
    }
    event {
      numEntrants
      videogame {
        id
      }
    }
  }
}
"""

event_query = """
query getEventId($slug: String) {
  event(slug: $slug) {
    id
    name
    videogame {
      id
    }
  }
}
"""

# Retrieve tournament id and events by ids
resultsQuery = """
query TourneyQuery($slug: String) {
		tournament(slug: $slug){
			id
			name
			events {
        id
        videogame {
            name
            id
        }
			}
		}
	}
"""

entrants_w_sets_query = """
query EntrantsWithSets($entrantId: ID!, $page: Int!, $perPage: Int!) {
  entrant(id: $entrantId) {  
  	paginatedSets (page: $page, perPage: $perPage) {
      nodes {
        winnerId
        slots {
          entrant {
            id
          }
        }
      }
    }
  }
}
"""

## Retrieve entrants from all brackets (events) in a tournament
events_query = """
query EventsQuery($id: ID!, $page: Int!, $perPage: Int!) {
  event(id: $id) {
    numEntrants 
    videogame {
      name
      id
    }
    entrants (query: {page: $page, perPage: $perPage}) {
      nodes {
        id
        participants {
          player {
            gamerTag
            id
          }
        }
      }
    }
  }
}
"""

players_query = """
query EventEntrants($eventId: ID!, $page: Int!, $perPage: Int!) {
  event(id: $eventId) {
    id
    name
    entrants(query: {
      page: $page
      perPage: $perPage
    }) {
      pageInfo {
        total
        totalPages
      }
      nodes {
        participants {
            player {
                gamerTag
                id
          }
        }
      }
    }
  }
}
"""

player_query = """
query Player($playerId: ID!) {
  player(id: $playerId) {
    id
    gamerTag
    user {
      discriminator
    }
  }
}
"""


def fetch_player_discriminator(player_id: int) -> object:
  """Fetch a single player's discriminator using `player_query`.

  Returns the discriminator value (often a string/int) or None.
  """
  resp = run_query(player_query, {"playerId": int(player_id)})
  if "errors" in resp:
    raise Exception(f"Player query failed: {resp['errors']}")
  player = (resp.get("data") or {}).get("player") or {}
  user = player.get("user") or {}
  return user.get("discriminator")


def fetch_player_discriminator_info(player_id: int) -> dict:
  """Fetch discriminator plus basic presence info using `player_query`.

  Returns:
    {
      "player_id": int,
      "exists": bool,
      "gamerTag": str|None,
      "has_user": bool,
      "discriminator": object|None,
    }
  """
  resp = run_query(player_query, {"playerId": int(player_id)})
  if "errors" in resp:
    raise Exception(f"Player query failed: {resp['errors']}")

  player = (resp.get("data") or {}).get("player")
  if not player:
    return {
      "player_id": int(player_id),
      "exists": False,
      "gamerTag": None,
      "has_user": False,
      "discriminator": None,
    }

  user = player.get("user")
  has_user = bool(user)
  disc = (user or {}).get("discriminator") if isinstance(user, dict) else None
  tag = player.get("gamerTag")

  return {
    "player_id": int(player.get("id") or player_id),
    "exists": True,
    "gamerTag": tag,
    "has_user": has_user,
    "discriminator": disc,
  }


def fetch_player_discriminators(
  player_ids: list[int],
  *,
  sleep_seconds: float = 0.0,
  log_every: int = 50,
) -> dict[int, object]:
  """Fetch discriminators for many player IDs using repeated `player_query` calls.

  This intentionally uses the existing `player_query` (single-ID query) rather than
  a batched/aliased GraphQL query.
  """
  if not player_ids:
    return {}

  # De-dupe while preserving order
  seen: set[int] = set()
  ordered_ids: list[int] = []
  for pid in player_ids:
    try:
      pid_int = int(pid)
    except Exception:
      continue
    if pid_int in seen:
      continue
    seen.add(pid_int)
    ordered_ids.append(pid_int)

  result: dict[int, object] = {}
  for i, pid in enumerate(ordered_ids, start=1):
    if log_every and i % max(1, int(log_every)) == 0:
      logger.info("Fetching player discriminator %s/%s", i, len(ordered_ids))

    disc = fetch_player_discriminator(pid)
    if disc is not None:
      result[pid] = disc

    if sleep_seconds and sleep_seconds > 0:
      time.sleep(float(sleep_seconds))

  return result


def fetch_player_gamertags(player_ids: list[int], batch_size: int = 200) -> dict[int, str]:
  """Fetch start.gg gamerTags for the given player IDs.

  Uses a batched GraphQL query with aliases to reduce API calls.
  Returns a mapping of player_id -> gamerTag (only for players that resolve).
  """
  if not player_ids:
    return {}

  result: dict[int, str] = {}

  # De-dupe while preserving order
  seen: set[int] = set()
  ordered_ids: list[int] = []
  for pid in player_ids:
    if pid in seen:
      continue
    seen.add(pid)
    ordered_ids.append(pid)

  batch_size = max(1, int(batch_size))
  for start in range(0, len(ordered_ids), batch_size):
    batch = ordered_ids[start : start + batch_size]

    query = "query PlayerBatch("
    for i in range(len(batch)):
      query += f"$id{i}: ID!, "
    query += ") {\n"
    for i in range(len(batch)):
      query += (
        f"P{i}: player(id: $id{i})" + "{\n"
        "  id\n"
        "  gamerTag\n"
        "}\n"
      )
    query += "}"

    resp = run_query(query, {f"id{i}": batch[i] for i in range(len(batch))})
    if "errors" in resp:
      raise Exception(f"Player batch query failed: {resp['errors']}")
    data = resp.get("data") or {}

    for i, pid in enumerate(batch):
      p = data.get(f"P{i}")
      if not p:
        continue
      tag = (p.get("gamerTag") or "").strip()
      if not tag:
        continue
      try:
        pid_int = int(p.get("id") or pid)
      except Exception:
        pid_int = pid
      result[pid_int] = tag

  return result

recent_sets_query = """ 
query Sets ($playerId: ID!, $page: Int!, $perPage: Int!) {
  player(id: $playerId) {
    id
    sets(perPage: $perPage, page: $page) {
      nodes {
        winnerId
       	slots {
          entrant {
            id
            participants {
              player {
                id
              }
            }
          }
        }
        event {
          videogame {
            id
          }  
        }
      }
    }
  }
}
"""

gameId_query = """
query VideogameQuery ($name: String!) {
  videogames(query: { filter: { name: $name }, perPage: 1 }) {
    nodes {
      id
      name
      displayName
    }
  }
}"""

tournaments_search_query = """
query TournamentsQuery($country: String, $state: String, $id: [ID]) {
  tournaments(query: {
    filter: {
      countryCode: $country
      addrState: $state
      hasOnlineEvents: false
      videogameIds: $id
    }
    sort: startAt
  }) {
    pageInfo {
      total
      totalPages
      page
      perPage
    }
    nodes {
      name
    }
  }
"""

# Paginated tournament search (supports after/before timestamps)
tournaments_search_query_v2 = """
query TournamentsQuery($country: String, $state: String, $id: [ID], $afterDate: Timestamp, $beforeDate: Timestamp, $perPage: Int, $page: Int, $sort: TournamentPaginationSort) {
  tournaments(
    query: {filter: {countryCode: $country, addrState: $state, videogameIds: $id, afterDate: $afterDate, beforeDate: $beforeDate}, sort: $sort, perPage: $perPage, page: $page}
  ) {
    pageInfo {
      total
      totalPages
      page
      perPage
    }
    nodes {
      name
      slug
      startAt
      endAt
    }
  }
}
"""


def fetch_tournaments_paginated(
    country: str | None,
    state: str | None,
    videogame_ids: list[str] | None,
    after_date: int | None = None,
    before_date: int | None = None,
    per_page: int = 50,
    sort: str = "startAt",
):
    """Yield tournament nodes from start.gg using page-based pagination.

    `after_date`/`before_date` are Unix timestamps (e.g. 1769320800).
    `videogame_ids` should be strings for GraphQL `ID`.
    """
    page = 1
    total_pages = 1
    while page <= total_pages:
        logger.info(
            "Fetching tournaments page=%s/%s sort=%s country=%s state=%s after=%s before=%s per_page=%s",
            page,
            total_pages,
            sort,
            country,
            state,
            after_date,
            before_date,
            per_page,
        )
        variables = {
            "country": country,
            "state": state,
            "id": videogame_ids,
            "afterDate": after_date,
            "beforeDate": before_date,
            "perPage": per_page,
            "page": page,
            "sort": sort,
        }
        resp = run_query(tournaments_search_query_v2, variables)
        if "errors" in resp:
            raise Exception(f"Tournament search failed: {resp['errors']}")
        data = resp.get("data", {}).get("tournaments")
        if not data:
            return
        page_info = data.get("pageInfo") or {}
        total_pages = int(page_info.get("totalPages") or 1)
        nodes = data.get("nodes") or []

        # Log a compact sample of slugs for visibility.
        slugs = [n.get("slug") for n in nodes if isinstance(n, dict) and n.get("slug")]
        logger.info(
            "Received tournaments page=%s nodes=%s sample_slugs=%s",
            page,
            len(nodes),
            slugs[:5],
        )
        for slug in slugs:
            logger.debug("Retrieved tournament slug=%s", slug)

        for node in nodes:
            yield node
        page += 1


def fetch_tournaments_all(
    country: str | None,
    state: str | None,
    videogame_ids: list[str] | None,
    after_date: int | None = None,
    before_date: int | None = None,
    per_page: int = 50,
    sort: str = "startAt",
    client_sort_ascending: bool = False,
    client_sort_field: str | None = None,
) -> list[dict]:
    """Fetch all pages into a list.

    If `client_sort_ascending=True`, sorts the combined results ascending by
    `client_sort_field` (defaults to `sort`). Null/missing values are treated
    as +infinity so they appear last.
    """
    nodes = list(
        fetch_tournaments_paginated(
            country=country,
            state=state,
            videogame_ids=videogame_ids,
            after_date=after_date,
            before_date=before_date,
            per_page=per_page,
            sort=sort,
        )
    )

    logger.info("Fetched total tournaments=%s (pre-client-sort)", len(nodes))

    if client_sort_ascending:
        field = client_sort_field or sort

        def sort_key(n: dict) -> float:
            v = n.get(field)
            if isinstance(v, (int, float)):
                return float(v)
            return float("inf")

        nodes.sort(key=sort_key)

        logger.info("Client-sorted tournaments ascending by %s", field)

    return nodes


def fetch_tournament_times(slug: str) -> dict:
    """Fetch a single tournament's timing info by slug."""
    resp = run_query(tournament_times_query, {"tourneySlug": slug})
    return (resp.get("data") or {}).get("tournament") or {}


def fetch_videogames_from_tournaments(
    tournament_slugs: list[str],
    batch_size: int = 10,
) -> dict[int, str]:
    """Return a de-duplicated mapping of videogame_id -> videogame_name.

    This is useful for populating/updating the Supabase `videogame_mapping` table.
    Batches slugs into a single GraphQL request to reduce API calls.
    """
    if not tournament_slugs:
        return {}

    result: dict[int, str] = {}

    for start in range(0, len(tournament_slugs), batch_size):
        batch = tournament_slugs[start : start + batch_size]

        query = "query TournamentVideogames("
        for i in range(len(batch)):
            query += f"$slug{i}: String!, "
        query += ") {\n"

        for i in range(len(batch)):
            query += (
                f"T{i}: tournament(slug: $slug{i})" + "{\n"
                "  events {\n"
                "    videogame { id name displayName }\n"
                "  }\n"
                "}\n"
            )

        query += "}"

        resp = run_query(query, {f"slug{i}": batch[i] for i in range(len(batch))})
        if "errors" in resp:
            raise Exception(f"Tournament videogame query failed: {resp['errors']}")

        data = resp.get("data") or {}
        for i in range(len(batch)):
            t = data.get(f"T{i}")
            if not t:
                continue
            for ev in t.get("events") or []:
                vg = (ev or {}).get("videogame")
                if not vg:
                    continue
                vg_id = vg.get("id")
                if vg_id is None:
                    continue
                try:
                    vg_id_int = int(vg_id)
                except Exception:
                    continue

                vg_name = vg.get("displayName") or vg.get("name") or "Unknown"
                # Keep the first seen non-empty name
                if vg_id_int not in result or result[vg_id_int] == "Unknown":
                    result[vg_id_int] = str(vg_name)

    return result

def create_entrant_query(num_events):
    query = "query EventEntrants("
    for i in range(num_events):
        query += f"$E{i+1}: ID! "
    query += ") {"
    
    for i in range(num_events):
        query += f"""
            E{i+1}: entrant(id: $E{i+1})""" + """{
              standing {
                placement
              }
              paginatedSets (page: 1, perPage:20) {
                nodes {
                  winnerId 
                  displayScore
                }
              }
              event {
                numEntrants
                videogame {
                  id
                }
              }
            }
        """
    query += "}"
    return query

def run_query(query, variables=None, retries=0):
  url = os.getenv("SGG_API_URL")
  key = os.getenv("SGG_API_KEY")
  timeout_seconds_raw = os.getenv("SGG_TIMEOUT_SECONDS", "60")
  max_retries_raw = os.getenv("SGG_MAX_RETRIES", "5")

  headers = {
    "Content-Type": "application/json",
    "Authorization": "Bearer " + str(key),  # Replace with your own token
  }

  try:
    timeout_seconds = max(5, int(timeout_seconds_raw))
  except Exception:
    timeout_seconds = 60

  try:
    max_retries = max(0, int(max_retries_raw))
  except Exception:
    max_retries = 5

  base_sleep_429 = 30  # Backoff for rate limits
  base_sleep_timeout = 5  # Smaller backoff for transient slow responses
  base_sleep_connection = 30

  while True:
    try:
      response = requests.post(
        str(url),
        json={"query": query, "variables": variables},
        headers=headers,
        timeout=timeout_seconds,
      )

      if response.status_code == 200:
        return response.json()

      if response.status_code == 429:
        if retries > 9:
          retries = 0
        sleep_time = base_sleep_429 * (2**retries)
        msg = (
          f"{datetime.now()} - Rate limit exceeded. Waiting for {sleep_time} seconds before retrying..."
        )
        logger.warning(msg)
        print(msg)
        time.sleep(sleep_time)
        retries += 1
        continue

      if response.status_code == 503:
        msg = f"{datetime.now()} - Service unavailable. Waiting for 60 seconds before retrying..."
        logger.warning(msg)
        print(msg)
        time.sleep(60)
        continue

      raise Exception(
        f"{datetime.now()} - Query failed to run with a status code of {response.status_code}. {query}"
      )

    except (requests.exceptions.ReadTimeout, requests.exceptions.Timeout) as err:
      retries += 1
      if retries > max_retries:
        raise Exception(
          f"{datetime.now()} - start.gg request timed out after {timeout_seconds}s (max retries reached)."
        ) from err

      sleep_time = base_sleep_timeout * (2 ** min(retries, 6))
      msg = (
        f"{datetime.now()} - Read timeout after {timeout_seconds}s, retrying "
        f"({retries}/{max_retries}) in {sleep_time} seconds..."
      )
      logger.warning(msg)
      print(msg)
      time.sleep(sleep_time)

    except requests.exceptions.ConnectionError as err:
      retries += 1
      if retries > max_retries:
        raise Exception(f"{datetime.now()} - Max connection retries reached. Aborting.") from err
      msg = (
        f"{datetime.now()} - Connection error occurred, retrying ({retries}/{max_retries}) "
        f"in {base_sleep_connection} seconds..."
      )
      logger.warning(msg)
      print(msg)
      time.sleep(base_sleep_connection)