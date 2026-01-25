import requests
import time
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

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
  }
}
"""

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
query TournamentsQuery($country: String, $state: String, $id: [ID], $afterDate: Timestamp, $beforeDate: Timestamp, $perPage: Int, $page: Int) {
  tournaments(
    query: {filter: {countryCode: $country, addrState: $state, videogameIds: $id, afterDate: $afterDate, beforeDate: $beforeDate}, sort: endAt, perPage: $perPage, page: $page}
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
):
    """Yield tournament nodes from start.gg using page-based pagination.

    `after_date`/`before_date` are Unix timestamps (e.g. 1769320800).
    `videogame_ids` should be strings for GraphQL `ID`.
    """
    page = 1
    total_pages = 1
    while page <= total_pages:
        variables = {
            "country": country,
            "state": state,
            "id": videogame_ids,
            "afterDate": after_date,
            "beforeDate": before_date,
            "perPage": per_page,
            "page": page,
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
        for node in nodes:
            yield node
        page += 1


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
    url = os.getenv('SGG_API_URL')
    key = os.getenv('SGG_API_KEY')
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + str(key) # Replace with your own token
    }
    max_retries = 5  # Adjust as needed
    base_sleep = 30  # Base sleep time for exponential backoff (in seconds)
    
    while True:
        try:
            response = requests.post(
                str(url),
                json={'query': query, 'variables': variables},
                headers=headers,
                timeout=30  # Adjust timeout if necessary
            )
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                if (retries > 9):
                    retries = 0
                sleep_time = base_sleep * (2 ** retries)
                print(f"{datetime.now()} - Rate limit exceeded. Waiting for {sleep_time} seconds before retrying...")
                time.sleep(sleep_time)
                retries += 1
            elif response.status_code == 503:
                print(f"{datetime.now()} - Service unavailable. Waiting for 60 seconds before retrying...")
                time.sleep(60)
            else:
                raise Exception(f"{datetime.now()} - Query failed to run with a status code of {response.status_code}. {query}")
        except requests.exceptions.ConnectionError as err:
            retries += 1
            if retries > max_retries:
                raise Exception(f"{datetime.now()} - Max connection retries reached. Aborting.") from err
            print(f"{datetime.now()} - Connection error occurred, retrying ({retries}/{max_retries}) in 30 seconds...")
            time.sleep(30)