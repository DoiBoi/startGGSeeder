import requests
import time
from datetime import datetime

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
			}
		}
	}
"""

entrants_w_sets_query = """
query EntrantsWithSets($entrantId: ID!, $page: Int!, $perPage: Int!) {
  paginatedSets (page: 0, perPage: 5) {
    nodes {
      winnerId
      slots {
        entrant {
          id
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
    url = 'https://api.start.gg/gql/alpha'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer c6df148d662dee8949027063fabc4a46'  # Replace with your own token
    }
    max_retries = 5  # Adjust as needed
    base_sleep = 30  # Base sleep time for exponential backoff (in seconds)
    
    while True:
        try:
            response = requests.post(
                url,
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