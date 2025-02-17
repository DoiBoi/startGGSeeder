import requests
import time

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

def run_query(query, variables=None):
    url = 'https://api.start.gg/gql/alpha'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer c6df148d662dee8949027063fabc4a46'
    }
    while True:
        response = requests.post(url, json={'query': query, 'variables': variables}, headers=headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            print("Rate limit exceeded. Waiting for 30 seconds before retrying...")
            time.sleep(30)
        elif response.status_code == 503:
            print("Service unavailable. Waiting for 30 seconds before retrying...")
            time.sleep(30)
        else:
            raise Exception(f"Query failed to run by returning code of {response.status_code}. {query}")