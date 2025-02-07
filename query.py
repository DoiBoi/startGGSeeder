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