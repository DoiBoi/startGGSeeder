import os
import time
import pandas as pd
import query as q
import json
from datetime import datetime
import re
from glicko2 import Player

## Player by ID
players = {}

## Player by name
player_name_map = {}

## Entrant to Player mapping
entrant_to_player = {}

# Set to keep track of seen set IDs
seen_set_ids = set()

# test with oki 1
# event = q.run_query(q.tournament_query, {"tourneySlug": "ubc-fgc-summer-slam-5"})['data']['tournament']['events']
# result = q.run_query(q.resultsQuery, {
#     "slug": "ubc-fgc-summer-slam-5",
#     "perPage": 50,
#     "page": 1
# })

# print(result)

def process_tournament(eventSlug):
    """
    Process an event by its slug, retrieving entrants and matches,
    returning data to be processed by the Glicko-2 algorithm.

    Args:
        eventSlug (str): The slug of the event to process.

    """

    # get IDs of the events
    events = q.run_query(q.resultsQuery, {"slug": eventSlug})['data']['tournament']['events']
    query = """query EventsQuery("""
    for i in range(len(events)):
        query += f"$id{i}: ID!, "
    query += "$page: Int!, $perPage: Int!) {\n"
    for i in range(len(events)):
         query += f"""E{i}: event(id: $id{i})""" + """{
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
        }\n"""
    query += """}"""

    eventsEntrants = q.run_query(query, {
        **{f"id{i}": events[i]['id'] for i in range(len(events))},
        "page": 0,
        "perPage": 512
    })

    # shoves the players into the set and the map
    if not eventsEntrants['data']: return 
    for i in range(len(eventsEntrants['data'])):
        process_event(eventsEntrants['data'][f"E{i}"])
            

def process_event(event): 
    entrant_ids = []
    for entrant in event['entrants']['nodes']:
        player_id = entrant['participants'][0]['player']['id']
        player_name = entrant['participants'][0]['player']['gamerTag']
        entrant_id = entrant['id']
        if player_id not in players:
            players[player_id] = Player()
            player_name_map[player_id] = player_name
        entrant_to_player[entrant_id] = player_id
        entrant_ids.append(entrant_id)

    # get sets for each entrants
    query = """query EntrantsWithSets("""
    for i in range(len(entrant_ids)):
        query += f"$entrantId{i}: ID!, "
    query += "$page: Int!, $perPage: Int!) {\n"
    for i in range(len(entrant_ids)):
        entrant_id = entrant_ids[i]
        query += f"""E{i}: entrant(id: $entrantId{i})""" + """{  
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
        }\n"""
    query += """}"""

    sets = q.run_query(query, {
        **{f"entrantId{i}": entrant_ids[i] for i in range(len(entrant_ids))},
        "page": 0,
        "perPage": 10
    })
    f = open('items.json', 'w')
    f.write(json.dumps(sets, indent=2))
    f.close()
        


        

        




process_tournament("end-of-heights-3")
print(player_name_map)

