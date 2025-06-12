import os
import time
import pandas as pd
import query as q
import json
from datetime import datetime
import re
from glicko2 import Player

# ## Player by ID
# players = {}

# ## Player by name
player_name_map = {}

# ## Entrant to Player mapping
# entrant_to_player = {}



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
        process_event(eventsEntrants['data'][f"E{i}"], eventsEntrants['data'][f"E{i}"]['numEntrants'])            

def process_event(event, players = {}):
    """
    Process an event, retrieving entrants and matches,
    and updating player ratings using the Glicko-2 algorithm.
    Args:
        event (dict or int): The event data (as a dict) or event ID (as an int).
        players (dict): Optional; a dictionary of Player objects keyed by player ID.
        player_name_map (dict): Optional; a dictionary mapping player IDs to names.
        entrant_to_player (dict): Optional; a dictionary mapping entrant IDs to player IDs.
    """
    if type(event) is int:
        event = q.run_query(q.events_query, {"id": event, "page": 0, "perPage": 512})['data']['event']

    entrant_ids = []
    matches = []
    entrant_to_player = {}

    for entrant in event['entrants']['nodes']:
        player_id = entrant['participants'][0]['player']['id']
        player_name = entrant['participants'][0]['player']['gamerTag']
        entrant_id = entrant['id']
        if player_id not in players:
            players[player_id] = Player()
            player_name_map[player_id] = player_name
        entrant_to_player[entrant_id] = player_id
        entrant_ids.append(entrant_id)

    # build query to get sets for each entrants
    query = """query EntrantsWithSets("""
    for i in range(len(entrant_ids)):
        query += f"$entrantId{i}: ID!, "
    query += "$page: Int!, $perPage: Int!) {\n"
    for i in range(len(entrant_ids)):
        entrant_id = entrant_ids[i]
        query += f"""E{i}: entrant(id: $entrantId{i})""" + """{  
            paginatedSets (page: $page, perPage: $perPage) {
                nodes {
                    id
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
    
    seen_sets = set()

    for i in range(len(sets['data'])):
        entrant_sets = sets['data'][f"E{i}"]['paginatedSets']['nodes']
        for gameSet in entrant_sets:
            if gameSet['id'] in seen_sets:
                continue
            seen_sets.add(gameSet['id'])
            winner_id = gameSet['winnerId']
            e1 = gameSet['slots'][0]['entrant']['id']
            e2 = gameSet['slots'][1]['entrant']['id']
            if e1 not in entrant_to_player or e2 not in entrant_to_player:
                continue
            p1 = entrant_to_player[e1]
            p2 = entrant_to_player[e2]

            if winner_id == e1:
                matches.append((p1,p2))
            elif winner_id == e2:
                matches.append((p2,p1)) 
    
    # look through matches and append results
    results = {pid: [] for pid in players}
    for winner, loser in matches:
        w = players[winner]
        l = players[loser]
        results[winner].append((l.rating, l.rd, 1))
        results[loser].append((w.rating, w.rd, 0))
    
    # update players with results
    for player_id, games in results.items():
        if games:
            r_list = [r for r, rd, score in games]
            rd_list = [rd for r, rd, score in games]
            outcome = [outcome for r, rd, outcome in games]
            players[player_id].update_player(r_list, rd_list, outcome)

    return players 
    
def serialize_players(players, player_name_map):
    """ Serialize player data to a JSON string.
    Args:
        players (dict): A dictionary of Player objects keyed by player ID.
        player_name_map (dict): A dictionary mapping player IDs to names.
    returns:
        dict: A dictionary containing serialized player data.
    """
    return {
        player_id: { 
            "name": player_name_map.get(player_id, "Unknown"),
            "rating": player.rating,
            "rd": player.rd,
            "vol": player.vol,
        }
        for player_id, player in players.items()
    }

def deserialize_players(json_path):
    """
    Deserialize player data from a JSON file.
    Args:
        json_path (str): The path to the JSON file containing player data.
    Returns:
        dict: A dictionary of Player objects keyed by player ID.
    """
    with open(json_path, "r") as f:
        data = json.load(f)
    players = {}
    for pid, values in data.items():
        p = Player()
        p.rating = values["rating"]
        p.rd = values["rd"]
        p.vol = values["vol"]
        players[pid] = p
    return players

with open("glicko_ratings.json", "w") as f:
    json.dump(serialize_players(process_event(1284870), player_name_map), f, indent=2)

