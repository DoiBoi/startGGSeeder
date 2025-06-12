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
    """
    Process an event, retrieving entrants and matches,
    and updating player ratings using the Glicko-2 algorithm.
    Args:
        event (dict): The event data containing entrants and matches.
        
    """
    
    if type(event) is int:
        event = q.run_query(q.events_query, {"id": event, "page": 0, "perPage": 512})['data']['event']

    entrant_ids = []
    matches = []

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
    
    results = {pid: [] for pid in players}
    for winner, loser in matches:
        w = players[winner]
        l = players[loser]
        results[winner].append((l.rating, l.rd, 1))
        results[loser].append((w.rating, w.rd, 0))
    
    for player_id, games in results.items():
        if games:
            r_list = [r for r, rd, score in games]
            rd_list = [rd for r, rd, score in games]
            outcome = [outcome for r, rd, outcome in games]
            players[player_id].update_player(r_list, rd_list, outcome)

    ranked = sorted(players.items(), key=lambda x: x[1].rating, reverse=True)

    print("\n🏆 Glicko-2 Rankings:")
    for pid, glicko_player in ranked:
        name = player_name_map.get(pid, "Unknown")
        print(f"{name}: Rating={glicko_player.rating:.2f}, RD={glicko_player.rd:.2f}, Vol={glicko_player.vol:.5f}")
    
    
    # f = open("items.json", "w")
    # f.write(json.dumps(sets, indent=2))
    # f.close


process_event(1284870)

