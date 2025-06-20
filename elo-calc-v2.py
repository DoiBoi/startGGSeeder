import os
import query as q
from glicko2 import Player
import csv
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

database_url = os.getenv("DATABASE_API_URL")
database_api = os.getenv("DATABASE_API_KEY")

supabase: Client = create_client(database_url, database_api)

# ## Player by ID
# players = {}

# ## Player by name


# ## Entrant to Player mapping
# entrant_to_player = {}

# batch_number = 0

def process_tournament(eventSlug, saved_games=False):
    """
    Process an event by its slug, retrieving entrants and matches,
    returning data to be processed by the Glicko-2 algorithm.

    Args:
        eventSlug (str): The slug of the event to process.
        players (dict): Optional; a dictionary of Player objects keyed by player ID.
    Returns:
        dict: A dictionary containing player ratings and other data.
    """

    print(f"Processing tournament {eventSlug}...")
    # get IDs of the events
    events = q.run_query(q.resultsQuery, {"slug": eventSlug})['data']['tournament']['events']

    if os.path.exists("data/videogame_map.csv"):
        with open("data/videogame_map.csv", "r", encoding='utf-8') as f:
            reader = csv.DictReader(f)
            videogame_map = {int(row['id']): row['name'] for row in reader}
    else:
        videogame_map = {}

    if saved_games:
        filtered_events = []
        for event in events:
            videogame_id = event['videogame']['id']
            if videogame_id in videogame_map:
                filtered_events.append(event)
        events = filtered_events

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
    if "errors" in eventsEntrants: 
        print(f"Error retrieving events for {eventSlug}: {eventsEntrants['errors']}")
        print(f"Previous query: {query}")
        return
    for i in range(len(eventsEntrants['data'])):
        videogame_id=eventsEntrants['data'][f"E{i}"]['videogame']['id']
        videogame_name=eventsEntrants['data'][f"E{i}"]['videogame']['name']
        if not saved_games:
            if videogame_id not in videogame_map:
                videogame_map[videogame_id] = videogame_name
            process_event(eventsEntrants['data'][f"E{i}"], videogame_id=videogame_id)
        else:
            if videogame_id in videogame_map:
                process_event(eventsEntrants['data'][f"E{i}"], videogame_id=videogame_id)
    with open("data/videogame_map.csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name"])
        if f.tell() == 0:  # Check if file is empty
            writer.writeheader()
        for vid, name in videogame_map.items():
            writer.writerow({"id": vid, "name": name})

def process_event(event, videogame_id = None):
    """
    Process an event, retrieving entrants and matches,
    and updating player ratings using the Glicko-2 algorithm.
    Args:
        event (dict or int): The event data (as a dict) or event ID (as an int).
        players (dict): Optional; a dictionary of Player objects keyed by player ID.
    
    """
    if not videogame_id:
        return
    if type(event) is int:
        event = q.run_query(q.events_query, {"id": event, "page": 0, "perPage": 512})['data']['event']
    max_batch_size = 25
    seen_sets = set()
    entrant_to_player = {}
    entrants = event['entrants']['nodes']

    entrant_ids = []

    if os.path.exists("data/player_name_map.csv"):
        with open("data/player_name_map.csv", "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            player_name_map = {int(row['player_id']): row['name'] for row in reader}
    else:
        player_name_map = {}
    if os.path.exists(f"data/{videogame_id}.csv"):
        players = deserialize_players(videogame_id)
    else:
        players = {}

    for entrant in entrants:
        player_id = entrant['participants'][0]['player']['id']
        player_name = entrant['participants'][0]['player']['gamerTag']
        entrant_id = entrant['id']
        if player_id not in players:
            players[player_id] = Player()
            players[player_id].appearances = 1
            player_name_map[player_id] = player_name
        else:
            # Increment appearances if already exists
            if hasattr(players[player_id], "appearances"):
                players[player_id].appearances += 1
            else:
                players[player_id].appearances = 1
        entrant_to_player[entrant_id] = player_id
        entrant_ids.append(entrant_id)

    for i in range(0, len(entrants), max_batch_size):
        batch = entrant_ids[i:i + max_batch_size]
        batch_number = i // max_batch_size + 1
        print(f"Processing batch {i // max_batch_size + 1} of {len(entrants) // max_batch_size + 1} for game {event['videogame']['name']}")
        
        seen_sets, players = process_player_batch(batch, players, entrant_to_player, seen_sets)
        # Save seen sets to a file for debugging
        # with open(f"data/seen_sets_{videogame_id}_{batch_number}.json", "w", encoding="utf-8") as f:
        #     json.dump(list(seen_sets), f, indent=2)

    serialize_players(players, player_name_map, videogame_id)
    # sorted_players = sorted(sanitized_players.items(), key=lambda x: x[1]['rating'], reverse=True)
    with open("data/player_name_map.csv", "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["player_id", "name"])
        if f.tell() == 0:
            writer.writeheader()
        for pid, name in player_name_map.items():
            writer.writerow({"player_id": pid, "name": name})
    player_data = [{"player_id": pid, "name": name} for pid, name in player_name_map.items()]
    supabase.table("player").upsert(player_data, on_conflict=["player_id"]).execute()
    return

def process_player_batch(entrant_ids, players, entrant_to_player, seen_sets=set()):
    """
    Process a batch of entrants, retrieving their matches and updating player ratings.
    Args:
        entrants (list): A list of entrant data dictionaries.
        videogame_id (int): The ID of the videogame.
        seen_sets (set): A set to keep track of already processed sets.
    Returns:
        set: A set of seen set IDs.
    """
    matches = []
    
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
        "perPage": 20
    })
    # with open(f"data/sets_debug_{videogame_id}_{batch_number}.json", "w", encoding="utf-8") as f:
    #     json.dump(sets, f, indent=2)
    
    if "errors" in sets:
        print(f"Error retrieving sets: {sets['errors']}")
        # print(f"Previous query: {query}")
        return

    for i in range(len(sets['data'])):
        entrant_sets = sets['data'][f"E{i}"]['paginatedSets']['nodes']
        # Reverse the order of sets
        entrant_sets = list(reversed(entrant_sets))
        for gameSet in entrant_sets:
            if gameSet['id'] in seen_sets:
                continue
            seen_sets.add(gameSet['id'])
            winner_id = gameSet['winnerId']
            e1 = gameSet['slots'][0]['entrant']['id']
            e2 = gameSet['slots'][1]['entrant']['id']
            # This should never happen, but just in case
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
    
    return seen_sets, players
    

def update_all_rankings_csv(players, player_name_map, videogame_id):
    """
    Update or append player rankings in data/all_rankings.csv.
    """
    all_rankings_path = "data/all_rankings.csv"
    fieldnames = ["player_id", "name", "rating", "rd", "vol", "game_id", "ranking", "appearances"]
    existing_rows = []

    # Read existing data if file exists
    if os.path.exists(all_rankings_path):
        with open(all_rankings_path, "r", encoding="utf-8", newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                existing_rows.append(row)

    existing_lookup = {(int(row["player_id"]), str(row["game_id"])): i for i, row in enumerate(existing_rows)}

    sorted_players = sorted(players.items(), key=lambda x: x[1].rating, reverse=True)
    player_rankings = {}
    for rank, (pid, player) in enumerate(sorted_players, start=1):
        player_rankings[pid] = rank

    for pid, player in players.items():
        key = (pid, str(videogame_id))
        ranking = player_rankings[pid]
        appearances = getattr(player, "appearances", 1)
        new_row = {
            "player_id": pid,
            "name": player_name_map.get(pid, "Unknown"),
            "rating": player.rating,
            "rd": player.rd,
            "vol": player.vol,
            "game_id": videogame_id,
            "ranking": ranking,
            "appearances": appearances
        }
        if key in existing_lookup:
            existing_rows[existing_lookup[key]] = {k: str(v) for k, v in new_row.items()}
        else:
            existing_rows.append({k: str(v) for k, v in new_row.items()})

    with open(all_rankings_path, "w", newline='', encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in existing_rows:
            writer.writerow(row)
    
    

def serialize_players(players, player_name_map, videogame_id):
    """
    Serialize player data to the Supabase 'ranking' table.
    """
    sorted_players = sorted(players.items(), key=lambda x: x[1].rating, reverse=True)
    player_rankings = {pid: rank for rank, (pid, _) in enumerate(sorted_players, start=1)}

    data = []
    for pid, player in sorted_players:
        appearances = getattr(player, "appearances", 1)
        data.append({
            "player_id": pid,
            "game_id": videogame_id,
            "name": player_name_map.get(pid, "Unknown"),
            "rating": player.rating,
            "rd": player.rd,
            "vol": player.vol,
            "ranking": player_rankings[pid],
            "appearances": appearances
        })

    # Upsert all player rankings for this game
    supabase.table("ranking").upsert(data, on_conflict=["player_id", "game_id"]).execute()

def deserialize_players(videogame_id):
    """
    Deserialize player data from the Supabase 'ranking' table for a specific game.
    """
    players = {}
    response = supabase.table("ranking").select("*").eq("game_id", videogame_id).execute()
    for row in response.data:
        player_id = int(row['player_id'])
        player = Player(
            rating=float(row['rating']),
            rd=float(row['rd']),
            vol=float(row['vol'])
        )
        player.appearances = int(row.get('appearances', 1))
        players[player_id] = player
    return players

## Scraping data (Run this once to get the data)
process_tournament("ubc-summer-slam")
# process_tournament("okizeme-46")
# process_tournament("ubc-summer-slam-2")
# process_tournament("ubc-summer-slam-3")
# process_tournament("sunset-series-2024", saved_games=True)
# process_tournament("ubc-sunset-showdown")
# process_tournament("okizeme-47")
# process_tournament("ubc-fgc-frenzy-friday-1")
# process_tournament("ubc-fgc-frenzy-friday-2")
# process_tournament("burnaby-boo-rawl", saved_games=True)
# process_tournament("ubc-fgc-frenzy-friday-3")
# process_tournament("pataka-esports-festival", saved_games=True)
# process_tournament("ubc-fgc-autumn-assault-1")
# process_tournament("ubc-fgc-frenzy-friday-4")
# process_tournament("ubc-fgc-frenzy-friday-5")
# process_tournament("goin-up", saved_games=True)
# process_tournament("ubc-fgc-frenzy-friday-6")
# process_tournament("okizeme-48")
# process_tournament("ubc-fgc-winter-wavedash-1")
# process_tournament("okizeme-49")
# process_tournament("ubc-fgc-frenzy-friday-7")
# process_tournament("ubc-fgc-frenzy-friday-8")
# process_tournament("ubc-fgc-frenzy-friday-9")
# process_tournament("end-of-heights-3")
# process_tournament("ubc-fgc-frenzy-friday-10")
# process_tournament("ubc-fgc-frenzy-friday-11")
# process_tournament("ubc-fgc-winter-wavedash-2")
# process_tournament("okizeme-50")
# process_tournament("ubc-fgc-frenzy-friday-12")
# process_tournament("cascadia-cup-road-to-bobc")
# process_tournament("battle-of-bc-7-6", saved_games=True)
# process_tournament("ubc-fgc-summer-slam-5")
# process_tournament("ubc-fgc-summer-slam-6")