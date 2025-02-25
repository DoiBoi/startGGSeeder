import os
import time
import pandas as pd
import query as q
import json
from datetime import datetime
import re

# Global videogames registry; keys are sanitized game names.
videogames = {}

other_vsb_events = ["okizeme", "okizeme-countdown-30", "okizeme-3-1", "okizeme-the-final",
               "okizeme-14-1", "okizeme-a-fighting-game-monthly", "okizeme-19-2", "burnaby-boo-rawl"]
ubc_events = ["end-of-heights-tournament-2", "ubc-matchup-monday-4-1", "ubc-matchup-monday-5-1", "ubc-summer-slam", "ubc-summer-slam-2", "ubc-summer-slam-3", "ubc-sunset-showdown",
              "end-of-heights-3", "ubc-fgc-autumn-assault-1", "ubc-fgc-winter-wavedash-1"]

def get_player_ids_and_games(tournament_name, videogame_list, existing_games=False):
    """
    Retrieves each player's ID and the unique videogames they've played,
    then returns the results in a Pandas DataFrame.
    """
    i = 1
    player_names = []
    player_ids = []
    entrants = []
    alias = []
    while True:
        result = q.run_query(q.participant_query, {"tourneySlug": tournament_name, "page": i, "perPage": 150})
        if result['data']['tournament'] is None:
            print("Tournament not found.")
            break
        participants = result['data']['tournament']['participants']['nodes']
        for participant in participants:
            player_ids.append(participant['player']['id'])
            player_names.append(participant['player']['gamerTag'])
            if existing_games and participant['entrants'] is not None:
                entrants_filtered = []
                for entrant in participant['entrants']:
                    if entrant['event']['videogame']['id'] in videogame_list:
                        entrants_filtered.append(entrant)
                entrants.append(entrants_filtered)
            else: entrants.append(participant['entrants'])
            if participant['entrants'] is not None: 
                aliases = []
                for entrant in participant['entrants']:
                    if entrant['name'] not in aliases:
                        aliases.append(entrant['name'])
                alias.append(aliases)
            else: 
                alias.append([])
        totalPages = result['data']['tournament']['participants']['pageInfo']['totalPages']
        if i > totalPages + 1:
            break
        i += 1
    return pd.DataFrame({'player_id': player_ids, 'player_name': player_names, 'entrants': entrants, 'alias': alias}), entrants

def append_to_dataframe(df, row): 
    if row['player_id'] in df['player_id'].values:
        idx = df.index[df['player_id'] == row['player_id']][0]

        existing_entrants = df.at[idx, 'entrants']
        new_entrants = row['entrants']
        if not isinstance(existing_entrants, list):
            existing_entrants = [existing_entrants] if existing_entrants is not None else []
        if not isinstance(new_entrants, list):
            new_entrants = [new_entrants] if new_entrants is not None else []
        if new_entrants:
            merged_entrants = existing_entrants + new_entrants
            df.at[idx, 'entrants'] = merged_entrants

        existing_alias = df.at[idx, 'alias']
        new_alias = row['alias']
        if new_alias:
            for alias in new_alias:
                if alias not in existing_alias:
                    existing_alias.append(alias)
        df.at[idx, 'alias'] = existing_alias
    else:
        new_row = row.copy()
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        
    update_videogame_registry(row, videogames)
    return df

def sanitize_game_name(game_name):
    return "".join(c if c.isalnum() else "_" for c in game_name)

def save_videogames(videogames_dict, file_path="data/games.json"):
    with open(file_path, "w") as f:
        json.dump(videogames_dict, f, indent=2)
    print(f"Videogames registry saved to {file_path}")

def load_videogames(file_path="data/games.json"):
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    else:
        return {}

def update_videogame_registry(row, videogames):
    if row['entrants'] is not None:
        for entrant in row['entrants']:
            game = entrant['event']['videogame']
            display_name = game['displayName']
            safe_name = sanitize_game_name(display_name)
            if safe_name not in videogames:
                # Initially, no elo ranking file path is assigned.
                videogames[safe_name] = {
                    "id": game['id'],
                    "displayName": display_name,
                    "eloPath": ""
                }

def iterate_through_tournament_series(tournament_series, num_tournaments, df=pd.DataFrame({'player_id': [], 'player_name': [], 'entrants': [], 'alias': []}), 
                                      start=1, existing_games=False, videogames={}):
    entrants_total = []
    for i in range(start, num_tournaments + 1):
        print(f"{datetime.now()} - Reading Tournament: " + tournament_series + "-" + str(i))
        ith_df, entrants = get_player_ids_and_games(f"{tournament_series}-{i}", videogames.values(), existing_games=False)
        entrants_total += entrants
        for index, row in ith_df.iterrows():
            df = append_to_dataframe(df, row)
    return df, entrants_total

def iterate_through_tournament_array(tournaments, df=pd.DataFrame({'player_id': [], 'player_name': [], 'entrants': [], 'alias': []}), 
                                     existing_games=False, videogames={}):
    entrants_total = []
    for tournament in tournaments:
        print(f"{datetime.now()} - Reading Tournament: " + tournament)
        ith_df, entrants = get_player_ids_and_games(tournament, videogames.values(), existing_games)
        entrants_total += entrants
        for index, row in ith_df.iterrows():
            df = append_to_dataframe(df, row)
    return df, entrants_total

def append_tournament(df, tournament_name, videogames={}, 
                      existing_games=False):
    print(f"{datetime.now()} - Reading Tournament: " + tournament_name)
    ith_df, entrants = get_player_ids_and_games(tournament_name, videogames.values(), existing_games)
    for index, row in ith_df.iterrows():
        df = append_to_dataframe(df, row)
    return df, entrants

def get_player_id_by_name(name, players_df):
    """
    Searches the players_df for a row where the player's name (or one of their aliases) matches the given name.
    Returns the player_id if found, otherwise None.
    """
    for _, row in players_df.iterrows():
        # Check if player's actual name matches.
        if row['player_name'] == name:
            return row['player_id']
        # Check aliases (if they are stored as a list).
        aliases = row.get('alias')
        if isinstance(aliases, list) and name in aliases:
            return row['player_id']
    return None

def update_points_on_upset(entrant_sets, players_df, game_elo_df, player_points_dict):
    """
    Uses the displayScore in nodes to look for an upset: extracts two names and scores.
    Then, finds the respective player_ids using players_df and retrieves their current points
    from game_elo_df. If the winning player's (name1) point total is less than the opponent's (name2),
    adds bonus points of half the opponent's points.
    """
    # Regex pattern: first player's name (possibly with spaces), its score, a dash, second player's name, its score.
    pattern = r"(.+?)\s+([\d\.]+)\s*-\s*(.+?)\s+([\d\.]+)"
    for ent in entrant_sets:
        display_score = ent.get("displayScore", "")
        if display_score:
            match = re.search(pattern, display_score)
        else:
            match = None
        if match:
            # Extract the names and scores from displayScore.
            name1, s1, name2, s2 = match.groups()
            score1 = float(s1)
            score2 = float(s2)
            # Look up the respective player_ids using players_df.
            id1 = get_player_id_by_name(name1, players_df)
            id2 = get_player_id_by_name(name2, players_df)
            id_name = get_player_id_by_name(name2, players_df) 
            # Proceed only if both IDs were found and the game Elo data exists.
            if id1 is not None and id2 is not None and game_elo_df is not None:
                pts1_series = game_elo_df.loc[game_elo_df['player_id'] == id1, 'points']
                pts2_series = game_elo_df.loc[game_elo_df['player_id'] == id2, 'points']
                if not pts1_series.empty and not pts2_series.empty:
                    pts1 = pts1_series.iloc[0]
                    pts2 = pts2_series.iloc[0]
                    # print(id1, id2, id_name)
                    if pts1 < pts2 and score1 > score2:
                        player_points_dict[id1] = player_points_dict.get(id1, 0.0) + pts2 / 4
                        print(f"{datetime.now()} - Upset detected! {name1} {score1} - {score2} {name2}, adding {pts2/4} points to {name1}.")
                    if pts2 < pts1 and score2 > score1:
                        player_points_dict[id2] = player_points_dict.get(id1, 0.0) + pts1 / 4
                        print(f"{datetime.now()} - Upset detected! {name1} {score1} - {score2} {name2}, adding {pts2/4} points to {name2}.")
    return

def process_batch(response, num_entrants):
    """
    Processes a batch of entrant objects (max 60) by sending a single GraphQL request.
    Points are calculated as (numEntrants/placement) and then adjusted based on displayScore upset bonus.
    The extra parameters players_df and game_elo_df (the Elo ranking DataFrame for the game)
    are passed to update_points_on_upset.
    
    Returns a list of points (one per entrant in the batch).
    """    
    points_list = []
    for i in range(1, num_entrants + 1):
        key = f"E{i}"
        entrant_resp = response.get("data", {}).get(key)
        if entrant_resp is None:
            points_list.append(0.0)
        else:
            standing = entrant_resp.get("standing")
            event = entrant_resp.get("event")
            if standing is not None and event is not None:
                numEntrants = event.get("numEntrants", 0)
                placement = standing.get("placement", 0)
                pt = 0.0
                if placement > 0:
                    pt = float(numEntrants / placement)
                points_list.append(pt)
            else:
                points_list.append(0.0)
    return points_list

def generate_points_batch(df, target_game_id, entrants_list):
    # This version traverses only the passed-in entrants_list
    # instead of the entire df.
    current_game_elo_df = None
    safe_name = None
    for key, info in videogames.items():
        if info["id"] == target_game_id:
            safe_name = key
            break
    if safe_name:
        file_path = videogames[safe_name].get("eloPath", "")
        if file_path and os.path.exists(file_path):
            try:
                current_game_elo_df = pd.read_csv(file_path)
            except Exception as e:
                print(f"{datetime.now()} - Error loading Elo CSV for {safe_name}: {e}")

    player_points = {}
    batch = []
    batch_player = []
    for player_entrant in entrants_list:
        if player_entrant is None:
            continue
        for entrant in player_entrant:
            # print(entrant)
            # Skip disqualified entrants.
            if entrant.get('isDisqualified', False):
                continue
            if entrant['event']['videogame']['id'] == target_game_id:
                batch.append(entrant)
                pid = get_player_id_by_name(entrant['name'], df)  # or however you track player IDs
                batch_player.append(pid)
                if len(batch) == 60:
                    # Same batch logic as before
                    print(f"{datetime.now()} - Processing batch of {len(batch)} entrants...")
                    num_entrants = len(batch)
                    query_str = q.create_entrant_query(num_entrants)
                    variables = {}
                    for i, ent in enumerate(batch):
                        variables[f"E{i+1}"] = ent['id']
                    response = q.run_query(query_str, variables)
                    results = process_batch(response, num_entrants)
                    for pid, pt in zip(batch_player, results):
                        player_points[pid] = player_points.get(pid, 0.0) + pt
                    for i in range(1, num_entrants + 1):
                        entr_response = response.get("data", {}).get(f"E{i}")
                        if entr_response:
                            if entr_response['paginatedSets']:
                                nodes = entr_response['paginatedSets'].get("nodes", [])
                                if nodes:
                                    update_points_on_upset(nodes, df, current_game_elo_df, player_points)
                    batch = []
                    batch_player = []

    # Handle any remaining batch
    if batch:
        print(f"{datetime.now()} - Processing batch of {len(batch)} entrants...")
        num_entrants = len(batch)
        query_str = q.create_entrant_query(num_entrants)
        variables = {}
        for i, ent in enumerate(batch):
            variables[f"E{i+1}"] = ent['id']
        response = q.run_query(query_str, variables)
        results = process_batch(response, num_entrants)
        for pid, pt in zip(batch_player, results):
            player_points[pid] = player_points.get(pid, 0.0) + pt
        for i in range(1, num_entrants + 1):
            entr_response = response.get("data", {}).get(f"E{i}")
            if entr_response:
                nodes = entr_response.get("paginatedSets", {}).get("nodes", [])
                if nodes:
                    update_points_on_upset(nodes, df, current_game_elo_df, player_points)
    return player_points

def generate_elo(player_df, new_entrants):
    """
    For each videogame in the global 'videogames' dictionary, computes an Elo ranking DataFrame
    where points are calculated for the newly added entrants only.
    """
    elo_dict = {}
    for game_name, game_info in videogames.items():
        game_id = game_info["id"]
        print(f"{datetime.now()} - Generating ELO for {game_name}... ")
        # Pass new_entrants to generate_points_batch
        player_points = generate_points_batch(player_df, game_id, new_entrants)
        elo_data = []
        for player_id, pts in player_points.items():
            if pts > 0:
                player_name = player_df.loc[player_df['player_id'] == player_id, 'player_name'].iloc[0]
                elo_data.append({
                    'player_id': player_id,
                    'player_name': player_name,
                    'points': pts
                })
        if elo_data:
            elo_df = pd.DataFrame(elo_data)
            elo_df.sort_values(by='points', ascending=False, inplace=True)
            elo_df.reset_index(drop=True, inplace=True)
            elo_dict[game_name] = elo_df
    return elo_dict

def load_elo_dataframes(videogames):
    elo_dict = {}
    for safe_name, game_info in videogames.items():
        file_path = game_info.get("eloPath", "")
        if file_path and os.path.exists(file_path):
            try:
                elo_df = pd.read_csv(file_path)
                elo_dict[safe_name] = elo_df
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
        else:
            print(f"ELO ranking file not found for game {game_info.get('displayName', safe_name)} (expected at {file_path}).")
    return elo_dict

def update_saved_elo(old_elo, new_elo):
    updated_elo = {}
    all_games = set(old_elo.keys()).union(new_elo.keys())

    for game in all_games:
        if game in old_elo and game in new_elo:
            # Concatenate and sum points for matching players
            combined = pd.concat([old_elo[game], new_elo[game]], ignore_index=True)
            combined = combined.groupby(["player_id", "player_name"], as_index=False)["points"].sum()
        elif game in new_elo:
            combined = new_elo[game].copy()
        else:
            combined = old_elo[game].copy()

        combined.sort_values(by="points", ascending=False, inplace=True)
        combined.reset_index(drop=True, inplace=True)
        updated_elo[game] = combined

    return updated_elo

def save_players(df, file_path="data/players.json"):
    df.to_json(file_path, orient="records", indent=2)
    print(f"Player data saved to {file_path}")

def load_players(file_path="data/players.json"):
    if os.path.exists(file_path):
        return pd.read_json(file_path, orient="records")
    else:
        return pd.DataFrame()

def update_players_df(old_players, new_players):
    if old_players.empty: return new_players
    for index, row in new_players.iterrows():
        old_players = append_to_dataframe(old_players, row)
    return old_players

def save_game_elo_data(videogames, elo_dataframes):
    for game, df_game in elo_dataframes.items():
        safe_game_name = sanitize_game_name(game)
        file_path = os.path.join("data", safe_game_name + ".csv")
        df_game.to_csv(file_path, index=False)
        if safe_game_name in videogames:
            videogames[safe_game_name]["eloPath"] = file_path
        else:
        # In case the game was not in the registry (shouldn't happen normally)
            videogames[safe_game_name] = {"id": None, "displayName": game, "eloPath": file_path}

def to_string(updated_elo):
    for game, df_game in updated_elo.items():
        print(f"ELO for {game}:")
        print(df_game)

# ----- Main Execution -----

# At startup, you can load the registry using:
videogames = load_videogames()
loaded_elo = load_elo_dataframes(videogames)
loaded_players = load_players()


# Scrape Data

# print(new_players)
# print("Videogames:", videogames)

# Initiate the okis and ubc events
players_df, entrants = iterate_through_tournament_series("party-battle", 6)
elo_dataframes = generate_elo(players_df, entrants)
updated_elo = update_saved_elo(loaded_elo, elo_dataframes)
save_game_elo_data(videogames, updated_elo)

# players_df, entrants = iterate_through_tournament_series("okizeme", 10)
# elo_dataframes = generate_elo(players_df, entrants)
# updated_elo = update_saved_elo(updated_elo, elo_dataframes)
# save_game_elo_data(videogames, updated_elo)

# players_df, entrants = iterate_through_tournament_series("okizeme", 20, start=11)
# elo_dataframes = generate_elo(players_df, entrants)
# updated_elo = update_saved_elo(updated_elo, elo_dataframes)
# save_game_elo_data(videogames, updated_elo)

# players_df, entrants = iterate_through_tournament_series("okizeme", 30, start=21)
# elo_dataframes = generate_elo(players_df, entrants)
# updated_elo = update_saved_elo(updated_elo, elo_dataframes)
# save_game_elo_data(videogames, updated_elo)

# players_df, entrants = iterate_through_tournament_series("okizeme", 40, start=31)
# elo_dataframes = generate_elo(players_df, entrants)
# updated_elo = update_saved_elo(updated_elo, elo_dataframes)
# save_game_elo_data(videogames, updated_elo)

# players_df, entrants = iterate_through_tournament_series("ubc-fgc-frenzy-friday", 11)
# elo_dataframes = generate_elo(players_df, entrants)
# updated_elo = update_saved_elo(updated_elo, elo_dataframes)
# save_game_elo_data(videogames, updated_elo)

# players_df, entrants = iterate_through_tournament_series("okizeme", 49, start=41)
# elo_dataframes = generate_elo(players_df, entrants)
# updated_elo = update_saved_elo(updated_elo, elo_dataframes)
# save_game_elo_data(videogames, updated_elo)

# players_df, entrants = iterate_through_tournament_array(ubc_events)
# elo_dataframes = generate_elo(players_df, entrants)
# updated_elo = update_saved_elo(updated_elo, elo_dataframes)
# save_game_elo_data(videogames, updated_elo)

# players_df, entrants = iterate_through_tournament_array(["ubc-fgc-winter-wavedash-2"])
# elo_dataframes = generate_elo(players_df, entrants)
# updated_elo = update_saved_elo(updated_elo, elo_dataframes)
# save_game_elo_data(videogames, updated_elo)

new_players = update_players_df(loaded_players, players_df)
save_videogames(videogames)
save_players(new_players)

def run(tournament_name, type, num=1, start=1):
    if type == "array":
        players_df, entrants = iterate_through_tournament_array(tournament_name)
    if type == "series":
        players_df, entrants = iterate_through_tournament_series(tournament_name, num, start=start)
    elo_dataframes = generate_elo(players_df, entrants)
    updated_elo = update_saved_elo(loaded_elo, elo_dataframes)

    save_game_elo_data(videogames, updated_elo)
    new_players = update_players_df(loaded_players, players_df)
    save_videogames(videogames)
    save_players(new_players)

type = input("Enter type (serires or array): ")
if type == "series":
    num = int(input("Enter number of tournaments: "))
    start = int(input("Enter starting tournament: "))
    tournament_name = input("Enter tournament name: ")
    run(tournament_name, type, num, start)
elif type == "array":
    tournament_name = input("Enter tournament name: ")
    tournament_name = tournament_name.split(" ")
    run(tournament_name, type)