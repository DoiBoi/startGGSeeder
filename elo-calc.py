import os
import time
import pandas as pd
import query as q
import json
from datetime import datetime

# Global videogames registry; keys are sanitized game names.
videogames = {}

unnamed_okizeme = ["okizeme", "okizeme-countdown-30", "okizeme-3-1", "okizeme-the-final",
               "okizeme-14-1", "okizeme-a-fighting-game-monthly", "okizeme-19-2"]

def get_player_ids_and_games(tournament_name):
    """
    Retrieves each player's ID and the unique videogames they've played,
    then returns the results in a Pandas DataFrame.
    """
    i = 1
    player_names = []
    player_ids = []
    entrants = []
    while True:
        result = q.run_query(q.participant_query, {"tourneySlug": tournament_name, "page": i, "perPage": 200})
        if result['data']['tournament'] is None:
            print("Tournament not found.")
            break
        participants = result['data']['tournament']['participants']['nodes']
        for participant in participants:
            player_ids.append(participant['player']['id'])
            player_names.append(participant['player']['gamerTag'])
            entrants.append(participant['entrants'])
        totalPages = result['data']['tournament']['participants']['pageInfo']['totalPages']
        if i > totalPages + 1:
            break
        i += 1
    return pd.DataFrame({'player_id': player_ids, 'player_name': player_names, 'entrants': entrants})

def append_to_dataframe(df, row):
    if row['player_id'] in df['player_id'].values:
        idx = df.index[df['player_id'] == row['player_id']][0]
        existing_entrants = df.at[idx, 'entrants']
        if row['entrants'] is not None and existing_entrants is not None:
            merged_entrants = existing_entrants + row['entrants']
            df.at[idx, 'entrants'] = merged_entrants
        elif row['entrants'] is not None:
            df.at[idx, 'entrants'] = row['entrants']
    else:
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
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

def iterate_through_tournament_series(tournament_series, num_tournaments, df=None):
    data = {'player_id': [], 'player_name': [], 'entrants': []}
    new_df = pd.DataFrame(data) if df is None else df
    for i in range(1, num_tournaments + 1):
        print("Reading Tournament: " + tournament_series + "-" + str(i))
        ith_df = get_player_ids_and_games(f"{tournament_series}-{i}")
        for index, row in ith_df.iterrows():
            new_df = append_to_dataframe(new_df, row)
    return new_df

def iterate_through_tournament_array(tournaments, df=None):
    data = {'player_id': [], 'player_name': [], 'entrants': []}
    new_df = pd.DataFrame(data) if df is None else df
    for tournament in tournaments:
        print("Reading Tournament: " + tournament)
        ith_df = get_player_ids_and_games(tournament)
        for index, row in ith_df.iterrows():
            new_df = append_to_dataframe(new_df, row)
    return new_df

def append_tournament(df, tournament_name):
    ith_df = get_player_ids_and_games(tournament_name)
    for index, row in ith_df.iterrows():
        df = append_to_dataframe(df, row)
    return df

def process_batch(batch):
    """
    Processes a batch of entrant objects (max 80) by sending a single GraphQL request.
    Prints batch details for debugging and returns a list of points for each entrant in order.
    """
    num_events = len(batch)
    query_str = q.create_entrant_query(num_events)
    variables = {}
    for i, entrant in enumerate(batch):
        variables[f"E{i+1}"] = entrant['id']
    # Print batch details
    print(f"{datetime.now()} - Processing batch of {len(batch)} entrants.")
    
    response = q.run_query(query_str, variables)
    points_list = []
    # Process each response from the batch
    for i in range(1, num_events + 1):
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
                if placement > 0:
                    pt = float(numEntrants / placement)
                    points_list.append(pt)
                else:
                    points_list.append(0.0)
            else:
                points_list.append(0.0)
    return points_list

def generate_points_batch(df, target_game_id):
    """
    Processes the entire players DataFrame to compute and sum points per player
    for entrants corresponding to the target videogame.
    
    Returns:
        dict: Mapping from player_id to total points.
    """
    player_points = {}
    batch = []       # List of entrant objects in the current batch.
    batch_player = []  # Parallel list holding the corresponding player_id for each entrant.
    # Iterate through each row in the players DataFrame.
    for _, row in df.iterrows():
        entrants = row.get('entrants', [])
        if entrants is None: continue
        for entrant in entrants:
            # Skip disqualified entrants.
            if entrant.get('isDisqualified', False):
                continue
            # Check if the entrant's event is for the target videogame.
            if entrant['event']['videogame']['id'] == target_game_id:
                batch.append(entrant)
                batch_player.append(row['player_id'])
                # When the batch reaches 80, process it.
                if len(batch) == 80:
                    results = process_batch(batch)
                    for pid, pt in zip(batch_player, results):
                        player_points[pid] = player_points.get(pid, 0.0) + pt
                    batch = []
                    batch_player = []
    # Process any remaining entrants in the last batch.
    if batch:
        results = process_batch(batch)
        for pid, pt in zip(batch_player, results):
            player_points[pid] = player_points.get(pid, 0.0) + pt
    return player_points

def generate_elo(df):
    """
    For each videogame in the global 'videogames' dictionary, this function
    computes an ELO ranking DataFrame where each player's points (summed across all
    of their matching entrants) are calculated in batches.
    
    Returns:
        dict: Mapping from game display name to its corresponding ELO ranking DataFrame.
    """
    elo_dict = {}
    for game_name, game_info in videogames.items():
        game_id = game_info["id"]
        # Compute points for all players for this game using batch queries.
        player_points = generate_points_batch(df, game_id)
        elo_data = []
        for player_id, pts in player_points.items():
            if pts > 0:
                # Get the player's name from the DataFrame.
                player_name = df.loc[df['player_id'] == player_id, 'player_name'].iloc[0]
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
    # Get all games from both dictionaries.
    all_games = set(old_elo.keys()).union(new_elo.keys())
    
    for game in all_games:
        if game in old_elo and game in new_elo:
            # Concatenate and sum points for matching players.
            combined = pd.concat([old_elo[game], new_elo[game]])
            combined = combined.groupby(["player_id", "player_name"], as_index=False).agg({"points": "sum"})
        elif game in new_elo:
            combined = new_elo[game].copy()
        else:
            combined = old_elo[game].copy()
        
        # Sort the updated ranking by points in descending order.
        combined.sort_values(by="points", ascending=False, inplace=True)
        combined.reset_index(drop=True, inplace=True)
        updated_elo[game] = combined
    
    return updated_elo

def save_players(df, file_path="data/players.json"):
    df.to_json(file_path, orient="records", indent=2)
    print(f"Player data saved to {file_path}")

def load_players(file_path="data/players.json"):
    if os.path.exists(file_path):
        return pd.read_json(file_path)
    else:
        return pd.DataFrame()

def update_players_df(old_players, new_players):
    # Combine the two DataFrames.
    combined = pd.concat([old_players, new_players], ignore_index=True)
    # Drop duplicates based on player_id and keep the latest record.
    combined = combined.drop_duplicates(subset=['player_id'], keep='last')
    combined.reset_index(drop=True, inplace=True)
    return combined

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

# ----- Main Execution -----

# At startup, you can load the registry using:
videogames = load_videogames()
loaded_elo = load_elo_dataframes(videogames)
loaded_players = load_players()

# Scrape Data
# tournament_input = input("Enter the tournament name(s): ")
# tournament_list = tournament_input.split()
# players_df = iterate_through_tournament_array(tournament_list)
players_df = iterate_through_tournament_series("okizeme", 49)
new_players = update_players_df(loaded_players, players_df)
print(new_players)
print("Videogames:", videogames)

# Generate ELO ranking DataFrames for each videogame.
elo_dataframes = generate_elo(players_df)
print("ELO Dataframes:", elo_dataframes)

updated_elo = update_saved_elo(loaded_elo, elo_dataframes)
for game, df_game in updated_elo.items():
    print(f"ELO for {game}:")
    print(df_game)

# Export each game's ELO ranking to CSV and update videogames dictionary to store the file path.
save_game_elo_data(videogames, updated_elo)

# Save the videogames registry with elo ranking file paths.
save_videogames(videogames)
save_players(new_players)


