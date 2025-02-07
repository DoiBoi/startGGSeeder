import requests
import query as q
import json
import time

auth = open("auth.txt", "r")

def run_query(query, variables=None):
    url = 'https://api.start.gg/gql/alpha'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer' + auth.read()
    }
    while True:
        response = requests.post(url, json={'query': query, 'variables': variables}, headers=headers)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            print("Rate limit exceeded. Waiting for 60 seconds before retrying...")
            time.sleep(60)
        elif response.status_code == 503:
            print("Service unavailable. Waiting for 60 seconds before retrying...")
            time.sleep(60)
        else:
            raise Exception(f"Query failed to run by returning code of {response.status_code}. {query}")


def get_events_id(t_name):
    result = run_query(q.tournament_query, {"tourneySlug": t_name})
    events_id = {}
    for event in result['data']['tournament']['events']:
        events_id[event['slug']] = event['id']

    return events_id

def get_game_id(game_name):
    return run_query(q.gameId_query, {"name": game_name})["data"]["videogames"]["nodes"][0]["id"]

def get_event_players(t_name, byName=True):
    events_id = get_events_id(t_name)
    event_players = {}

    for event, event_id in events_id.items():
        players = []
        page = 1
        per_page = 50
        while True:
            result = run_query(q.players_query, {"eventId": event_id, "page": page, "perPage": per_page})
            entrants = result['data']['event']['entrants']['nodes']
            for entrant in entrants:
                for participant in entrant['participants']:
                    players.append(participant['player']['id'])
            if page >= result['data']['event']['entrants']['pageInfo']['totalPages']:
                break
            page += 1
        if byName:
            event_players[event] = players
        else:
            event_players[event_id] = players

    return event_players

def get_player_win_rate(player_id, videogame_id):
    variables = {
        "playerId": player_id,
        "page": 1,
        "perPage": 20
    }

    result = run_query(q.recent_sets_query, variables)
    sets = result['data']['player']['sets']['nodes']
    
    amount_of_wins = 0
    total_sets = 0
    for set in sets:
        # print(set)
        # Some ppl are registered without an account
        if set['event'] is None:
            return 0
        if set['event']['videogame']['id'] == videogame_id:
            total_sets += 1
            for slot in set['slots']:
                if slot['entrant']['participants'][0]['player']['id'] == player_id and set['winnerId'] == slot['entrant']['id']:
                    amount_of_wins += 1

    if total_sets == 0:
        return 0

    return float(amount_of_wins / total_sets)

def get_ordered_gamer_tags(tournament_name):
    event_players = get_event_players(tournament_name, True)
    player_win_rates = {}
    for event, players in event_players.items():
        players_dict = []
        for player_id in players:
            win_rate = get_player_win_rate(player_id, run_query(q.event_query, {"slug": event})['data']['event']['videogame']['id'])
            player_info = run_query(q.player_query, {"playerId": player_id})
            # print(player_info)
            gamer_tag = player_info['data']['player']['gamerTag']
            players_dict.append({gamer_tag: win_rate})
        player_win_rates[run_query(q.event_query, {"slug": event})["data"]['event']["name"]] = players_dict
    
    ordered_gamer_tags = {}
    for event, players in player_win_rates.items():
        ordered_gamer_tags[event] = sorted(players, key=lambda x: x[list(x.keys())[0]], reverse=True)
    return ordered_gamer_tags

print("Please enter the tournament name (https://www.start.gg/tournament/[this part]/details):")
tournament_name = input()
print("Writing to data.json, please wait...")
with open('data.json', 'w') as f:
    json.dump(get_ordered_gamer_tags(tournament_name), f, indent=4)
print("Done!")

