from typing import List
from utils.connections.supabaseClient import SupabaseClient
from utils.connections.startgg import StartGGClient
from glicko2 import Player 
import math
from collections import defaultdict
from utils.players import Players
from utils.videogames import Videogames
from datetime import datetime,timezone
import time


def new_player_entry():
    return {
        "player": Player(),
        "appearances": 0,
    }

ENTRANTID_PERPAGE = 300
SET_PERPAGE = 195
BATCH_SIZE = 1_000

class Tournament:
    def __init__(self, 
                 supabaseClient: SupabaseClient, 
                 startGGClient: StartGGClient,
                 slug: str = None,
                 savedGames = False):
        self.supabase = supabaseClient.getClient()
        self.startgg = startGGClient
        self.slug = slug if slug is not None else None
        self.ids = {}
        self.entrants = {}
        self.sets = {}
        self.players = defaultdict(lambda: defaultdict(new_player_entry)) 
        self.new_players = set()
        self.saved_games = savedGames
        pass

    def getEvents(self, slug=None) -> List[object]:
        if not self.slug:
            self.slug = slug
        
        response = self.startgg.runQuery(name="getEvents", variables={
            "slug": self.slug
        })
        
        print(f"Retrieving {self.slug}")
        self.name = response["data"]["tournament"]["name"]
        self.id = response["data"]["tournament"]["id"]
        self.startAt = response["data"]["tournament"]["startAt"]

        if self.saved_games:
            games = self.supabase.table("videogame_mapping").select("id").execute().data
            games = [game["id"] for game in games]
            self.ids = {event["id"]: event["videogame"]["id"] for event in response["data"]["tournament"]["events"] if event["videogame"]["id"] in games}
        else:
            self.ids = {event["id"]: event["videogame"]["id"] for event in response["data"]["tournament"]["events"]}

        return self.ids
    
    def getEntrants(self, ids=None) -> List[object]: 
        for id in self.ids:
            pages = self.startgg.runQuery(name="getTotalPages", variables={
                "id": id,
                "perPage": ENTRANTID_PERPAGE,
                "page": 1
            })
            event_entrants = {}
            print(f"Processing {pages["data"]["event"]["name"]}")
            totalPages = pages["data"]["event"]["entrants"]["pageInfo"]["totalPages"]
            for i in range(1, totalPages + 1):
                response = self.startgg.runQuery(name="getEntrantIds", variables={
                    "id": id,
                    "perPage": ENTRANTID_PERPAGE,
                    "page": i
                })
                response = response["data"]
                entrants = {node["id"]: [player["player"]["id"] for player in node["participants"]] 
                            for node in response["event"]["entrants"]["nodes"]}
                event_entrants.update(entrants)
            self.entrants.update({
                id: event_entrants
            })
            self.player_ids = {
                player_id 
                for event_entrants in self.entrants.values()
                for entrant_players in event_entrants.values()
                for player_id in entrant_players 
            }
        return self.entrants
    
    def transformSets(self, set, event):
        display_score = set["displayScore"]
        if not display_score or "DQ" in display_score:
            return
        ids = [slot["entrant"]["id"] for slot in set["slots"]]
        winner = set["winnerId"]
        loser = ids[0] if ids[1] == winner else ids[1]
        winner = self.entrants[event][winner]
        loser = self.entrants[event][loser]
        return { "winner": winner, "loser": loser }
    
    def getSets(self):        
        if not self.ids:
            self.getEvents()
        if not self.entrants: 
            self.getEntrants()
        print("Processing Sets...")
        for event_id, videogame_id in self.ids.items():
            response = self.startgg.runQuery(name="getSetPages", variables={
               "id": event_id,
               "page": 1,
               "perPage": SET_PERPAGE
            })
            totalPages = response["data"]["event"]["sets"]["pageInfo"]["totalPages"]
            sets = []
            for page in range(1, totalPages + 1):
                response = self.startgg.runQuery(name="getSets", variables= {
                    "id": event_id,
                    "page": page,
                    "perPage": SET_PERPAGE
                })
                sets.extend(response["data"]["event"]["sets"]["nodes"])
            transformedSets = list(map(lambda set: self.transformSets(set, event_id), sets))
                
            self.sets[videogame_id] = transformedSets
        print("Done")
        return self.sets
    
    def populateDataFromSupabase(self):
        print("Pulling data from supabase")
        data = []
        lastId = None
        while True:
            query = (
                self.supabase
                    .table("ranking")
                    .select("*")
                    .order("player_id", desc=False)
                    .in_("game_id", list(self.ids.values()))
                    .limit(BATCH_SIZE)
            )
            if lastId is not None:
                query = query.gt("player_id", lastId)
            
            response = query.execute()
            batch = response.data
            
            if not batch:
                break
            
            data.extend(batch)
        
            lastId = batch[-1]["player_id"]
        print(f"Fetched batch... Total collected items: {len(data)}")
        data.sort(key=lambda item: item["game_id"])
        
                
        for entry in data:
            player_entry = {
                "player": Player(rating=entry["rating"], rd=entry["rd"]),
                "appearances": entry["appearances"]
            }
            
            if entry["game_id"] not in self.players:
                self.players[entry["game_id"]] = {}
                
            self.players[entry["game_id"]][entry["player_id"]] = player_entry
        print("Done") 
        return self.players
        
    def buildCompositePlayer(self, players, videogame) -> Player:
        if not players:
           return
        temp_players = []
        for player in players:
            if player in self.players[videogame]:
                player_obj = self.players[videogame][player]["player"]
            else:
                player_obj = Player()
                self.players[videogame][player] = {
                    "player": player_obj,
                    "appearances": 0
                }
                self.new_players.add(player)
            temp_players.append(player_obj)
        avg_rating = sum(p.rating for p in temp_players) / len(temp_players)
        rms_rd = math.sqrt(sum(p.rd**2 for p in temp_players) / len(temp_players))
        
        return Player(rating=avg_rating, rd=rms_rd)
    
    def convertToGlicko2(self):
        if not self.sets:
            self.getSets()
        # TODO: IMPLEMENT THE GLICKO2 CONVERSION FUNCTIONALITY
        #       Iterate through the sets, record the matches using glicko2.Player()
        #       If record exist in supabase, update from there
        self.populateDataFromSupabase()
        print("Converting to Glicko2")
        for videogame, sets in self.sets.items():
            history = defaultdict(lambda: {"ratings": [], "rds": [], "outcomes": []}) 
            for set in sets:
                if not set:
                    continue
                winners = set["winner"]
                losers = set["loser"]
                los_team = self.buildCompositePlayer(losers, videogame)
                win_team = self.buildCompositePlayer(winners, videogame)
                
                for winner in winners:
                    history[winner]["ratings"].append(los_team.rating)
                    history[winner]["rds"].append(los_team.rd)
                    history[winner]["outcomes"].append(1)
                    
                for loser in losers:
                    history[loser]["ratings"].append(win_team.rating)
                    history[loser]["rds"].append(win_team.rd)
                    history[loser]["outcomes"].append(0)
            
            for player_id, hist in history.items():
                player_obj = self.players[videogame][player_id]["player"]
                player_obj.update_player(
                    hist["ratings"],
                    hist["rds"],
                    hist["outcomes"]
                )
        return True

    
    def parseAndUpload(self):
        self.convertToGlicko2()
        if not self.saved_games:
            print("Updating games")
            Videogames(SupabaseClient(), self.startgg, self.slug).updateGames()
        print("Updating new players onto database")
        Players(SupabaseClient(), self.startgg).updatePlayers(list(self.new_players))
        print("Building batch for supabase upload")
        batch = []
        for videogame_id, players in self.players.items():
            for player_id, player_obj in players.items():
                batch.append({
                    "player_id": player_id,
                    "game_id": videogame_id,
                    "rating": player_obj["player"].rating,
                    "rd": player_obj["player"].rd,
                    "appearances": player_obj["appearances"] + 1
                })
        print("Sending to supabase")
        if not batch:
            print("No rows to upload")
            return
        (self.supabase.table("ranking")
            .upsert(batch)
            .execute()
        )
        (self.supabase.table("history").upsert({
            "id": self.id,
            "name": self.name,
            "startAt": datetime.fromtimestamp(self.startAt, tz=timezone.utc).isoformat(),
            "slug": self.slug,
            "players": list(self.player_ids)
        }).execute())
        print("Done")
        return True

if __name__ == "__main__":
    tournament = Tournament(SupabaseClient(), StartGGClient(), "okizeme-53", True)
    tournament.parseAndUpload()
