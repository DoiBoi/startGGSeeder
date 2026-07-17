from typing import List
from utils.connections.supabaseClient import SupabaseClient
from utils.connections.startgg import StartGGClient
from utils.players import Players
import math
import collections

BASE_ENTRANT = 16
STANDINGS_PERPAGE = 256

class Standings():
    def __init__(self, supabaseClient: SupabaseClient, startGGClient: StartGGClient):
        self.supabase = supabaseClient.getClient()
        self.startGGClient = startGGClient
        self.standings = collections.defaultdict()
        self.players = Players(supabaseClient, self.startGGClient)
        pass

    def getStandings(self, videogameID=None):
        query = (
            self.supabase.table("local_points")
                .select("*")
        )

        if (videogameID is not None): query.eq("game_id", videogameID)

        response = query.execute()

        return response.data
    
    def updateStandings(self, videogameID, standings: List[object]):
        if not standings:
            return

        select_data = (
            self.supabase.table("local_points")
                .select("*")
                .eq("game_id", videogameID)
                .execute() 
            ).data
        players = []

        for points in standings:
            target = next((player for player in select_data if player["id"] == points["id"]), None)
            if (target is not None): points["points"] += target["points"]
            players.append(points["id"])

        self.players.updatePlayers(players)
        self.supabase.table("local_points").upsert(standings).execute()

    def processEventStanding(self, eventId):
        response = self.startGGClient.runQuery(name="getStandingPages", variables = {
            "id": eventId,
            "page": 1,
            "perPage": STANDINGS_PERPAGE
        })
        
        pages = response["data"]["event"]["standings"]["pageInfo"]["totalPages"]
        
        ret = []
        
        for page in range(1, pages+1):
            response = self.startGGClient.runQuery(name="getStandings", variables={
                "id": eventId,
                "page": page,
                "perPage": STANDINGS_PERPAGE
            })
            
            
            nodes = response["data"]["event"]["standings"]["nodes"]
            if not nodes: continue
            
            ret.extend(nodes)
            
        return ret
            

    def pullStandingsUpdate(self, slug):
        response = self.startGGClient.runQuery(name="getEvents", variables={
            "slug": slug
        })

        if response["data"]["tournament"] == None: return print("Tournament not found!")
        events = response["data"]["tournament"]["events"]
        for event in events:
            standing = self.processEventStanding(event["id"])
            game = event["videogame"]["id"]
            self.standings[game] = standing
        
        for videogameID, standings in self.standings.items():
            ret = []
            standings = [player for player in standings if player["entrant"]["isDisqualified"] is None]
            for standing in standings:
                if standing["player"] is None: continue
                points = 5
                multiplier = round(math.sqrt(len(standings)/BASE_ENTRANT), 2)
                match standing["placement"]:
                    case 1:
                        points = 100 * multiplier
                    case 2:
                        points = 80 * multiplier
                    case 3:
                        points = 65 * multiplier
                    case 4:
                        points = 55 * multiplier
                    case 5:
                        points = 45 * multiplier
                    case 7:
                        points = 35 * multiplier
                    case _:
                        points = 5
                ret.append({
                    "id": standing["player"]["id"],
                    "game_id": videogameID,
                    "points": round(points)
                })
            self.updateStandings(videogameID, ret)
