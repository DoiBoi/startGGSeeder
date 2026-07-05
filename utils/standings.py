from typing import List
from connections.supabaseClient import SupabaseClient
from connections.startgg import StartGGClient
from players import Players
import math

BASE_ENTRANT = 16

class Standings():
    def __init__(self, supabaseClient: SupabaseClient, startGGClient: StartGGClient):
        self.supabase = supabaseClient.getClient()
        self.startGGClient = startGGClient
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

    def pullStandingsUpdate(self, slug):
        response = self.startGGClient.runQuery(name="getStandings", variables={
            "slug": slug
        })

        if response["data"]["tournament"] == None: return print("Tournament not found!")
        response = response["data"]["tournament"]["events"]
        for event in response:
            ret = []
            videogameID = event["videogame"]["id"]
            for standing in event["standings"]["nodes"]:
                points = 5
                multiplier = round(math.sqrt(len(event["standings"]["nodes"])/BASE_ENTRANT))
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
                    "points": points
                })
            self.updateStandings(videogameID, ret)

if __name__ == "__main__":
    standing = Standings(SupabaseClient(), StartGGClient())
    standing.pullStandingsUpdate("ubc-fgc-thunderbird-summit")
