from utils.connections.supabaseClient import SupabaseClient
from utils.connections.startgg import StartGGClient
from utils.tournaments import Tournament
import time

PERPAGE = 256

class Search:
    def __init__(self, supabaseClient: SupabaseClient, startgg: StartGGClient, saved_games=False):
        self.supabase = supabaseClient.getClient()
        self.startgg = startgg
        self.saved_games = saved_games
        self.tournaments = []
        pass
    
    def search(self, afterDate=None, beforeDate=None, country=None, state=None):
        variables = {}
        print("Retrieving Tournaments")
        if afterDate is not None:
            variables["afterDate"] = afterDate
        if beforeDate is not None:
            variables["beforeDate"] = beforeDate
        if country is not None:
            variables["country"] = country
        if state is not None:
            variables["state"] = state
        variables["page"] = 1
        variables["perPage"] = PERPAGE
        response = self.startgg.runQuery(name="getTournamentsPages", variables=variables)
        pages = response["data"]["tournaments"]["pageInfo"]["totalPages"]
        for page in range(1, pages+1):
            variables["page"] = page
            print(f"Retrieving page {page}")
            response = self.startgg.runQuery(name="getTournaments", variables=variables)
            tournaments_batch = response["data"]["tournaments"]["nodes"]
            self.tournaments.extend(tournaments_batch)
        self.tournaments.sort(key=lambda tournament: tournament["startAt"])
        for tournament in self.tournaments:
            tournamentClient = Tournament(SupabaseClient(), self.startgg, tournament["slug"], self.saved_games)
            tournamentClient.parseAndUpload()
        return 
    

            
            
            
            
            
            