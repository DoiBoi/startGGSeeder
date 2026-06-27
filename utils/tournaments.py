from typing import List
from connections.supabaseClient import SupabaseClient
from connections.startgg import StartGGClient

ENTRANTID_PERPAGE = 300

class Tournament:
    def __init__(self, 
                 supabaseClient: SupabaseClient, 
                 startGGClient: StartGGClient,
                 slug: str = None,
                 ids: List[int] = None):
        self.supabase = supabaseClient.getClient()
        self.startgg = startGGClient
        self.slug = slug if slug is not None else None
        self.ids = ids if ids is not None else None
        self.entrants = {}
        pass

    def getEvents(self, slug=None) -> List[object]:
        if not self.slug:
            self.slug = slug
        
        response = self.startgg.runQuery(name="getEvents", variables={
            "slug": self.slug
        })
        print(f"Retrieving {response["data"]["tournament"]["name"]}")

        self.ids = [event["id"] for event in response["data"]["tournament"]["events"]]

        return self.ids
    
    def getEntrants(self, ids=None) -> List[object]: 
        if not self.ids:
            self.ids = ids
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
                entrants = {node["id"]: [player["player"]["id"] for player in node["participants"]] for node in response["event"]["entrants"]["nodes"]}
                event_entrants.update(entrants)
            self.entrants.update({
                id: event_entrants
            })
        return self.entrants

if __name__ == "__main__":
    tournament = Tournament(SupabaseClient(), StartGGClient(), "okizeme-53")
    tournament.getEvents()
    print(tournament.getEntrants())
