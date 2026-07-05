from utils.connections.supabaseClient import SupabaseClient
from utils.connections.startgg import StartGGClient

class Videogames:
    def __init__(self, supabase: SupabaseClient, startgg: StartGGClient, slug):
        self.supabase = supabase.getClient()
        self.startgg = startgg
        self.slug = slug
        pass
    
    def updateGames(self):
        print("Updating Games Database")
        response = self.startgg.runQuery(name="getGames", variables={
            "slug": self.slug
        })
        games = response["data"]["tournament"]["events"]
        payload_by_id = {}

        for game in games:
            videogame = game["videogame"]
            payload_by_id[videogame["id"]] = {
                "id": videogame["id"],
                "name": videogame["name"],
            }

        payload = list(payload_by_id.values())
        self.supabase.table("videogame_mapping").upsert(payload).execute()
        print("Done")