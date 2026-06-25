import os
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

url: str = os.environ.get("DATABASE_API_URL")
key: str = os.environ.get("DATABASE_API_KEY")
supabase: Client = create_client(supabase_url=url, supabase_key=key)

class SupabaseClient():
    def __init__(self):  
        self.url: str = os.environ.get("DATABASE_API_URL")
        self.key: str = os.environ.get("DATABASE_API_KEY")
        self.supabase: Client = create_client(supabase_url=url, supabase_key=key)
        pass

    def getClient(self):
        return self.supabase