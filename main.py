from utils.players import Players
from utils.standings import Standings
from utils.connections.startgg import StartGGClient
from utils.connections.supabaseClient import SupabaseClient
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A script to process user data.")

    parser.add_argument("-slug", type=str, help="tournament slug")

    args = parser.parse_args()

    supabase = SupabaseClient()
    startgg = StartGGClient()
    standings = Standings(supabase, startgg)
