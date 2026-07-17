from utils.players import Players
from utils.standings import Standings
from utils.connections.startgg import StartGGClient
from utils.connections.supabaseClient import SupabaseClient
from utils.tournaments import Tournament
from utils.search import Search
import argparse

if __name__ == "__main__":   
    parser = argparse.ArgumentParser(description="A script to process user data.")

    parser.add_argument("-slug", type=str, help="tournament slug")
    parser.add_argument("-standings", action="store_true")
    parser.add_argument("-search", action="store_true")
    parser.add_argument("-update", action="store_true")
    parser.add_argument("--saved-games", action="store_true")
    parser.add_argument("--afterDate", type=int, default=None)
    parser.add_argument("--beforeDate", type=int, default=None)
    parser.add_argument("--country", type=str, default=None)
    parser.add_argument("--state", type=str, default=None)

    args = parser.parse_args()

    supabase = SupabaseClient()
    startgg = StartGGClient()
    
    if (args.standings): 
        standings = Standings(supabase, startgg)  
        standings.pullStandingsUpdate(args.slug)      
        
    if (args.search):
        tournament = Tournament(supabase, startgg, args.slug, True if args.saved_games else False)
        tournament.parseAndUpload()
        
    if (args.update):
        search = Search(supabase, startgg, True if args.saved_games else False)
        search.search(args.afterDate, args.beforeDate, args.country, args.state)
        pass

    