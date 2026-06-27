from typing import List
from utils.connections.supabaseClient import SupabaseClient
from utils.connections.startgg import StartGGClient

BATCH_SIZE = 50

class Players():
    def __init__(self, supabaseClient: SupabaseClient, startGGClient: StartGGClient) -> None:
        self.supabase = supabaseClient.getClient()
        self.startGG = startGGClient
        pass

    def buildPlayerQuery(self, batch: List[str]):
        variables_list = []
        queries = []
        variables = {}

        for index, player_id in enumerate(batch):
            variable_name = f"id{index}"
            variables_list.append(f"${variable_name}: ID!")
            queries.append(
                f"""player{index}: player(id: ${variable_name}){{
                        id
                        gamerTag
                        user {{
                            discriminator
                        }}
                    }}
                """
            )
            variables[variable_name] = player_id

        query = f"""query findPlayer({', '.join(variables_list)}) {{
                    {''.join(queries)}
                }}
                """
        return query, variables
        

    def updatePlayers(self, players: List[str]):
        result = (
            self.supabase.table("player_table")
                .select("player_id")
                .execute()
            )
        
        existing_players = {player["player_id"] for player in result.data}
        new_players = [player for player in players if player not in existing_players]

        batches = self.startGG.splitIntoBatches(new_players, BATCH_SIZE)

        for batch in batches:
            query, variables = self.buildPlayerQuery(batch)
            response = self.startGG.runQuery(query=query, variables=variables)
            payload = []
            for player in response["data"].values(): # type: ignore
                payload.append({
                    "player_id": player["id"],
                    "name": player["gamerTag"],
                    "discriminator": player["user"]["discriminator"]
                })
            self.supabase.table("player_table").insert(payload).execute()
    print("Successfully updated player table")