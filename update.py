import os
import query as q
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

MAX_QUERY_SIZE = 250
database_url = os.getenv("DATABASE_API_URL")
database_api = os.getenv("DATABASE_API_KEY")

if not database_url or not database_api:
    raise ValueError("DATABASE_API_URL and DATABASE_API_KEY must be set in the environment variables.")

supabase: Client = create_client(database_url, database_api)


email = os.getenv("DATABASE_LOGIN_EMAIL")
password = os.getenv("DATABASE_LOGIN_PASSWORD")
if not email or not password:
    raise ValueError("DATABASE_LOGIN_EMAIL and DATABASE_LOGIN_PASSWORD must be set in the environment variables.")

response = supabase.auth.sign_in_with_password(
    {
        "email": email,
        "password": password,
    }
)

query = ""

def update_with_discriminator():
    response = supabase.table("player_table").select('player_id').execute()
    result = response.data
    # print(result)    
    merged = []
    for index in range(0, len(result), MAX_QUERY_SIZE):
        batch = result[index: index + MAX_QUERY_SIZE]
        print(f"Processing batch of {len(batch)} items")
        batch_result = process_batch(batch)
        for i, player in enumerate(batch):
            discriminator = batch_result['data'][f'E{i}']['user']['discriminator'] if batch_result['data'][f'E{i}']['user'] else None
            merged.append({
                'player_id': player['player_id'],
                'discriminator': discriminator
            })
    response = supabase.table("player_table").upsert(merged).execute()
        

def process_batch(batch):
    query = "query PlayerId("
    for i in range(len(batch)):
        query += f"$id{i}: ID!, "
    query += ") {\n"
    for i in range(len(batch)):
        query += f"""E{i}: player(id: $id{i})""" + """{
            user {
                discriminator
            }
        }\n"""
    query += "}"
    response = q.run_query(query, {
        **{f"id{i}": batch[i]['player_id'] for i in range(len(batch))}
    })
    return response