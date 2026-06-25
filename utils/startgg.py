import os
import requests

getStandings = """
query findStandings($slug: String) {
  tournament(slug: $slug) {
    events {
      name
      standings(query: {page: 1, perPage: 50}) {
        nodes {
          placement
          player {
            gamerTag
            id
          }
        }
      }
    }
  }
}
"""

def runQuery(query, variables=None):
    url = os.getenv("SGG_API_URL")
    key = os.getenv("SGG_API_KEY")

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + str(key),  # Replace with your own token
    }

    try: 
        response = requests.post(
            str(url),
            json={"query": query, "variables": variables},
            headers=headers
        )

        if response.status_code == 200:
            return response.json()
    except Exception as e: 
        print(f"An error occured in run_query:\n{e}") 

