import os
import requests
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

class StartGGClient():
  def __init__(self):
      self.url = os.getenv("SGG_API_URL")
      self.key = os.getenv("SGG_API_KEY")
      pass

  def load_query(self, name):
      return (Path(__file__).parent / "queries" / f"{name}.graphql").read_text()

  def runQuery(self, name, variables=None):
      query = self.load_query(name)
      
      headers = {
          "Content-Type": "application/json",
          "Authorization": "Bearer " + str(self.key),  # Replace with your own token
      }

      try: 
          response = requests.post(
              str(self.url),
              json={"query": query, "variables": variables},
              headers=headers
          )

          if response.status_code == 200:
              return response.json()
      except Exception as e: 
          print(f"An error occured in run_query:\n{e}") 


