import os
import requests
from dotenv import load_dotenv
from pathlib import Path
from typing import List
import time

load_dotenv()

class StartGGClient():
  def __init__(self):
      self.url = os.getenv("SGG_API_URL")
      self.key = os.getenv("SGG_API_KEY")
      pass

  def loadQuery(self, name):
      return (Path(__file__).parent / "queries" / f"{name}.graphql").read_text()
  
  def splitIntoBatches(self, items: List, size: int) -> List[List]:
        ret = []
        for start in range(0, len(items), size):
            ret.append(items[start:start+size])
        return ret

  def runQuery(self, name=None, variables=None, query=None):
        if (name is not None): 
          query = self.loadQuery(name)
      
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
            
            response_data = response.json()

            while response_data is None or response_data.get("data") is None or response_data.get("error"):
                print(f"Something went wrong, trying again in 30 seconds\nResponse:\n{response}")
                time.sleep(30)
                response = requests.post(
                    str(self.url),
                    json={"query": query, "variables": variables},
                    headers=headers
                )
            response_data = response.json()
            return response_data
                
        except Exception as e: 
            print(f"An error occured in run_query:\n{e}") 

