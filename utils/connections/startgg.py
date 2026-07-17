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
      self.key = os.getenv("SGG_API_KEY_1")
      self.toggle = True
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

            attempt = 1
            while response_data is None or response_data.get("data") is None or response_data.get("error") or response.status_code != 200:
                if attempt % 3 == 0:
                    if self.toggle: 
                        self.key = os.getenv("SGG_API_URL_2")
                    else:
                        self.key = os.getenv("SGG_API_URL_1")
                    print("toggled")
                    self.toggle = not self.toggle

                print(f"Something went wrong, response:\n{response_data}, status: {response.status_code}")
                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After", 60)

                    print(f"Rate limited. Waiting for {retry_after} seconds...")
                    time.sleep(int(retry_after))
                else:
                    time.sleep(60)
                response = requests.post(
                    str(self.url),
                    json={"query": query, "variables": variables},
                    headers=headers
                )
                attempt += 1
                response_data = response.json()
            return response_data
                
        except Exception as e: 
            print(f"An error occured in run_query:\n{e}") 
            time.sleep(60)
            response = requests.post(
                str(self.url),
                json={"query": query, "variables": variables},
                headers=headers
            )
            attempt += 1
            response_data = response.json()
            return response_data
        
            

