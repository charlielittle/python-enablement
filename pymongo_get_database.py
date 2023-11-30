import os
from pprint import pprint

import bson
from dotenv import load_dotenv

from pymongo import MongoClient

def get_database( db_name="user_shopping_list" ):
 
   # Load config from a .env file:
   load_dotenv(verbose=True)
   MONGODB_URI = os.environ["MONGODB_URI"]

   # Connect to your MongoDB cluster:
   client = MongoClient(MONGODB_URI)
 
   # Create the database for our example (we will use the same database throughout the tutorial
   return client[db_name]
  
# This is added so that many files can reuse the function get_database()
if __name__ == "__main__":   
  
   # Get the database
   dbname = get_database('sample_mflix')
   for x in dbname.list_collection_names():
      print(x)

