import os
from pprint import pprint

import bson
from dotenv import load_dotenv
import pymongo
from pymongo_get_database import get_database

# Get a reference to the "sample_mflix" database:
db = get_database("sample_mflix")

# Get a reference to the "movies" collection:
movie_collection = db["movies"]

# Match title = "A Star Is Born":
stage_match_title = {
   "$match": {
         "title": "A Star Is Born"
   }
}

# Sort by year, ascending:
stage_sort_year_ascending = {
   "$sort": { "year": pymongo.DESCENDING }
}

# Limit to 1 document:
stage_limit_1 = { "$limit": 1 }

# Now the pipeline is easier to read:
pipeline = [
   stage_match_title, 
   stage_sort_year_ascending,
    stage_limit_1
]

results = movie_collection.aggregate(pipeline)
for movie in results:
   print(" * {title}, {first_castmember}, {year}".format(
         title=movie["title"],
         first_castmember=movie["cast"][0],
         year=movie["year"],
   ))

# Look up related documents in the 'comments' collection:
stage_lookup_comments = {
   "$lookup": {
         "from": "comments", 
         "localField": "_id", 
         "foreignField": "movie_id", 
         "as": "related_comments",
   }
}

# Limit to the first 5 documents:
stage_limit_5 = { "$limit": 5 }

pipeline = [
   stage_lookup_comments,
   stage_limit_5,
]

results = movie_collection.aggregate(pipeline)
for movie in results:
   pprint(movie)
   