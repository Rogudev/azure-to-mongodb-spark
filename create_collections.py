from pymongo import MongoClient

# connect to mongo atlas
uri = '<YOUR_MONGO_URI>'
conn = MongoClient(uri)

# select database
db = conn.gym_tracker

# load the collections list
exercises_list = []
with open('exercises_list.txt', 'r') as file:
    exercises_list = [line.strip() for line in file.readlines()]

# create collections
for exercise in exercises_list:
    db.create_collection(exercise)