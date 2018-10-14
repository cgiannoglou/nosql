
# coding: utf-8

# In[1]:


from pymongo import MongoClient
from pyspark import SparkConf, SparkContext
import json
import numpy as np

def connect_to_mongodb(MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_USER, MONGO_PASSWORD, colllection_name):
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]
    #db.authenticate(MONGO_USER, MONGO_PASSWORD)
    print("Connection Succeed")
    return db[colllection_name]

#mycol = connect_to_mongodb('localhost', 27017, 'trajectories', '', '','objects')
#mydict = { "id": "2116", "lon": -3.4032816, "lat": 47.34281, "t": 1443688571 }
#mycol.insert_one(mydict)


# In[2]:


def k_neghbors(x,y,k,t_min,t_max):
    mycol = connect_to_mongodb('localhost', 27017, 'trajectories', '', '','objects')
    #query = '{"$and":[{"t":{"$gt":'+str(t_min)+'},{"t":{"$lt":'+str(t_max)+'}}]}'
    query = '{"$and":[{"t":{"$gt":'+str(t_min)+'}},{"t":{"$lt":'+str(t_max)+'}}]}'
    #query = '{"t":{"$gt":'+str(t_min)+'}}'
    k1 = json.loads(query)
    cursor = mycol.find()
    documents = []
    counter = 0
    for document in cursor:
        documents.insert(counter, document)
        counter += 1
    #print(documents)
    b1 = np.array((x,y))
    distances = []
    document = []
    counter = 0
    for doc in documents:
        b2 = np.array((doc['lon'],doc['lat']))
        dist = np.linalg.norm(b1-b2)
        distances.append((counter,dist))
        counter += 1
    distances = sorted(distances, key=lambda distance: distance[1])
    if len(distances) > k:
        for j in range(k):
            document.append(documents[distances[j][0]])
    else:
        for j in range(len(distances)):
            document.append(documents[distances[j][0]])
    return document
    
document = k_neghbors(-3.4032917,46.91752,10,1443687610,1643689790)
print(document)


# In[3]:


def k_unique_neghbors(x,y,k,t_min,t_max):
    mycol = connect_to_mongodb('localhost', 27017, 'trajectories', '', '','objects')
    query = '{"$and":[{"t":{"$gt":'+str(t_min)+'}},{"t":{"$lt":'+str(t_max)+'}}]}'
    k1 = json.loads(query)
    cursor = mycol.find(k1)
    documents = []
    counter = 0
    for document in cursor:
        documents.insert(counter, document)
        counter += 1
    #print(documents)
    b1 = np.array((x,y))
    distances = []
    ids = []
    document = []
    counter = 0
    for doc in documents:
        b2 = np.array((doc['lon'],doc['lat']))
        dist = np.linalg.norm(b1-b2)
        distances.append((counter,dist,doc['id']))
        counter += 1
    distances = sorted(distances, key=lambda distance: distance[1])
    for j in range(len(distances)):
        if len(document) < k:
            if document != []:
                for doc in document:
                    traj_object = documents[distances[j][0]]
                    if traj_object['id'] != doc['id']:
                        document.append(documents[distances[j][0]])
            else:
                document.append(documents[distances[j][0]])
        else:
            break
    return document
    
document = k_unique_neghbors(-3.4032917,46.91752,2,1443687610,1643689790)
print(document)


# In[11]:


def range_query(x_min, x_max, y_min, y_max, t_min, t_max):
    mycol = connect_to_mongodb('localhost', 27017, 'trajectories', '', '','objects')
    #query = '{"$and":[{"t":{"$gt":'+str(t_min)+'},{"t":{"$lt":'+str(t_max)+'}}]}'
    query = '{"$and":[{"t":{"$gt":'+str(t_min)+'}},{"t":{"$lt":'+str(t_max)+'}},{"lon":{"$gt":'+str(x_min)+'}},    {"lon":{"$lt":'+str(x_max)+'}},{"lat":{"$gt":'+str(y_min)+'}},{"lat":{"$lt":'+str(y_max)+'}}]}'
    #query = '{"t":{"$gt":'+str(t_min)+'}}'
    k1 = json.loads(query)
    cursor = mycol.find(k1)
    #cursor = mycol.find()
    documents = []
    counter = 0
    for document in cursor:
        documents.insert(counter, document)
        counter += 1
    return documents
        
objects = range_query(-4.4032916, 10.4032917, 40.91752, 50.91752, 1343687610,1743689790)
print(objects)


# In[12]:


def range_query(x_min, x_max, y_min, y_max, t_min, t_max, agg):
    mycol = connect_to_mongodb('localhost', 27017, 'trajectories', '', '','objects')
    #query = '{"$and":[{"t":{"$gt":'+str(t_min)+'},{"t":{"$lt":'+str(t_max)+'}}]}'
    query = '{"$and":[{"t":{"$gt":'+str(t_min)+'}},{"t":{"$lt":'+str(t_max)+'}},{"lon":{"$gt":'+str(x_min)+'}},    {"lon":{"$lt":'+str(x_max)+'}},{"lat":{"$gt":'+str(y_min)+'}},{"lat":{"$lt":'+str(y_max)+'}}]}'
    #query = '{"t":{"$gt":'+str(t_min)+'}}'
    k1 = json.loads(query)
    cursor = mycol.find(k1)
    #cursor = mycol.find()
    documents = []
    counter = 0
    for document in cursor:
        documents.insert(counter, document)
        counter += 1
    if agg == "count":
        return len(documents)
    
objects_len = range_query(-4.4032916, 10.4032917, 40.91752, 50.91752, 1343687610,1743689790,"count")
print(objects_len)

