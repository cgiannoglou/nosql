
# coding: utf-8

# In[1]:


from pymongo import MongoClient
from pyspark import SparkConf, SparkContext

conf = SparkConf()
conf.setMaster("local")
conf.setAppName("My application")
conf.set("spark.executor.memory", "1g")
sc = SparkContext.getOrCreate()

def connect_to_mongodb(MONGO_HOST, MONGO_PORT, MONGO_DB, MONGO_USER, MONGO_PASSWORD, colllection_name):
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[MONGO_DB]
    #db.authenticate(MONGO_USER, MONGO_PASSWORD)
    print("Connection Succeed")
    return db[colllection_name]

mycol = connect_to_mongodb('localhost', 27017, 'test-database', '', '','posts')


# In[2]:


keys_list = []
types_list = []
def find_keywords(mycollection):
    cursor = mycollection.find({})
    for document in cursor:
        for key in document:
            counter = 0
            exists = 0
            for k in keys_list:
                counter += 1
                if k == key:
                    exists = 1
                    break
            if exists == 0:
                result = mycollection.distinct(key)
                types_list.insert(counter,type(result[0]))
                keys_list.insert(counter,key)
    if not keys_list:
        print("The collection doesn 't exist or is empty!")
    else:
        print("You can search by the following keywords")
        counter = 0
        for i in keys_list:
            print(str(counter)+" "+keys_list[counter]+" "+str(types_list[counter]))
            counter += 1
        

find_keywords(mycol)


# In[3]:


import json

def filter_and(col,a,b,c):
    query = ''
    for i in range(len(a)):
        a[i] = '"'+a[i]+'"'
        if type(b[i]) == str:
            b[i] = '"'+b[i]+'"'
        if i == 0:
            if c[i] == ':':
                query += a[i]+c[i]+str(b[i])
            elif c[i] == '>=':
                query += '"$or":[{'+a[i]+':'+str(b[i])+'},{'+a[i]+':{"$gt":'+str(b[i])+'}}]'
            elif c[i] == '<=':
                query += '"$or":[{'+a[i]+':'+str(b[i])+'},{'+a[i]+':{"$lt":'+str(b[i])+'}}]'
            elif c[i] == '>':
                query += a[i]+':{"$gt":'+str(b[i])+'}'
            elif c[i] == '<':
                query += a[i]+':{"$lt":'+str(b[i])+'}'
        else:
            if c[i] == ':':
                query += ','+a[i]+c[i]+str(b[i])
            elif c[i] == '>=':
                query += ',"$or":[{'+a[i]+':'+str(b[i])+'},{'+a[i]+':{"$gt":'+str(b[i])+'}}]'
            elif c[i] == '<=':
                query += ',"$or":[{'+a[i]+':'+str(b[i])+'},{'+a[i]+':{"$lt":'+str(b[i])+'}}]'
            elif c[i] == '>':
                query += ','+a[i]+':{"$gt":'+str(b[i])+'}'
            elif c[i] == '<':
                query += ','+a[i]+':{"$lt":'+str(b[i])+'}'
    query = "{"+query+"}"
    k1 = json.loads(query)
    cursor = col.find(k1)
    documents = []
    counter = 0
    for document in cursor:
        documents.insert(counter, document)
        counter += 1
    print(documents)
    
    
filter_and(mycol,["author","like"],["Mike",10],[':','>='])


# In[4]:


import json

def filter_or(col,a,b,c):
    query = ''
    for i in range(len(a)):
        a[i] = '"'+a[i]+'"'
        if type(b[i]) == str:
            b[i] = '"'+b[i]+'"'
        if i == 0:
            if c[i] == ':':
                query += '{'+a[i]+c[i]+str(b[i])+'}'
            elif c[i] == '>=':
                query += '{"$or":[{'+a[i]+':'+str(b[i])+'},{'+a[i]+':{"$gt":'+str(b[i])+'}}]}'
            elif c[i] == '<=':
                query += '{"$or":[{'+a[i]+':'+str(b[i])+'},{'+a[i]+':{"$lt":'+str(b[i])+'}}]}'
            elif c[i] == '>':
                query += '{'+a[i]+':{"$gt":'+str(b[i])+'}}'
            elif c[i] == '<':
                query += '{'+a[i]+':{"$lt":'+str(b[i])+'}}'
        else:
            if c[i] == ':':
                query += ',{'+a[i]+c[i]+str(b[i])+'}'
            elif c[i] == '>=':
                query += ',{"$or":[{'+a[i]+':'+str(b[i])+'},{'+a[i]+':{"$gt":'+str(b[i])+'}}]}'
            elif c[i] == '<=':
                query += ',{"$or":[{'+a[i]+':'+str(b[i])+'},{'+a[i]+':{"$lt":'+str(b[i])+'}}]}'
            elif c[i] == '>':
                query += ',{'+a[i]+':{"$gt":'+str(b[i])+'}}'
            elif c[i] == '<':
                query += ',{'+a[i]+':{"$lt":'+str(b[i])+'}}'
    query = '{"$or":['+query+"]}"
    k1 = json.loads(query)
    cursor = col.find(k1)
    documents = []
    counter = 0
    for document in cursor:
        documents.insert(counter, document)
        counter += 1
    print(documents)
    
    
filter_or(mycol,["like","author"],[11,"Mike"],['<',':'])

