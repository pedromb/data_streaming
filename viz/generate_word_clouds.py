from wordcloud import WordCloud
from pymongo import MongoClient
import matplotlib.pyplot as plt
from conf import db_conf

mongo_client = MongoClient(db_conf["MONGO_CONNECTION_STRING"])
db = mongo_client.get_database("reddit-stream")
topics = db['topics']

for doc in topics.find():
    topic = doc["topic"]
    filename="static/img/topic_{}_worldcloud.png".format(topic)
    total = sum(doc['keywords'].values())
    freques = dict(list(map(lambda x: (x[0], x[1]/total), doc['keywords'].items())))
    wordcloud=WordCloud(width=600, height=400, background_color="white")\
        .generate_from_frequencies(freques)
    plt.figure( figsize=(6,4) )
    plt.imshow(wordcloud)
    plt.tight_layout(pad=0)
    plt.axis('off')
    plt.savefig(filename, transparent=True)