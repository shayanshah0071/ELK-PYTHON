import json
import re
import os
from elasticsearch import Elasticsearch, helpers
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime

# no authentication
#client = Elasticsearch(["localhost:9200"], http_auth=('elastic', '<your-es-pwd>'))
def create_index(client):
    """Creates an index in Elasticsearch if one isn't already there."""

    client.indices.create(
        index="my-index",
        body={
            "settings": {
                "number_of_shards": 1, 
                "data_stream": { },
            },
            "mappings": {
                "properties": {
                    "@timestamp": {"type": "date", "format": "yyyy Mth dd HH:mm:ss"},
                    "name": {"type": "text"},
                    "msg": {"type": "text"},
                }
            },
        },
        ignore=400,
    )

def load_json():
    with open("data.json", 'r') as open_file:
        json_data = json.load(open_file)
        for k in json_data:
            print(k["timestamp"])
            print(type(k["timestamp"]))
            try:
                timestamp = datetime.strptime(k["timestamp"], "%Y %b %d %X")
                yield {
                "_index": "my-index",
                "_id": k["_id"],
                "@timestamp": timestamp,
                "name": k["name"],
                "msg": k["msg"]
                }
            except ValueError:
                print("Incorrect date format")
            

client = Elasticsearch(
     "https://localhost:9200",
    ca_certs="/etc/elasticsearch/certs/http_ca.crt",
    basic_auth=("elastic", "ojJbBAtrH13+0rLY1Ea6"))


i = 0
result = []

with open('syslog') as f:
    lines = f.readlines()

#regex = '(?P<ipaddress>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<dateandtime>.*)\] \"(?P<httpstatus>(GET|POST) .+ HTTP\/1\.1)\" (?P<returnstatus>\d{3} \d+) (\".*\")(?P<browserinfo>.*)\"'
#for line in lines:
 #   r = re.match(regex, line) 
    #r = re.compile("\S+ = (?:"[^"]+?") | (?:'[^']+?') | \S+ = \S+ ", re.X) 
    #r = re.compile("\S+ = (?:"[^"]+?") |(?:'[^']+?')|\S+=\S+", re.X)
    #r = r.match(line)                                                                                                                                   
#    r = line.strip().split(None, 1)
#    print(r)
for line in lines:
    try:
        line = line.replace('\x00', '')
        if(line !=None and len(line)> 16):
            timestamp, detail = line.strip()[:15], line.strip()[16:]
            name, msg = detail.split(':', 1)
            if (']' in msg):
                msg = msg.split(']', 1)[1]

            now = datetime.now().strftime("%Y")
            print(type(timestamp))
            timestamp = now+" "+timestamp
            #timestamp = datetime.strptime(timestamp, "%Y %b %d %X")
            print(timestamp)
            result.append ({
            "_id": i,
                "timestamp": timestamp,
                "name": name,
                "msg": msg,
            })
            i += 1
    except (ValueError, TypeError):
        print("Values Error")


with open('data.json', 'w') as fp:
    json.dump(result, fp)
#
#create_index(client)
helpers.bulk(client, load_json())

# check data is in there, and structure in there
print(client.search(
    query= {"match_all": {}}, index = 'my-index'))
print(client.indices.get_mapping(index = 'my-index'))
