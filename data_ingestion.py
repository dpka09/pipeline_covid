import requests
import json
import pydoop.hdfs as hdfs


url= "https://api.quarantine.country/api/v1/summary/latest"

response= requests.get(url)
result= response.json()

#select only regions which contains covid information of various regions/countries
result1= result["data"]["regions"]

a=[]
j=1

for i in result1.items():
    
    a.append(i[j])
#print(a)


with open("Covid.json","w") as f:
    json.dump(a, f)

#write data in hdfs:
from_path= "/home/deepika/Documents/Covid.json"
to_path="hdfs://localhost:9000/Covid"
hdfs.put(from_path, to_path)