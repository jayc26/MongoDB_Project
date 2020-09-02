import pymongo
import json
from pymongo import MongoClient
import pandas as pd
from json2xml import json2xml
client = MongoClient("localhost",27017)
db=client['Project2']
f1=open("part1_extra_credit1.xml",'w')
f2=open("part2_extra_credit1.xml",'w')
f=db.part1.find({},{'_id':0})
m=db.part2.find({},{'_id':0})
# for i in f:
#     pass

p=json2xml.Json2xml(f).to_xml()
f1.write(p)
q=json2xml.Json2xml(m).to_xml()
f2.write(q)

