import pymongo
import json
from pymongo import MongoClient
import pandas as pd
from pprint import pprint
client = MongoClient("localhost",27017)
db=client['Project2']
f= open("Queries.txt",'w')
ps="db.part2.find({'Projects':{'$elemMatch':{'HOURS':10}}},{'FNAME':1,'LNAME':1,'DNAME':1,'Projects.$':1})"
p=db.part2.find({'Projects':{'$elemMatch':{'HOURS':10}}},{'FNAME':1,'LNAME':1,'DNAME':1,'Projects.$':1})

f.write("Part 1:\n1. "+ps+"\n\n answer:")
for i in p:
    f.write(str(i))
ps1="db.part2.find({'Projects':{'$elemMatch':{'HOURS':{'$gt':30}}}},{'FNAME':1,'LNAME':1,'DNAME':1,'Projects.$':1})"
p1=db.part2.find({'Projects':{'$elemMatch':{'HOURS':{'$gt':30}}}},{'FNAME':1,'LNAME':1,'DNAME':1,'Projects.$':1})
f.write("\n2. "+ps1+"\n\n answer:")
for j in p1:
    f.write(str(j))
ps2="db.part2.find({'Projects':{'$elemMatch':{'PNO':{'$lt':90}}}},{'FNAME':1,'LNAME':1,'DNAME':1,'Projects.$':1})"
p2=db.part2.find({'Projects':{'$elemMatch':{'PNO':{'$lt':90}}}},{'FNAME':1,'LNAME':1,'DNAME':1,'Projects.$':1})
f.write("\n3. "+ps2+"\n\n answer:")
for k in p2:
    f.write(str(k))

ps3="db.part2.find({'DNAME':'HR'},{'FNAME':1,'LNAME':1,'DNAME':1,'Projects':1})"

p3=db.part2.find({'DNAME':'HR'},{'FNAME':1,'LNAME':1,'DNAME':1,'Projects':1})
f.write("\n4. "+ps3+"\n\n answer:")
for l in p3:
    f.write(str(l))

qs="db.part1.find({'EMPLOYEES':{'$elemMatch':{'HOURS':10}}},{'PNAME':1,'PNO':1,'DNAME':1,'EMPLOYEES.$':1})"
q=db.part1.find({'EMPLOYEES':{'$elemMatch':{'HOURS':10}}},{'PNAME':1,'PNO':1,'DNAME':1,'EMPLOYEES.$':1})
f.write("\n\n\nPart 2\n1. "+qs+"\n\n answer:")
for m in q:
    f.write(str(m))

qs1="q1=db.part1.find({'EMPLOYEES':{'$elemMatch':{'HOURS':{'$lt':35}}}},{'PNAME':1,'PNO':1,'DNAME':1,'EMPLOYEES.$':1})"
q1=db.part1.find({'EMPLOYEES':{'$elemMatch':{'HOURS':{'$lt':35}}}},{'PNAME':1,'PNO':1,'DNAME':1,'EMPLOYEES.$':1})
f.write("\n2. "+qs1+"\n\n answer:")
for n in q1:
    f.write(str(n))

qs2="db.part1.find({'EMPLOYEES':{'$elemMatch':{'FNAME':'John'}}},{'PNAME':1,'PNO':1,'DNAME':1,'EMPLOYEES.$':1})"
q2=db.part1.find({'EMPLOYEES':{'$elemMatch':{'FNAME':'John'}}},{'PNAME':1,'PNO':1,'DNAME':1,'EMPLOYEES.$':1})
f.write("\n3. "+qs2+"\n\n answer:")
for o in q2:
    f.write(str(o))


qs3="db.part1.find({'DNAME':'Research'},{'PNAME':1,'PNO':1,'DNAME':1,'EMPLOYEES':1})"
q3=db.part1.find({'DNAME':'Research'},{'PNAME':1,'PNO':1,'DNAME':1,'EMPLOYEES':1})
f.write("\n4. "+qs2+"\n\n answer:")
for p in q3:
    f.write(str(p))

f.close()

# p = db.part1.find({},
#     {
#         'EMPLOYEES.HOURS':[
        
#         {'$gt':30}
#         ]
        
#     }
# )

for i in q3:
    print(i)