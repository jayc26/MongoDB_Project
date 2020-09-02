import pymongo
import json
from pymongo import MongoClient
import pandas as pd
client = MongoClient("localhost",27017)
db=client['Project2']
p = "Projects"
# d = "DepartmentEC"
dl = "Dept_Loc"
e = "Employee"
w = "Works"
m1="Projdept"
m2="EMPLOYEEWORK"
# pw="ProjWork"
dbn=db[p]
# dbn2=db[d]
dbn3=db[dl]
dbn4=db[e]
dbn5=db[w]
dbn6=db[m1]
dbn8=db[m2]
# dbn7=db[pw]
proj = pd.read_csv("PROJECT.csv", names=["PNAME","PNO","PLOC","DNO"])
dept = pd.read_csv("DEPARTMENT.csv", names=["DNAME","DNO","M_SSN","M_START"],skipinitialspace=True, quotechar="'")
dloc = pd.read_csv("DEPT_LOCATIONS.csv", names=["DNO","DLOC"],skipinitialspace=True, quotechar="'")
emp = pd.read_csv("EMPLOYEE.csv", names=["FNAME","MINIT","LNAME","SSN","BDATE","ADDRESS","SEX","SALARY","M_SSN","DNO"],skipinitialspace=True, quotechar="'")
wk = pd.read_csv("WORKS_ON.csv", names=["SSN","PNO","HOURS"],skipinitialspace=True, quotechar="'")

m11 = proj.merge(dept, on='DNO')
m21 = emp.merge(wk, on='SSN')

dp= json.loads(proj.to_json(orient='records'))
# dd= json.loads(m.to_json(orient='records'))
ddl= json.loads(dloc.to_json(orient='records'))
de= json.loads(emp.to_json(orient='records'))
dw= json.loads(wk.to_json(orient='records'))
dm1=json.loads(m11.to_json(orient='records'))
dm2=json.loads(m21.to_json(orient='records'))
dbn.insert_many(dp)
dbn3.insert_many(ddl)
dbn4.insert_many(de)
dbn5.insert_many(dw)
dbn6.insert_many(dm1)
dbn8.insert_many(dm2)

#ef=emp.merge(wk,on="SSN")
# empl=proj.merge(wk,on="PNO")
# empl.to_csv("ProjWork.csv",index=False)
# # ef = pd.read_csv("IMP.csv")
# en= json.loads(empl.to_json(orient='records'))
# dbn7.insert_many(en)

f=db.Projdept.aggregate([
    {
        '$lookup' :
            {
                'from': 'EMPLOYEEWORK',
                'localField': 'PNO',
                'foreignField': 'PNO',
                'as': 'EMPLOYEES'
            }
    },
    {
        '$project':
            {
            '_id':1,
            'PNAME':1,
            'PNO':1,
            'DNAME':1,
            'EMPLOYEES.FNAME':1,
            'EMPLOYEES.LNAME':1,
            'EMPLOYEES.HOURS':1
            }
    },
    {
        '$out':
            'part1'
    }
])

p1o=db.part1.find({},{'_id':0})
pf=open("part1.json",'w')
for b in p1o:
    pf.write(str(b))