import pymongo
import json
from pymongo import MongoClient
import pandas as pd
client = MongoClient("localhost",27017)
db=client['Project2']
m1="DeptEMP"
dbn=db[m1]
proj = pd.read_csv("PROJECT.csv", names=["PNAME","PNO","PLOC","DNO"])
dept = pd.read_csv("DEPARTMENT.csv", names=["DNAME","DNO","M_SSN","M_START"],skipinitialspace=True, quotechar="'")
dloc = pd.read_csv("DEPT_LOCATIONS.csv", names=["DNO","DLOC"],skipinitialspace=True, quotechar="'")
emp = pd.read_csv("EMPLOYEE.csv", names=["FNAME","MINIT","LNAME","SSN","BDATE","ADDRESS","SEX","SALARY","M_SSN","DNO"],skipinitialspace=True, quotechar="'")
wk = pd.read_csv("WORKS_ON.csv", names=["SSN","PNO","HOURS"],skipinitialspace=True, quotechar="'")

m11 = dept.merge(emp, on=['DNO','M_SSN'])

dm1=json.loads(m11.to_json(orient='records'))
dbn.insert_many(dm1)

f=db.DeptEMP.aggregate([
    {
        '$lookup' :
            {

                'from': 'Employee',
                'localField': 'DNO',
                'foreignField': 'DNO',
                'as': 'DEPARTMENTS'

            }
    },
    {
        '$project':
            {
            '_id':1,
            'DNAME':1,
            'DNO':1,
            'LNAME':1,
            'FNAME':1,
            'DEPARTMENTS.LNAME':1,
            'DEPARTMENTS.FNAME':1,
            'DEPARTMENTS.SALARY':1,
            }
    },
    {
        '$out':
            'EC2'
    }
])