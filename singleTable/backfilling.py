import psycopg2
import time
from kafka import KafkaProducer
from json import dumps

print("************ Script Started ************")
# Establishing Connection

connection = psycopg2.connect(
     database="usersdata", user='shiva_ms', password='1234', host='localhost', port= '5432'
)

print("************ Connected ************")

def jsonDumper(data):
    return dumps(data).encode('utf-8')

def getData(cursor,tableName,endTime,startTime=0,filterColumn='lastupdated'):
    query = f'select * from {tableName} where {filterColumn} > {startTime} and {filterColumn} < {endTime}'
    cursor.execute(query)
    result = list(cursor.fetchall())
    column_names = [desc[0] for desc in cursor.description]
    if(len(result)!=0):
        return (column_names,result)



cursor = connection.cursor()

myProducer = KafkaProducer(
    bootstrap_servers = ['localhost:9092'],
    value_serializer = jsonDumper
)


column_names,Data = getData(cursor,'userschema.usertable',endTime=1679650149000,startTime=1679649860000,filterColumn='lastupdated')
if(Data != None):
    for row in Data:
        message = "Bacfilling data: "+str(row)
        print("Bacfilling data ",message)
        myProducer.send('userschema_usertable',value=message)
        myProducer.flush()
    print("No.of rows Got",len(Data))