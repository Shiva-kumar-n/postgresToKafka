import psycopg2
import time
from kafka import KafkaProducer
from json import dumps,loads



connection = psycopg2.connect(
     database="usersdata", user='shiva_ms', password='1234', host='localhost', port= '5432'
)

def getLatestoffset(cursor,tableName,offestColumnName):
    query = f'select * from {tableName} order by {offestColumnName} desc limit 1'
    cursor.execute(query)
    result = list(cursor.fetchall())
    if(len(result) == 0):
        latest_offset = 0
    else:
        latest_offset = result[0][0]
    return latest_offset


def jsonDumper(data):
    return dumps(data).encode('utf-8')

def putLatestoffset(cursor,tableName,offsetValue):
    query = f'insert into {tableName} values({offsetValue})'
    cursor.execute(query)


def getData(connection,cursor,tableName,filterColumn,offestColumnName,offsetTableName):
    offsetValue = getLatestoffset(cursor,offsetTableName,offestColumnName)
    query = f'select * from {tableName} where {filterColumn} > {offsetValue} order by {filterColumn} limit 100'
    cursor.execute(query)
    result = list(cursor.fetchall())
    if(len(result)!=0):
        #Only User Schenario
        maxOffset = max([row[2] for row in result])
        putLatestoffset(cursor,offsetTableName,maxOffset)
        connection.commit()
        return result


def myPartitioner(key, all_partitions, available):
    if(loads(key.decode('utf-8')) <= 'M' ):
        return 0
    else:
        return 1

cursor = connection.cursor()

myProducer = KafkaProducer(
    bootstrap_servers = ['localhost:9092'],
    value_serializer = jsonDumper,
    key_serializer = jsonDumper,
    partitioner=myPartitioner
)


while True:
    Data = getData(connection,cursor,'userschema.usertable','lastupdated','latest_lastupdate','userschema.utkafkaoffset')
    if(Data != None):
        for row in Data:
            message = str(row)
            print(message[2])
            print("Message Pushed ",message)
            myProducer.send('userschema_usertable',value=message,key=message[2])
        print("No.of rows Got",len(Data))
        time.sleep(3)

