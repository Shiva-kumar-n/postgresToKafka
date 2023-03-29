import psycopg2
import time
from kafka import KafkaProducer
from json import dumps,loads
import pandas as pd
from concurrent.futures import ThreadPoolExecutor




class myProducer():
    
    def __init__(self,topicDetails,connection) -> None:

        self.kProducer = KafkaProducer(
            bootstrap_servers = ['localhost:9092'],
            value_serializer = self.jsonDumper
            )
        
        self.topicName = f'{topicDetails[1]}.{topicDetails[2]}'
        self.topicId = topicDetails[0]
        self.connection = connection
        self.cursor = self.connection.cursor()

    def push(self):
        self.cursor.execute(f'select * from {self.topicName} where lastupdated >{self.getOffest()} order by lastupdated limit 1;')
        fetchedRow= list(self.cursor.fetchall())[0]
        print(f't={self.topicName} fr={fetchedRow}')
        self.kProducer.send(self.topicName,value=fetchedRow)
        self.kProducer.flush()
        value = fetchedRow[-2]
        self.cursor.execute(f'update userschema.metadata set lastupdated = {value} where id={self.topicId};')
        self.connection.commit()

    def getOffest(self):
        self.cursor.execute(f'select * from userschema.metadata where id={self.topicId};')
        return list(self.cursor.fetchall())[0][-1]
    
    def jsonDumper(self,Data):
        return dumps(Data).encode('utf-8')

connection = psycopg2.connect(
     database="usersdata", user='shiva_ms', password='1234', host='localhost', port= '5432'
)

cur = connection.cursor()



pool = ThreadPoolExecutor(max_workers=5)



while True:
    cur.execute('select * from userschema.metadata;')
    metadata = list(cur.fetchall())
    l = []
    for i in metadata:
        #If the tables has isactive = 1 then adding to our pipeline
        if(i[-2]==1):
            l.append(myProducer(i,connection))
    for i in l:
        pool.submit(i.push)
    time.sleep(15)


connection.close()