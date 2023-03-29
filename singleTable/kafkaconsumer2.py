from kafka import KafkaConsumer,TopicPartition
from json import loads  

def jsonLoad(data):
    return loads(data.decode('utf-8'))

my_consumer = KafkaConsumer(   
    bootstrap_servers = ['localhost:9092'],  
    auto_offset_reset = 'earliest',   
    group_id = 'group1' ,
    value_deserializer = jsonLoad  
)

topic_partition = TopicPartition('userschema_usertable',  1)
my_consumer.assign([topic_partition])

print("Reading From Group2 Consumer....Earliest messages(>M): ")
for message in my_consumer:  
    print(f'Message Consumed is {message.value}')

