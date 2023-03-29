# postgresToKafka


Working:
singleTable:
    --> We are using dataproducer.py to generate random data of users(name,age,timestamp) and storing to a table in postgres
    --> kafkaproducer.py will help us to fetch data from the postgres table and pushing to a kafkaTopic
        -we are fetching 100 records at a time by storing the lastupdated column as offset in metatable(userschema.utkafkaoffset)
        -so next time this kafkaproducer will fetch 100 records with lastupdated greater than this offset value in the metatable and push into the kafkatopic and so on.
        -And we are pushing the data into two partions where users having A-M letter as first letter in their name will be pushed to partition 0 and others will be to partition 1
    --> kafkaconsumer and kafkaconsumer2 are used to consume the partition0 and partition1 data.

    -->backfilling.py is used to backfill data for usertable by providing starttime and endtime


MultipleTables:
    --> we are using metaTable(userschema.metadata) to store tables data.
    --> and we are fetching the tables with *isactive = 1* field
    --> then we are using myProducer class to create pipeline for the tables and pushing the data to their respective kafkatopics


*Files Details*
singleTable/dataproducer.py   : Used to produce user name,age,timestamp to userschema.usertable
singleTable/kafkaproducer.py  : Used to retrieve data from postgres and pushing to a kafka Topic with two partitions
singleTable/kafkaconsumer.py  : Used to consume from usertable topic's partition 0
singleTable/kafkaconsumer2.py : Used to consume from usertable topic's partition 1
singleTable/backfilling.py    : Used to backfill data from userschema.usertable table 

multipleTables/producer.py    : Used to create pipeline for multiple tables from postgress to kafkaTopic