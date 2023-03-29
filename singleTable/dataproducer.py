import psycopg2
import time
import names
import random



def randomDataGenerator():
    cTimeStamp = int(time.time()) * 1000
    return (names.get_first_name(), int(random.random() * 100), cTimeStamp)



connection = psycopg2.connect(
    database="usersdata", user='shiva_ms', password='1234', host='localhost', port= '5432'
)

cursor = connection.cursor()


while True:
    newUser = randomDataGenerator()
    query = f'insert into userschema.usertable values{newUser}'
    cursor.execute(query)
    connection.commit()
    print(f"New User Data: {newUser}")
    time.sleep(3)