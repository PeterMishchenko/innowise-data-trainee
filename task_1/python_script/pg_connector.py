from typing import Generator
import psycopg2
import json
import datetime as dt
import time
import os


class PostgreSQL_connector:
    def __init__(self):
        for i in range(5):
            try:
                self.__connection = psycopg2.connect(host=os.getenv('DB_HOST'), 
                                                    database=os.getenv('DB_NAME'), 
                                                    user=os.getenv('DB_USER'), 
                                                    password=os.getenv('DB_PASSWORD'),
                                                    port = os.getenv('DB_PORT'))
                return
            except psycopg2.OperationalError:
                print('trying to connect DB', i+1)
                time.sleep(1)
        raise Exception("db connection timeout")

    def isert_data(self, rooms:str = 'data/rooms.json', students:str = 'data/students.json'):
        cursor = self.__connection.cursor()

        #inserting rooms data
        rooms_values=',\n'.join(f"({i}, '{n}')" for i,n in read_rooms_lines(rooms))
        inser_rooms_comand = f"INSERT INTO\nroom(id, name)\nVALUES\n{rooms_values}"
        cursor.execute(inser_rooms_comand)

        # inserting students data
        students_values=',\n'.join(f"({i}, '{n}', {r}, {s}, {b})" for i,n,r,s,b in read_students_lines(students))
        inser_students_comand = f"INSERT INTO\nstudent(id, name, room_id, sex, birthday)\nVALUES\n{students_values}"
        cursor.execute(inser_students_comand)



        # finishing
        cursor.close()
        self.__connection.commit()

    def create_index(self):
        cursor = self.__connection.cursor()
        cursor.execute('CREATE INDEX ind_student_room ON student(room_id);')

        cursor.close()
        self.__connection.commit()
        
    def first(self):
        cursor = self.__connection.cursor()
        with open('sqlscripts/first.sql') as script:
            command = script.read()
            
            cursor.execute(command)
        res = cursor.fetchall()
        cursor.close()
        return res

    def second(self):
        cursor = self.__connection.cursor()
        with open('sqlscripts/second.sql') as script:
            command = script.read()
            cursor.execute(command)
        res = cursor.fetchall()
        cursor.close()
        return res

    def third(self):
        cursor = self.__connection.cursor()
        with open('sqlscripts/third.sql') as script:
            command = script.read()
            cursor.execute(command)
        res = cursor.fetchall()
        cursor.close()
        return res

    def fourth(self):
        cursor = self.__connection.cursor()
        with open('sqlscripts/fourth.sql') as script:
            command = script.read()
            cursor.execute(command)
        res = cursor.fetchall()
        cursor.close()
        return res
    
def read_rooms_lines(path: str) -> Generator[str,int,int]:
        with open(path) as rooms_json:
            j = json.load(rooms_json)
            for row in j:
                # If we dont trust json file, here should be added protection against sql injection
                yield [row["id"], row["name"]]

def read_students_lines(path: str) -> Generator[str,int,int]:
        with open(path) as students_json:
            j = json.load(students_json)
            for row in j:
                # If we dont trust json file, here should be added protection against sql injection
                yield [row["id"], 
                       row["name"], 
                       row['room'], 
                       1 if row['sex'] == 'M' else 0, 
                       dt.datetime.strptime(row['birthday'],'%Y-%m-%dT%H:%M:%S.%f').timestamp()]
