from pickle import TRUE
from typing import Generator
import psycopg2
import json
import datetime as dt

class MyDB_PostgreSQL:
    def __init__(self, host:str, db_user:str, db_password:str, db_name:str ):
        self.connection = psycopg2.connect(host=host, database=db_name, user=db_user, password=db_password)


    def create_tabels(self, rooms:str = 'data/rooms.json', students:str = 'data/students.json'):
        cursor = self.connection.cursor()

        # creating tables
        with open('sqlscripts/initdb/init.sql') as init_script:
            command_create = init_script.read()
            cursor.execute(command_create)


        #inserting rooms data
        rooms_values=',\n'.join(f"({i}, '{n}')" for i,n in read_rooms_lines(rooms))
        inser_rooms_comand = f"""
INSERT INTO 
    room(id, name)
VALUES
    {rooms_values}        
"""
        cursor.execute(inser_rooms_comand)

        # inserting students data
        students_values=',\n'.join(f"({i}, '{n}', {r}, {s}, {b})" for i,n,r,s,b in read_students_lines(students))
        inser_students_comand = f"""
INSERT INTO 
    student(id, name, room_id, sex, birthday)
VALUES
    {students_values}        
"""
        cursor.execute(inser_students_comand)


        # finishing
        cursor.close()
        self.connection.commit()
        
    def first(self):
        cursor = self.connection.cursor()
        with open('sqlscripts/first.sql') as script:
            command = script.read()
            #print(command)
            cursor.execute(command)
        #print(cursor.fetchall())
        return cursor.fetchall()

    def second(self):
        cursor = self.connection.cursor()
        with open('sqlscripts/second.sql') as script:
            command = script.read()
            cursor.execute(command)
        return cursor.fetchall()

    def third(self):
        cursor = self.connection.cursor()
        with open('sqlscripts/third.sql') as script:
            command = script.read()
            cursor.execute(command)
        return cursor.fetchall()

    def fourth(self):
        cursor = self.connection.cursor()
        with open('sqlscripts/fourth.sql') as script:
            command = script.read()
            cursor.execute(command)
        return cursor.fetchall()
    
def read_rooms_lines(path: str) -> Generator[str,int,int]:
        with open(path) as rooms_json:
            j = json.load(rooms_json)
            for row in j:
                yield [row["id"], row["name"]]

def read_students_lines(path: str) -> Generator[str,int,int]:
        with open(path) as students_json:
            j = json.load(students_json)
            for row in j:
                yield [row["id"], 
                       row["name"], 
                       row['room'], 
                       1 if row['sex'] == 'M' else 0, 
                       dt.datetime.strptime(row['birthday'],'%Y-%m-%dT%H:%M:%S.%f').timestamp()]



