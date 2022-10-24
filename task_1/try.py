import json
from jsonschema import validate , ValidationError
                 
try:
    with open("rooms.json") as r, open("python_script/data/rooms_schema.json") as s:
        validate(instance= json.load(r),schema = json.load(s))
except ValidationError as e:
    print("Wrong format of rooms.json")
    print(e)
with open("students.json") as r, open("python_script/data/students_schema.json") as s:
    validate(instance= json.load(r),schema = json.load(s))
    