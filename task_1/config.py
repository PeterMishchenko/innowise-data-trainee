from dataclasses import dataclass
import json

@dataclass
class Config:
    pg_image: str
    pg_host: str
    pg_port: str
    pg_db_name: str
    pg_user: str
    pg_password: str

with open('config.json') as f:
    #print(json.load(f))
    CONFIG = Config(**json.load(f))