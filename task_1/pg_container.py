import os
import docker

from config import CONFIG

class PostgreSQL_container:
    def __init__(self) -> None:
        self._docker_client = docker.from_env()
        self._db_container = None
        
    def run(self):

        self._db_container = self._docker_client.containers.run(
                                                                image= CONFIG.pg_image, 
                                                                detach=True, 
                                                                environment = {'POSTGRES_PASSWORD': CONFIG.pg_password,
                                                                                'POSTGRES_DB':CONFIG.pg_db_name,
                                                                                'POSTGRES_USER':CONFIG.pg_user},
                                                                ports = {'5432':CONFIG.pg_port},
                                                                volumes = {os.getcwd() + '/sqlscripts/initdb':{'bind':'/docker-entrypoint-initdb.d','mode':'ro'}}
                                                                )


    

    def kill(self):
        self._db_container.kill()
