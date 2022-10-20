import docker
import time
import click

import psycopg2
from DB_class import MyDB_PostgreSQL


@click.command()
@click.option('--students_path','-s', default='data/students.json', help='Path to students.json')
@click.option('--rooms_path','-r', default= 'data/rooms.json', help='Path to rooms.json')
@click.option('--output_format','-of', default= 'terminal', help='Output format: terminal - show on screen, xml, json')
def main(students_path , rooms_path, output_format):
    docker_client = docker.from_env()
    db_name = 'pg_task1'
    db_user = 'admin'
    db_password = 'P@ssw0rd'
    print('starting database...\r')
    db_container = docker_client.containers.run(image='postgres:15.0', 
                                                detach=True, 
                                                environment = {'POSTGRES_PASSWORD': db_password,
                                                            'POSTGRES_DB':db_name,
                                                            'POSTGRES_USER':db_user},
                                                ports = {'5432':'5432'},
                                                volumes = {'sqlscripts/initdb':{'bind':'/docker-entrypoint-initdb.d','mode':'ro'}}
                                                )
    time.sleep(1)

    print('databas is created\nconnecting to db...\r')
    for _ in range(5):
        try:
            db = MyDB_PostgreSQL('0.0.0.0', db_user, db_password, db_name,)
            break
        except psycopg2.OperationalError:
            pass
    if db!=db:
        print('db connection timeout')
        db_container.kill()
        exit()

    print('connection success\ncreatingT tables...\r')
    db.create_tabels(rooms=rooms_path, students=students_path)
    print('tables are created')
    print("")
    #while t != 'quit':
    click.echo('Now everything is ready!\nChose the command you whant:\n*first - for first script\n*second - for second script\n*third - for third script\n*fourth - for fourth script\n*exit-for exit')
    while True:
        c = click.prompt('>>')
        match c:
            case 'first':
                print(db.first())
            case 'second':
                pass
            case 'third':
                pass
            case 'fourth':
                pass
            case 'exit':
                break
    
    
    db_container.kill()
    print('thank you')



def foo(cf):
    print(cf)

if __name__ == '__main__':
    main()