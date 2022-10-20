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
    allowed_output_format = ['terminal', 'json','xml']
    if output_format not in allowed_output_format:
        print(f'Wrong --output_format {output_format}, sholud be in {allowed_output_format}')

    # create docker container for postgresql

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
    click.echo('Now everything is ready!\nChose the command you whant:\n`first` - for first script\n`second` - for second script\n`third` - for third script\n`fourth` - for fourth script\n`all` - for all scripts\n`exit`-for exit')
    while True:
        c = click.prompt('>>')
        match c:
            case 'first':
                first(db, output_format)
            case 'second':
                second(db, output_format)
            case 'third':
                third(db, output_format)
            case 'fourth':
                fourth(db,output_format)
            case 'all':
                first(db, output_format)
                second(db, output_format)
                third(db, output_format)
                fourth(db,output_format)
            case 'exit':
                break
    
    
    db_container.kill()
    print('thank you')



def first(db:MyDB_PostgreSQL, output_format):
    result = db.first()
    if output_format == 'terminal':
        print('Room number, student count\n'+'\n'.join([f'{s}, {j}' for s,j in result]))
    elif output_format == 'json':
        s = '{\n\t"results":[\n' \
        + ',\n'.join(f'\t\t{{\n\t\t\t"room_number": {s},\n\t\t\t"students_count": {j}\n\t\t}}' for s,j in result) \
        + '\n\t]\n}'
        with open("first_results.json",'w') as f:
            f.write(s)
    elif output_format == 'xml':
        s = '<results>\n' \
            + '\n'.join(f'<row><room_number>{s}</room_number><students_count>{j}</students_count></row>' for s,j in result) \
            + '\n</results>'
        with open("first_results.xml",'w') as f:
            f.write(s)

def second(db:MyDB_PostgreSQL, output_format):
    result = db.second()
    if output_format == 'terminal':
        print('Room number\n'+'\n'.join([str(s) for s in result]))
    elif output_format == 'json':
        s = '{\n\t"results":[\n' \
        + ',\n'.join(f'\t\t{{\n\t\t\t"room_number": {s}\n\t\t}}' for s in result) \
        + '\n\t]\n}'
        with open("second_results.json",'w') as f:
            f.write(s)
    elif output_format == 'xml':
        s = '<results>\n' \
            + '\n'.join(f'<row><room_number>{s}</room_number></row>' for s in result) \
            + '\n</results>'
        with open("second_results.xml",'w') as f:
            f.write(s)

def third(db:MyDB_PostgreSQL, output_format):
    result = db.third()
    if output_format == 'terminal':
        print('Room number\n'+'\n'.join([str(s) for s in result]))
    elif output_format == 'json':
        s = '{\n\t"results":[\n' \
        + ',\n'.join(f'\t\t{{\n\t\t\t"room_number": {s}\n\t\t}}' for s in result) \
        + '\n\t]\n}'
        with open("third_results.json",'w') as f:
            f.write(s)
    elif output_format == 'xml':
        s = '<results>\n' \
            + '\n'.join(f'<row><room_number>{s}</room_number></row>' for s in result) \
            + '\n</results>'
        with open("third_results.xml",'w') as f:
            f.write(s)

def fourth(db:MyDB_PostgreSQL, output_format):
    result = db.fourth()
    if output_format == 'terminal':
        print('Room number\n'+'\n'.join([str(s) for s in result]))
    elif output_format == 'json':
        s = '{\n\t"results":[\n' \
        + ',\n'.join(f'\t\t{{\n\t\t\t"room_number": {s}\n\t\t}}' for s in result) \
        + '\n\t]\n}'
        with open("fourth_results.json",'w') as f:
            f.write(s)
    elif output_format == 'xml':
        s = '<results>\n' \
            + '\n'.join(f'<row><room_number>{s}</room_number></row>' for s in result) \
            + '\n</results>'
        with open("fourth_results.xml",'w') as f:
            f.write(s)

if __name__ == '__main__':
    main()
