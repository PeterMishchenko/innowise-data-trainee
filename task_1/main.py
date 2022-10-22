
import click

from pg_connector import PostgreSQL_connector
from pg_container import PostgreSQL_container


@click.command()
@click.option('--students_path','-s', default='data/students.json', help='Path to students.json')
@click.option('--rooms_path','-r', default= 'data/rooms.json', help='Path to rooms.json')
@click.option('--output_format','-of', default= 'terminal', help='Output format: terminal - show on screen, xml, json')
def main(students_path , rooms_path, output_format):

    allowed_output_format = ['terminal', 'json','xml', 'time']
    if output_format not in allowed_output_format:
        print(f'Wrong --output_format {output_format}, sholud be in {allowed_output_format}')

    # create docker container for postgresql
    container = PostgreSQL_container()
    container.run()
    
    # connecting to DB
    try: 
        db = PostgreSQL_connector()
    except Exception as e:
        print(e.args)
        container.kill()
        return
    
    # inserting data to db
    db.isert_data(rooms=rooms_path, students=students_path)
    
    
    click.echo('Now everything is ready!')
    click.echo('\nChose the command you want:\n`first` - for first script\n`second` - for second script\n`third` - for third script\n`fourth` - for fourth script\n`all` - for all scripts\n`exit`-for exit')
    while True:
        c = click.prompt('>>')
        match c:
            case 'first':
                first(db, output_format)
                continue
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
            case _:
                click.echo(f'Wrong command - {c}')
                click.echo('Chose the command you want:\n`first` - for first script\n`second` - for second script\n`third` - for third script\n`fourth` - for fourth script\n`all` - for all scripts\n`exit`-for exit')
    
    container.kill()
    print('thank you')



def first(db:PostgreSQL_connector, output_format):
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

def second(db:PostgreSQL_connector, output_format):
    result = db.second()
    if output_format == 'terminal':
        print('Room number\n'+'\n'.join([str(s[0]) for s in result]))
    elif output_format == 'json':
        out = '{\n\t"results":[\n' \
        + ',\n'.join(f'\t\t{{\n\t\t\t"room_number": {s[0]}\n\t\t}}' for s in result) \
        + '\n\t]\n}'
        with open("second_results.json",'w') as f:
            f.write(out)
    elif output_format == 'xml':
        out = '<results>\n' \
            + '\n'.join(f'<row><room_number>{s[0]}</room_number></row>' for s in result) \
            + '\n</results>'
        with open("second_results.xml",'w') as f:
            f.write(out)

def third(db:PostgreSQL_connector, output_format):
    result = db.third()
    if output_format == 'terminal':
        print('Room number\n'+'\n'.join([str(s[0]) for s in result]))
    elif output_format == 'json':
        out = '{\n\t"results":[\n' \
        + ',\n'.join(f'\t\t{{\n\t\t\t"room_number": {s[0]}\n\t\t}}' for s in result) \
        + '\n\t]\n}'
        with open("third_results.json",'w') as f:
            f.write(out)
    elif output_format == 'xml':
        out = '<results>\n' \
            + '\n'.join(f'<row><room_number>{s[0]}</room_number></row>' for s in result) \
            + '\n</results>'
        with open("third_results.xml",'w') as f:
            f.write(out)

def fourth(db:PostgreSQL_connector, output_format):
    result = db.fourth()
    if output_format == 'terminal':
        print('Room number\n'+'\n'.join([str(s[0]) for s in result]))
    elif output_format == 'json':
        out = '{\n\t"results":[\n' \
        + ',\n'.join(f'\t\t{{\n\t\t\t"room_number": {s[0]}\n\t\t}}' for s in result) \
        + '\n\t]\n}'
        with open("fourth_results.json",'w') as f:
            f.write(out)
    elif output_format == 'xml':
        out = '<results>\n' \
            + '\n'.join(f'<row><room_number>{s[0]}</room_number></row>' for s in result) \
            + '\n</results>'
        with open("fourth_results.xml",'w') as f:
            f.write(out)




if __name__ == '__main__':
    main()
