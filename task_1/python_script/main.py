from pg_connector import PostgreSQL_connector
#from pg_container import PostgreSQL_container
import os



def main():
    output_format = os.getenv('output_format')

    try: 
        db = PostgreSQL_connector()
    except Exception as e:
        print(e.args)
        return
    
    # inserting data to db
    db.isert_data(rooms='data/rooms.json', students='data/students.json')
    print(os.getenv('OUTPUT_FORMAT'))
    second(db,os.getenv('OUTPUT_FORMAT'))
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
        with open("results/second_results.json",'w') as f:
            f.write(out)
    elif output_format == 'xml':
        out = '<results>\n' \
            + '\n'.join(f'<row><room_number>{s[0]}</room_number></row>' for s in result) \
            + '\n</results>'
        with open("results/second_results.xml",'w') as f:
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
