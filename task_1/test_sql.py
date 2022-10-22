

from pg_connector import PostgreSQL_connector
from pg_container import PostgreSQL_container


container = PostgreSQL_container()
container.run()
db = PostgreSQL_connector()
db.isert_data(rooms='data/rooms.json', students='data/students.json')

db.create_index()
m_times = 100

results = [[],[],[],[]]
for i in range(m_times):
    results[0].append(float(db.first()[-1][0].replace('Execution Time: ','').replace(' ms','')))

for i in range(m_times):
    results[1].append(float(db.second()[-1][0].replace('Execution Time: ','').replace(' ms','')))

for i in range(m_times):
    results[2].append(float(db.third()[-1][0].replace('Execution Time: ','').replace(' ms','')))

for i in range(m_times):
    results[3].append(float(db.fourth()[-1][0].replace('Execution Time: ','').replace(' ms','')))

f = f"""results
first:  {sum(results[0])/len(results[0])}
second: {sum(results[1])/len(results[1])}
third:  {sum(results[2])/len(results[2])}
fourth: {sum(results[3])/len(results[3])}"""
print(f)

container.kill()
