import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import sum,avg,max,count

spark = SparkSession \
    .builder \
    .config("spark.driver.extraClassPath", "/home/jovyan/.ivy2/jars/org.postgresql_postgresql-42.2.19.jar")\
    .appName("My App") \
    .getOrCreate()

def get_table(table_name):
    return spark.read \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://host.docker.internal:5432/postgres") \
                .option("dbtable", table_name) \
                .option("user", 'postgres') \
                .option("password", '123456') \
                .option("driver", "org.postgresql.Driver") \
                .load()

# Вывести количество фильмов в каждой категории, отсортировать по убыванию.

category = get_table('category')
film_category = get_table('film_category')

first = film_category.groupby('category_id')\
                     .count()\
                     .join(category, on ='category_id')\
                     .orderBy('count', ascending=False)['name','count']


# Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
inventory = get_table('inventory')
rental = get_table('rental')
film_actor = get_table('film_actor')
actor = get_table('actor')

second = rental.join(inventory, on='inventory_id')\
                .groupby('film_id')\
                .count()\
                .join(film_actor, on='film_id')\
                .groupby('actor_id')\
                .sum('count')\
                .join(actor, on='actor_id')\
                .withColumnRenamed('sum(count)', 'count')\
                .orderBy('count', ascending=False)['first_name','last_name','count']\
                .head(10)


# Вывести категорию фильмов, на которую потратили больше всего денег.
film = get_table('film')

third = film.join(film_category, on = 'film_id')\
            .groupby('category_id')\
            .sum('replacement_cost')\
            .join(category, on='category_id')\
            .withColumnRenamed('sum(replacement_cost)', 'cost')\
            .orderBy('cost',ascending=False)\
            .first()['name']

# Вывести названия фильмов, которых нет в inventory.
fourth = film.join(inventory, on ='film_id', how= 'left')['title','inventory_id']\
             .filter('inventory_id is Null').distinct()['title']

# Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. 
#Если у нескольких актеров одинаковое кол-во фильмов, вывести всех..
children_category_id = category.filter('name == "Children"').first()[0]
children_films = [row['film_id'] for row in film_category.filter(f'category_id == {children_category_id}').collect()]
df = film_actor.filter(film_actor.film_id.isin(children_films)).groupby('actor_id').count().orderBy('count',ascending=False)

top_3_score = [row['count'] for row in df.dropDuplicates(["count"]).select("count").orderBy('count',ascending=False).head(3)]

fifth= df.filter(df['count'].isin(top_3_score))\
         .join(actor, on ='actor_id')\
         .orderBy('count',ascending=False)['first_name','last_name']


# Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). 
#Отсортировать по количеству неактивных клиентов по убыванию.
city = get_table('city')
customer = get_table('customer')
address = get_table('address')

df = customer.join(address, on = 'address_id')\
            .groupby('city_id')\
            .agg(count('*').alias('cnt'), sum('active').alias('active'))\
            .join(city, on='city_id')['city','cnt','active']

sixth = df.withColumn('non_active', df.cnt - df.active)\
            .orderBy('non_active',ascending=False)['city','active','non_active']\



# Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах 
#(customer.address_id в этом city), и которые начинаются на букву “a”. 
# Тоже самое сделать для городов в которых есть символ “-”.

df = city.join(address, on='city_id')\
.join(customer, on='address_id')\
.join(rental, on='customer_id')\
.join(inventory, on='inventory_id')\
.join(film_category, on='film_id')\
.join(category, on='category_id')['city','name']

seventh = []

seventh.append(df.filter(df['city'].like("a%") | df['city'].like("A%"))\
.groupby('name').count().orderBy('count', ascending = False).first()['name'])

seventh.append(df.filter(df['city'].like("%-%"))\
        .groupby('name').count().orderBy('count', ascending = False).first()['name'])


