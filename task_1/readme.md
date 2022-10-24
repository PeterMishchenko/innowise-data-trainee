# Task 1. Python 
Project assignment (russian)

Task 1. Python
Необходимо:
с использованием базы MySQL (или другую реляционную БД, например, PostgreSQL) создать схему данных соответствующую данным файлам (связь многие к одному).
написать скрипт, целью которого будет загрузка этих двух файлов и запись данных в базу

Запросы к базе данных чтобы вернуть:
* список комнат и количество студентов в каждой из них
* top 5 комнат, где самый маленький средний возраст студентов
* top 5 комнат с самой большой разницей в возрасте студентов
* список комнат где живут разнополые студенты.

Требования и замечания:
* предложить варианты оптимизации запросов с использования индексов
* в результате надо сгенерировать SQL запрос который добавить нужные индексы
* выгрузить результат в формате JSON или XML
* всю "математику" делать стоит на уровне БД.
* командный интерфейс должен поддерживать следующие входные параметры
    * students (путь к файлу студентов)
    * rooms (путь к файлу комнат)
    * format (выходной формат: xml или json)
* использовать ООП и SOLID.
* отсутствие использования ORM (использовать SQL)

## Requirements
* docker

## Running
To run, use `start.sh`
```
cd task_1
./start.sh
```
`start.sh` has three args:
* output format: json or xml (default `json`)
* students (default `python_script/data/student.json`)
* rooms.json (default `python_script/data/student.json`)

### Manual run
to run manually, copy your `students.json` and `rooms.json` files to python_script/data/

and run `docker-compose up` with OUTPUT_FORMAT variable
```
OUTPUT_FORMAT=json docker-compose up
```
