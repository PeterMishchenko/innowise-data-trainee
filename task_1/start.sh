#!/bin/bash
if [ -n "$1" ]
then 
OUTPUT_FORMAT="$1"
else
OUTPUT_FORMAT=json
fi

if [ -n "$2" ]
then 
cp "$2" python_script/data/students.json
fi

if [ -n "$3" ]
then 
cp "$3" python_script/data/rooms.json
fi
echo OUTPUT_FORMAT = $OUTPUT_FORMAT
OUTPUT_FORMAT=$OUTPUT_FORMAT docker-compose up --abort-on-container-exit --build
