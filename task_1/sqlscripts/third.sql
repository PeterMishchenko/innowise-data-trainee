select room_id from student group by room_id order by  cast(max(birthday) as bigint) - cast(min(birthday) as bigint) desc limit 5;