EXPLAIN ANALYZE select room_id from student  group by room_id having avg(sex)!=0 or avg(sex)!=1 order by room_id;