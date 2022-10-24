CREATE table room (
    id smallint not null,
    name character(10)
);

CREATE table student (
    id smallint not null,
    name character(30),
    room_id integer,
    sex int,
    birthday int
);

CREATE INDEX ind_student_room ON student(room_id);
