CREATE table room (
    id integer not null,
    name character(10)
);

CREATE table student (
    id integer not null,
    name character(30),
    room_id integer,
    sex int,
    birthday int
)