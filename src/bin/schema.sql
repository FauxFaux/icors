create table if not exists name (
    id integer primary key not null,
    value text not null
);

create table if not exists literal (
    id integer primary key not null,
    value text not null
);

create table if not exists quad (
    graph integer not null,
    subject integer not null,
    predicate integer not null,
    object integer not null
);

