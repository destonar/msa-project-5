create table if not exists orders (
    id bigserial primary key,
    userId bigint,
    created timestamptz default now(),
    status text
);

insert into orders (userId, status) values
(1, 'success'),
(2, 'fail'),
(2, 'fail'),
(3, 'fail'),
(3, 'fail');