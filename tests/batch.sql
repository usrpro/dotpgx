-- These queries are unlabeled, so they get auto-numbered

create temporary table batchme (
    id serial NOT NULL,
    content varchar,
    constraint batchme_pkey primary key (id)
);

insert into batchme (content) values ('Hello');

insert into batchme (content) values ('World!');

insert into batchme (content) values ('Spanac');

insert into batchme (content) values ('Eggs');

select content from batchme;

select content from batchme where content = 'Spanac' limit 1;