-- This file serves to test the parser
/* 
Comments should not be included in the parsed result
Also no multi line comments.
Query terminates on semi-colon ; <- this one should be ignored
Empty lines should be stripped off.
Comment lines starting with --name: will act as query tagname.
*/

--name: one
select 1 from
    users -- Exlude this comment also!
    where $1 = me;

-- name: two
 -- Space in front of comment

select 2;

-- Unnamed query

select 3;

-- Ommited semi-colon
select 4

-- name: five
select 5

-- name: func
create or replace function tester()
    returns integer
    language 'sql'
    as $$
        select 1;
    $$;

create or replace function another()
    returns integer
    language 'sql'
    as $$
        select 2;
    $$;