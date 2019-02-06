-- name: create-peer
INSERT INTO peers (name, email) VALUES($1, $2);

-- name: find-peers-by-email
SELECT name,email FROM peers WHERE email = $1;

-- name: find-one-peer-by-email
SELECT name,email FROM peers WHERE email = $1 LIMIT 1;
