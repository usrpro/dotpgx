-- name: create-peers-table
CREATE TABLE peers (
    id serial NOT NULL,
    name VARCHAR,
    email VARCHAR,
    CONSTRAINT peers_pkey PRIMARY KEY (id)
);

-- name: drop-peers-table
DROP TABLE peers;
