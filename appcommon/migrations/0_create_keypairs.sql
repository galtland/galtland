CREATE TABLE IF NOT EXISTS keypairs
(
    id          INTEGER PRIMARY KEY NOT NULL,
    keypair BLOB                NOT NULL,
    name        TEXT             NOT NULL
);
