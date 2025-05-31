CREATE DATABASE tennis;

\connect tennis;

CREATE USER tennis_user WITH ENCRYPTED PASSWORD 'tennis_user_strong_password';

GRANT ALL PRIVILEGES ON DATABASE tennis TO tennis_user;

CREATE TABLE IF NOT EXISTS atp_matches (
    id SERIAL PRIMARY KEY,
    tournament VARCHAR(100),
    year INT,
    winner VARCHAR(100),
    loser VARCHAR(100),
    score VARCHAR(50),
    round VARCHAR(10),
    surface VARCHAR(20)
);