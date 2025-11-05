CREATE SCHEMA IF NOT EXISTS tmdb;

CREATE TABLE IF NOT EXISTS tmdb.movies_final (
    id BIGINT PRIMARY KEY,
    title TEXT,
    release_date DATE,
    vote_average NUMERIC,
    vote_count BIGINT,
    popularity NUMERIC,
    runtime NUMERIC,
    genres TEXT,
    cast_top3 TEXT
);
