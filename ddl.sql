-- СЛОЙ STG (Staging)
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.posts (
    userId INT,
    id INT,
    title TEXT,
    body TEXT,
    _loaded_at TIMESTAMP DEFAULT now()
);

-- СЛОЙ DDS (Data Vault 2.0)
CREATE SCHEMA IF NOT EXISTS dds;

-- Hub: посты
CREATE TABLE IF NOT EXISTS dds.hub_post (
    hub_post_id SERIAL PRIMARY KEY,
    post_bk INT UNIQUE NOT NULL
);

-- Hub: пользователи
CREATE TABLE IF NOT EXISTS dds.hub_user (
    hub_user_id SERIAL PRIMARY KEY,
    user_bk INT UNIQUE NOT NULL
);

-- Link: связь пост-пользователь
CREATE TABLE IF NOT EXISTS dds.link_post_user (
    link_id SERIAL PRIMARY KEY,
    hub_post_id INT REFERENCES dds.hub_post(hub_post_id),
    hub_user_id INT REFERENCES dds.hub_user(hub_user_id)
);

-- Satellite: атрибуты постов
CREATE TABLE IF NOT EXISTS dds.sat_post (
    sat_post_id SERIAL PRIMARY KEY,
    hub_post_id INT REFERENCES dds.hub_post(hub_post_id),
    title TEXT,
    body TEXT,
    load_date TIMESTAMP DEFAULT now()
);