import os
import duckdb
from dotenv import load_dotenv

load_dotenv()

CONN_STRING = (
    f"dbname={os.getenv('POSTGRES_DB')} "
    f"user={os.getenv('POSTGRES_USER')} "
    f"password={os.getenv('POSTGRES_PASSWORD')} "
    f"host={os.getenv('POSTGRES_HOST')} "
    f"port={os.getenv('POSTGRES_PORT')}"
)


def main():
    conn = duckdb.connect()
    try:
        conn.sql(f"""
            INSTALL postgres_scanner;
            LOAD postgres_scanner;

            ATTACH '{CONN_STRING}' AS pg (TYPE postgres);

            CREATE SCHEMA IF NOT EXISTS pg.dds;

            -- Hub: посты
            CREATE TABLE IF NOT EXISTS pg.dds.hub_post (
                hub_post_id SERIAL PRIMARY KEY,
                post_bk INT UNIQUE NOT NULL
            );

            INSERT INTO pg.dds.hub_post (post_bk)
            SELECT DISTINCT id FROM pg.stg.posts
            ON CONFLICT (post_bk) DO NOTHING;

            -- Hub: пользователи
            CREATE TABLE IF NOT EXISTS pg.dds.hub_user (
                hub_user_id SERIAL PRIMARY KEY,
                user_bk INT UNIQUE NOT NULL
            );

            INSERT INTO pg.dds.hub_user (user_bk)
            SELECT DISTINCT userId FROM pg.stg.posts
            ON CONFLICT (user_bk) DO NOTHING;

            -- Link: связь
            CREATE TABLE IF NOT EXISTS pg.dds.link_post_user (
                link_id SERIAL PRIMARY KEY,
                hub_post_id INT,
                hub_user_id INT
            );

            INSERT INTO pg.dds.link_post_user (hub_post_id, hub_user_id)
            SELECT hp.hub_post_id, hu.hub_user_id
            FROM pg.stg.posts s
            JOIN pg.dds.hub_post hp ON s.id = hp.post_bk
            JOIN pg.dds.hub_user hu ON s.userId = hu.user_bk;

            -- Satellite: атрибуты
            CREATE TABLE IF NOT EXISTS pg.dds.sat_post (
                sat_post_id SERIAL PRIMARY KEY,
                hub_post_id INT,
                title TEXT,
                body TEXT,
                load_date TIMESTAMP DEFAULT now()
            );

            INSERT INTO pg.dds.sat_post (hub_post_id, title, body)
            SELECT hp.hub_post_id, s.title, s.body
            FROM pg.stg.posts s
            JOIN pg.dds.hub_post hp ON s.id = hp.post_bk;

        """)
        print("ELT 2 завершён успешно")

    except Exception as error:
        print(f"Ошибка в ELT 2: {error}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()