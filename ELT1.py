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
API_URL = os.getenv('API_URL')


def main():
    conn = duckdb.connect()
    try:
        conn.sql(f"""
            INSTALL postgres_scanner;
            LOAD postgres_scanner;

            ATTACH '{CONN_STRING}' AS pg (TYPE postgres);

            CREATE SCHEMA IF NOT EXISTS pg.stg;

            CREATE TABLE IF NOT EXISTS pg.stg.posts (
                userId INT,
                id INT,
                title TEXT,
                body TEXT,
                _loaded_at TIMESTAMP DEFAULT now()
            );

            INSERT INTO pg.stg.posts
            SELECT userId, id, title, body
            FROM read_json_auto('{API_URL}');

            SELECT COUNT(*) FROM pg.stg.posts;
        """)
        print("ELT 1 завершён: данные загружены в stg.posts")

    except Exception as error:
        print(f"Ошибка в ELT 1: {error}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()