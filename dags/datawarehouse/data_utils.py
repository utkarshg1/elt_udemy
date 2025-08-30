from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

table = "yt_api"

def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cur


def close_conn_cur(conn, cur):
    cur.close()
    conn.close()


def create_schema(schema):
    conn, cur = get_conn_cursor()
    schema_sql = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
        sql.Identifier(schema)
    )
    cur.execute(schema_sql)
    conn.commit()
    close_conn_cur(conn, cur)


def create_table(schema):
    conn, cur = get_conn_cursor()
    if schema == "staging":
        table_sql = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{}(
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" VARCHAR(20) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
            );
            """
        ).format(
            sql.Identifier(schema).
            sql.Identifier(table)
        )
    else:
        table_sql = sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{}(
                "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                "Video_Title" TEXT NOT NULL,
                "Upload_Date" TIMESTAMP NOT NULL,
                "Duration" TIME NOT NULL,
                "Video_Type" VARCHAR(10) NOT NULL,
                "Video_Views" INT,
                "Likes_Count" INT,
                "Comments_Count" INT
            );
            """
        ).format(
            sql.Identifier(schema).
            sql.Identifier(table)
        )

    cur.execute(table_sql)
    cur.commit()
    close_conn_cur(conn, cur)


def get_video_ids(cur, schema):
    id_sql = sql.SQL("""SELECT "Video_ID" FROM {}.{};""").format(
        sql.Identifier(schema),
        sql.Identifier(table)
    )

    cur.execute(id_sql)
    ids = cur.fetchall()
    video_ids = [row["Video_ID"] for row in ids]
    return video_ids
