import logging
from psycopg2 import sql

logger = logging.getLogger(__name__)
table = "yt_api"


def insert_rows(conn, cur, schema, row):
    try:
        if schema == "staging":
            video_id = "video_id"
            insert_sql = sql.SQL(
                """
                INSERT INTO {}.{} ("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s)
                """
            ).format(sql.Identifier(schema), sql.Identifier(table))
            cur.execute(insert_sql, row)
        else:
            video_id = "Video_ID"
            insert_sql = sql.SQL(
                """
                INSERT INTO {}.{}("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
                VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s)
                """
            ).format(sql.Identifier(schema), sql.Identifier(table))
            cur.execute(insert_sql, row)

        conn.commit()
        logger.info(f"Inserted row with Video Id : {row[video_id]}")

    except Exception as e:
        logger.error(f"Error occured while inserting Video Id : {row[video_id]}")  # type: ignore
        raise e


def update_rows(conn, cur, schema, row):
    try:
        # staging
        if schema == "staging":
            video_id = "video_id"
            upload_date = "publishedAt"
            video_title = "title"
            video_views = "viewCount"
            likes_count = "likeCount"
            comments_count = "commentCount"
        # core
        else:
            video_id = "Video_ID"
            upload_date = "Upload_Date"
            video_title = "Video_Title"
            video_views = "Video_Views"
            likes_count = "Likes_Count"
            comments_count = "Comments_Count"

        cur.execute(
            f"""
            UPDATE {schema}.{table}
            SET "Video_Title" = %({video_title})s,
                "Video_Views" = %({video_views})s, 
                "Likes_Count" = %({likes_count})s, 
                "Comments_Count" = %({comments_count})s
            WHERE "Video_ID" = %({video_id})s AND "Upload_Date" = %({upload_date})s;
            """,
            row,
        )

        conn.commit()

        logger.info(f"Updated row with Video_ID: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error updating row with Video_ID: {row[video_id]} - {e}")
        raise e


def delete_rows(conn, cur, schema, ids_to_delete):

    try:

        ids_to_delete = f"""({', '.join(f"'{id}'" for id in ids_to_delete)})"""

        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" IN {ids_to_delete};
            """
        )

        conn.commit()
        logger.info(f"Deleted rows with Video_IDs: {ids_to_delete}")

    except Exception as e:
        logger.error(f"Error deleting rows with Video_IDs: {ids_to_delete} - {e}")
        raise e
