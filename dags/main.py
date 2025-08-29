from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, get_video_data, save_to_json

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "dataengineers"
}