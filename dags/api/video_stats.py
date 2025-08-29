import json
import requests
from pathlib import Path
from datetime import date

from airflow.decorators import task
from airflow.models import Variable

# import os
# from dotenv import load_dotenv
# load_dotenv()
api_key = Variable.get("API_KEY")
channel_handle = Variable.get("CHANNEL_HANDLE")
max_results = 50
base_url = "https://youtube.googleapis.com/youtube/v3"


@task
def get_playlist_id(channel_handle: str) -> str:
    try:
        url = base_url + "/channels"

        params = {
            "part": "contentDetails",
            "forHandle": channel_handle,
            "key": api_key,
        }

        response = requests.get(url=url, params=params)
        response.raise_for_status()

        data = response.json()
        # print(json.dumps(data, indent=4))
        channel_items = data["items"][0]
        chanel_playlist_id = channel_items["contentDetails"]["relatedPlaylists"][
            "uploads"
        ]
        return chanel_playlist_id
    except requests.exceptions.RequestException as e:
        raise e


@task
def get_video_ids(playlist_id: str) -> list[str]:
    try:
        video_ids = []
        page_token = None
        url = base_url + "/playlistItems"
        params = {"part": "contentDetails", "playlistId": playlist_id, "key": api_key}

        while True:
            if page_token:
                params["pageToken"] = page_token

            response = requests.get(url=url, params=params)
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                video_id = item["contentDetails"]["videoId"]
                video_ids.append(video_id)

            page_token = data.get("nextPageToken")

            if not page_token:
                break

        return video_ids

    except requests.exceptions.RequestException as e:
        raise e


@task
def get_video_data(video_id_list: list[str]):

    def batch_list(video_id_list: list[str], batch_size: int = 50):
        for idx in range(0, len(video_id_list), batch_size):
            yield video_id_list[idx : idx + batch_size]

    try:
        extracted_data = []
        url = base_url + "/videos"
        params = {"part": ["contentDetails", "snippet", "statistics"], "key": api_key}
        for batch in batch_list(video_id_list):
            video_ids_str = ",".join(batch)
            params["id"] = video_ids_str
            response = requests.get(url=url, params=params)
            response.raise_for_status()
            data = response.json()
            for item in data.get("items", []):
                video_id = item["id"]
                snippet = item["snippet"]
                content_details = item["contentDetails"]
                statistics = item["statistics"]
                video_data = {
                    "video_id": video_id,
                    "title": snippet["title"],
                    "publishedAt": snippet["publishedAt"],
                    "duration": content_details["duration"],
                    "viewCount": statistics.get("viewCount"),
                    "likeCount": statistics.get("likeCount"),
                    "commentCount": statistics.get("commentCount"),
                }
                extracted_data.append(video_data)

        return extracted_data

    except requests.exceptions.RequestException as e:
        raise e


@task
def save_to_json(extracted_data: list[dict]):
    file_dir = Path("data")
    file_dir.mkdir(exist_ok=True)
    file_name = f"YT_data_{date.today()}.json"
    file_path = file_dir / file_name
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(extracted_data, f, indent=4, ensure_ascii=False)
    print(f"File successfully saved at : {file_path}")


if __name__ == "__main__":
    chanel_playlist_id = get_playlist_id(channel_handle)
    print(f"Chanel playlist id for {channel_handle} is {chanel_playlist_id}")
    video_ids = get_video_ids(playlist_id=chanel_playlist_id)
    print(video_ids)
    video_data = get_video_data(video_ids)
    print(video_data)
    save_to_json(video_data)
