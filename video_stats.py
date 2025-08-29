import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()
api_key = os.environ["API_KEY"]
chanel_handle = "MrBeast"
max_results = 50
base_url = "https://youtube.googleapis.com/youtube/v3"


def get_playlist_id(chanel_handle: str) -> str:
    try:
        url = base_url + "/channels"

        params = {
            "part": "contentDetails",
            "forHandle": chanel_handle,
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


if __name__ == "__main__":
    chanel_playlist_id = get_playlist_id(chanel_handle)
    print(f"Chanel playlist id for {chanel_handle} is {chanel_playlist_id}")
    video_ids = get_video_ids(playlist_id=chanel_playlist_id)
    print(video_ids)
