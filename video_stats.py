import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()


def get_playlist_id(chanel_handle: str):
    try:
        url = "https://youtube.googleapis.com/youtube/v3/channels"

        api_key = os.environ["API_KEY"]

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


if __name__ == "__main__":
    chanel_handle = "MrBeast"
    chanel_playlist_id = get_playlist_id(chanel_handle)
    print(f"Chanel playlist id for {chanel_handle} is {chanel_playlist_id}")
