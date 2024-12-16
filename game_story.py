from abc import ABC, abstractmethod
import json
import logging
from typing import Any

import requests

import globals

logging.basicConfig(
    level=logging.INFO,
    style='{',
    filename='./logs/game_story.log',
    filemode='a',
    format="{asctime} - {levelname} - {message}",
    datefmt="%Y-%m-%d %H:%M",
)


class IGameStoryAPI(ABC):

    @abstractmethod
    def pull_data(self):
        raise NotImplementedError()


class GameStoryAPI(IGameStoryAPI):

    def __init__(self, 
                 base_url: str = globals.BASEURL,
                 end_point: str = 'v1/wsc/game-story/') -> None:
        self.base_url: str = base_url
        self.end_point: str = end_point

    @staticmethod
    def validate(response, key) -> None:
        if key not in response:
            raise ValueError("Missing key {key} in response")

    def pull_data(self, game: int) -> Any:
        url = self.base_url + self.end_point + str(game)
        try:
            logging.info("Pulling data from {url}")
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()
            return data
        except requests.exceptions.RequestException:
            logging.error("Error pulling data for {url}")
            raise RuntimeError("Error pulling data for")


if __name__ == '__main__':
    data = GameStoryAPI().pull_data(2024020586)
    print(data)
