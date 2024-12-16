from abc import ABC, abstractmethod
import json
import logging
from pathlib import Path
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
            logging.info(f"Pulling data from {url}")
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()
            return data
        except requests.exceptions.RequestException as e:
            logging.error(f"Error pulling data for {url}")
            raise RuntimeError(f"Error pulling data for {url} error {e}")


class GameStoryData:

    def __init__(self, game_story_api: IGameStoryAPI) -> None:
        """
        Constructor

        Args:
            game_story_api (IGameStoryAPI): Interface for game story api data
        """
        self.game_story_api = game_story_api

    def pull_data(self, game_id: int) -> Any:
        """
        Pull data for the game

        Args:
            game_id (int): Game to pull data for

        Returns:
            Any: Json of the game data
        """
        return self.game_story_api.pull_data(game)


class IWriteGameStory(ABC):

    @abstractmethod
    def raw_data(self) -> None:
        raise NotImplementedError


class WriteGameStoryLocal(IWriteGameStory):

    def __init__(self, game_data: GameStoryData) -> None:
        """
        Constructor

        Args:
            game_data (GameStoryData): Game Data Instance
        """
        self.game_data = game_data

    def raw_data(self,
                 game_id: int,
                 path: str = './raw/',
                 file_name: Path = 'game_story') -> None:
        """
        Write the raw game data to a file

        Args:t
            game_id (int): Game id
            path (str, optional): Path to store raw data. Defaults to './raw/'.
            file_name (Path, optional): File Name. Defaults to 'game_story'.

        Raises:
            RuntimeError: Error running the script
        """
        try:
            logging.info(f"Writing file for game {game_id}")
            data = self.game_data.pull_data(game_id=game_id)
            data = json.dumps(data)
            file = Path(path + file_name + '_' + str(game_id) + '.json')
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text(data, encoding='utf-8')
            logging.info(f"Wrote file for game {game_id}")
        except Exception as e:
            logging.error(f"Error writing file for game {game_id} error {e}")
            raise RuntimeError(f"Error writing file {e}")


if __name__ == '__main__':
    # Test game
    game = 2024020586
    api = GameStoryAPI()
    data = GameStoryData(api)
    # print(data.pull_data(game_id=game))
    WriteGameStoryLocal(data).raw_data(game_id=game)
