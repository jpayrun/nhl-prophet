from abc import ABC, abstractmethod
from functools import lru_cache
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import requests
import pandas as pd

import globals

logging.basicConfig(
    filename="./logs/rosters.log",
    format="{asctime} - {levelname} - {message}",
    level=logging.INFO,
    style='{',
    datefmt="%Y-%m-%d %H:%M",)


class IRoster(ABC):

    @abstractmethod
    def get_current_roster(self):
        raise NotImplementedError()
    

class RosterAPI(IRoster):

    def __init__(self, base_url: str = globals.BASEURL) -> None:
        self.base_url = base_url

    def valid_data(self, response: Dict, key: str) -> None:
        if key not in response:
            logging.ERROR(f"Unable to pull roster for team, missing key {key}")
            raise ValueError(f"Invalid response, missing key {key} in response")

    @lru_cache
    def get_current_roster(self, team: str) -> Any:
        """
        Get the current team roster

        Args:
            team (str): Team Id

        Raises:
            RuntimeError: Error is error pulling roster

        Returns:
            Any: Results for the current roster
        """
        url = self.base_url + 'v1/roster/' + team + '/current'

        try:
            logging.info(f"Pulling roster for team {team}")
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()
            self.valid_data(data, 'forwards')
            self.valid_data(data, 'goalies')
            self.valid_data(data, 'defensemen')
            logging.info(f"Data pulled for team {team}")
            return data
        except requests.exceptions.RequestException as e:
            logging.error(f"Unable to pull roster for team {team} error: {e}")
            raise RuntimeError(f"Error pulling {team} team roster {e}")


class RosterData:

    def __init__(self, roster: IRoster) -> None:
        """
        Constructor

        Args:
            roster (IRoster): Roster external data interface
        """
        self.roster_api: IRoster = roster
        self._roster_data: Optional[Any] = None

    def roster_data(self, team: str) -> Any:
        """
        Set the roster data if None

        Args:
            team (str): Team to pull data for

        Returns:
            Any: Roster data returned
        """
        if self._roster_data is None:
            self._roster_data = self.roster_api.get_current_roster(team)
        return self._roster_data
    
    def raw_data(self, team: str, file_name: Path = Path('roster.json')) -> None:
        """
        Save the raw data

        Args:
            team (str): Team to pull data fro
            file_name (Path, optional): _description_. Defaults to Path('roster.json').

        Raises:
            RuntimeError: _description_
        """
        try:
            logging.info(f"Pulling roster for team {team}")
            data = self.roster_data(team)
            file_name.parent.mkdir(parents=True, exist_ok=True) #Ensure the path exists
            file_name.write_text(json.dumps(data), encoding='utf-8')
            logging.info(f"Wrote file for team {team}")
        except Exception as e:
            logging.error(f"Issue writing file for team {team} error {e}")
            raise RuntimeError(f"Error writing file {e}")

if __name__ == "__main__":
    roster_api = RosterAPI()
    roster_data = RosterData(roster_api)
    # data = roster_data.roster_data('NYR')
    # data = roster_api.get_current_roster('NYR')
    # print(data)
    roster_data.raw_data('NYR', Path('rangers_roster.json'))

