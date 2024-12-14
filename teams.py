from __future__ import annotations
from abc import ABC, abstractmethod
from functools import lru_cache 
import json
import logging
from pathlib import Path
from typing import Any, List, NamedTuple, Optional, Tuple

import duckdb
import pandas as pd
import requests

import globals

logging.basicConfig(level=logging.INFO)

class StartEndSeason(NamedTuple):
    start: int
    end: int

class ITeams(ABC):

    @abstractmethod
    def pull_teams(self):
        raise NotImplementedError()
    
    @abstractmethod
    def pull_team_season(self):
        raise NotImplementedError()

class TeamsAPI(ITeams):

    def __init__(self,
                 base_url: str = globals.BASEURL) -> None:
        """
        Constructor

        Args:
            base_url (str, optional): Baseurl for requests. Defaults to globals.BASEURL
        """
        self.base_url = base_url

    @lru_cache
    def pull_teams(self,
                   end_point: str = 'stats/rest/en/team') -> Any:
        """
        Pull team data

        Args:
            end_point (str, optional): Endpoint for teams data

        Returns:
            Any: Teams from global api as JSON object
        """
        url = self.base_url + end_point
        try:
            logging.info(f"Fetching data from url: {url}")
            r = requests.get(url)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching team data: {e}")
            raise RuntimeError(f"Error fetching team data: {e}")
        
    @lru_cache
    def pull_team_season(self,
                         triCode: str,
                         end_point: str = 'v1/roster-season/') -> List[int]:
        """
        Returns a list of seasons that the franchise played

        Args:
            triCode (str): team code
            end_point (str): api end point. Default v1/roster-season/

        Returns:
            List[int]: Seasons the team played
        """
        url = self.base_url + end_point + triCode
        try:
            logging.info(f"Fetching from url: {url}")
            r = requests.get(url)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching team data: {e}")
            raise RuntimeError(f"Error fetching team season: {e}")


class TeamsData:
    
    def __init__(self, teams: TeamsAPI) -> None:
        """
        Constructor

        Args:
            teams (TeamsAPI): Teams data interface
        """
        self.teams = teams

    def raw_results(self,
                    file_name: Path = Path('teams.json')) -> None:
        """
        Write the request results into a file

        Args:
            file_name (Path, optional): file name. Defaults to 'teams.json'.
        """
        try:
            logging.info("Pulling teams data")
            data = self.teams.pull_teams()
            file_name.write_text(json.dumps(data), encoding='utf-8')
            logging.info("Team data written to file {file_name}")
        except Exception as e:
            logging.error(f"Error writing {file_name} {e}")
            raise RuntimeError(f"Error writing results: {e}")

    def pull_teams_df(self) -> pd.DataFrame:
        """
        Format teams into a pandas DataFrame

        Returns:
            pd.DataFrame: Pandas DataFrame with teams
        """
        result: Any = self.teams.pull_teams()
        self.df = pd.DataFrame(result['data'])
        return self.df
    
    def to_csv(self,
               path: Path = Path('./data/teams.csv')) -> None:
        """
        Write results to csv file

        Args:
            path (Path, optional): path to write csv file. Defaults to './data/teams.csv'.
        """
        self.pull_teams_df()
        logging.info(f"Writing csv file {path}")
        self.df.to_csv(path, index=False)

    @staticmethod
    def split_season_years(date: int) -> StartEndSeason:
        """
        Split the season start and end years for the team

        Args:
            date (int): The start and end date integer

        Returns:
            StartEndSeason: Name tuple of start and end dates
        """
        if not  isinstance(date, int) or len(str(date)) != 8:
            raise ValueError(f"Date must be type int with two years concatenated in format YYYYYYYY")
        return StartEndSeason(date // 10_000, date % 10_000)


if __name__ == "__main__":
    season = TeamsData().split_season_years(20202021)
    print(season.start)
    print(season.end)
