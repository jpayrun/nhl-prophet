from __future__ import annotations
from abc import ABC, abstractmethod
from functools import lru_cache
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Tuple

import pandas as pd
import requests

import globals

logging.basicConfig(
    filename="./logs/teams.log",
    format="{asctime} - {levelname} - {message}",
    level=logging.INFO,
    style='{',
    datefmt="%Y-%m-%d %H:%M",)

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

    def valid_response(self, response: Dict, key: str) -> None:
        """
        Assert key is in API response

        Args:
            response (Dict): The API response
            key (str): Key to ensure is in response

        Raises:
            RuntimeError: If error, raise the runtime error
        """
        if key not in response:
            logging.error(f"Required key {key} missing in data")
            raise ValueError("Key not in data returned")

    @lru_cache
    def pull_teams(self,
                   teams_url: str = 'https://api.nhle.com/',
                   end_point: str = 'stats/rest/en/team') -> Any:
        """
        Pull team data

        Args:
            teams_url (str, optional): Url for teams data
            end_point (str, optional): Endpoint for teams data

        Returns:
            Any: Teams from global api as JSON object
        """
        url = teams_url + end_point
        try:
            logging.info(f"Fetching data from url: {url}")
            r = requests.get(url)
            r.raise_for_status()
            data = r.json()
            self.valid_response(data, 'data')
            return data
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
            raise RuntimeError(f"Error fetching team {triCode} season: {e}")


class TeamsData:

    def __init__(self, teams: ITeams) -> None:
        """
        Constructor

        Args:
            teams (ITeams): Teams data interface
        """
        self.teams: ITeams = teams
        self.teams_data: Optional[Any] = None
        self._df: Optional[pd.DataFrame] = None

    @property
    def df(self) -> pd.DataFrame:
        """
        Get the df value

        Raises:
            ValueError: Not set error

        Returns:
            pd.DataFrame: Returns the DataFrame
        """
        if self._df is None:
            raise ValueError("df called before initialized. Please run the pull_teams")
        return self._df

    @df.setter
    def df(self, value: pd.DataFrame) -> None:
        """
        Setter

        Args:
            value (pd.DataFrame): The value for the DataFrame

        Raises:
            TypeError: Invalid entry
        """
        if not isinstance(value, pd.DataFrame):
            raise TypeError("df must be set to type pandas DataFrame")
        self._df = value

    def pull_teams(self, refresh: bool = False) -> Any:
        """
        Get the teams data if None or manual refresh

        Args:
            refresh (bool, optional): Teams data from Teams object. Defaults to False.

        Returns:
            Any: Teams data
        """
        if self.teams_data is None or refresh:
            self.teams_data = self.teams.pull_teams()
        return self.teams_data

    def pull_teams_df(self) -> pd.DataFrame:
        """
        Format teams into a pandas DataFrame

        Returns:
            pd.DataFrame: Pandas DataFrame with teams
        """
        result: Any = self.pull_teams()
        self.df = pd.DataFrame(result['data'])
        return self.df

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
            raise ValueError("Date must be type int with two years concatenated in format YYYYYYYY")
        return StartEndSeason(date // 10_000, date % 10_000)

    def season_years_list(self, dates: List[int]) -> List[Tuple[StartEndSeason]]:
        result: List[StartEndSeason] = []
        for date in dates:
            result.append(self.split_season_years(date))
        return result

    def add_season(self) -> None:
        """
        Added the season to the DataFrame
        """
        self.pull_teams_df()
        self.df = (self.
                   df.
                   assign(Seasons = lambda df_a: df_a.triCode.apply(self.teams.pull_team_season)).
                   assign(StartEndSeason = lambda df_a: df_a.Seasons.apply(self.season_years_list))
                   )


class IWriteTeamsData(ABC):

    @abstractmethod
    def raw_results(self):
        raise NotImplementedError()

    @abstractmethod
    def to_csv(self):
        raise NotImplementedError()


class WriteTeamsDataLocal(IWriteTeamsData):

    def __init__(self, teams: TeamsData) -> None:
        """
        Constructor

        Args:
            teams (TeamsData): _description_
        """
        self.teams: TeamsData = teams

    def set_up(self) -> None:
        """
        Set up the data for csv export
        """
        self.teams.add_season()

    def raw_results(self,
                    file_name: Path = Path('./raw/teams.json')) -> None:
        """
        Write the raw results to local location

        Args:
            file_name (Path, optional): File name and path. Defaults to Path('./raw/teams.json').

        Raises:
            RuntimeError: Error writing file
        """
        try:
            logging.info("Writing teams data")
            data = teams.pull_teams()
            file_name.parent.mkdir(parents=True, exist_ok=True)
            file_name.write_text(data, encoding='utf-8')
            logging.info("Finished writing file")
        except Exception as e:
            logging.error("Error writing teams data")
            raise RuntimeError("Error writing teams data {e}")

    def to_csv(self,
               path: Path = Path('./data/teams.csv')) -> None:
        """
        Write results to csv file

        Args:
            path (Path, optional): path to write csv file. Defaults to './data/teams.csv'.
        """
        self.set_up()
        df = self.teams.df
        logging.info(f"Writing csv file {path}")
        df.to_csv(path, index=False)


if __name__ == "__main__":
    # season = TeamsData().split_season_years(20202021)
    # print(season.start)
    # print(season.end)

    api = TeamsAPI()
    teams = TeamsData(api)
    file_writer = WriteTeamsDataLocal(teams)

    file_writer.to_csv()

    # teams = TeamsAPI()
    # data = teams.pull_teams()
    # print(data)

    # data = teams.pull_team_season('NYR')
    # print(data)
