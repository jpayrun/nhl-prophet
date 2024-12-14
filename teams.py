from functools import lru_cache 
import json
from pathlib import Path
from typing import Any, List, NamedTuple, Optional, Tuple

import duckdb
import pandas as pd
import requests

import globals

class StartEndSeason(NamedTuple):
    start: int
    end: int

class TeamsAPI:

    def __init__(self,
                 base_url: str = globals.BASEURL) -> None:
        """
        Constructor

        Args:
            base_url (str, optional): Baseurl for requests. Defaults to globals.BASEURL
        """
        self.base_url = base_url

    def pull_teams(self,
                   end_point: str = 'stats/rest/en/team') -> Any:
        """
        Pull team data

        Args:
            end_point (str, optional): Endpoint for teams data

        Returns:
            Any: Teams from global api
        """
        try:
            url = self.base_url + end_point
            r = requests.get(url)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Error fetching team data: {e}")
        
    def pull_team_season(self,
                         end_point: str = 'v1/roster-season/',
                         triCode: str) -> List[int]:
        """
        Returns a list of seasons that the franchise played

        Args:
            triCode (str): team code

        Returns:
            List[int]: Seasons the team played
        """
        try:
            url = self.base_url + end_point + triCode
            r = requests.get(url)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Error fetching team season: {e}")


class TeamsData:
    pass

class Teams:

    def __init__(self) -> None:
        """
        Constructor
        """
    
    def raw_results(self,
                    file_name: Path = Path('teams.json')) -> None:
        """
        Write the request results into a file

        Args:
            file_name (Path, optional): file name. Defaults to 'teams.json'.
        """
        data = self.pull_teams()
        with open(file=file_name, 'w') as file:
            data = json.dumps(data)
            file.write(data)

    def pull_teams_df(self) -> pd.DataFrame:
        """
        Format teams into a pandas DataFrame

        Returns:
            pd.DataFrame: Pandas DataFrame with teams
        """
        result: Any = self.pull_teams()
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
        return StartEndSeason(date // 10_000, date % 10_000)

if __name__ == "__main__":
    season = Teams().split_season_years(20202021)
    print(season.start)
    print(season.end)
