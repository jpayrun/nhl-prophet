from typing import Any, List, NamedTuple, Tuple

import duckdb
import pandas as pd
import requests

import globals

class StartEndSeason(NamedTuple):
    start: int
    end: int

class Teams:

    def pull_teams(self,
                   url: str = 'https://api.nhle.com/stats/rest/en/team') -> Any:
        r = requests.get(url)
        return r.json()

    def pull_teams_df(self) -> pd.DataFrame:
        result: Any = self.pull_teams()
        self.df = pd.DataFrame(result['data'])
        return self.df
    
    def to_csv(self,
               path: str = './data/teams.csv') -> None:
        self.df.to_csv(path, index=False)
                       
    @staticmethod
    def pull_team_season(triCode: str) -> List[int]:
        """
        Returns a list of seasons that the franchise played

        Args:
            triCode (str): _description_

        Returns:
            List[int]: _description_
        """
        url = f"https://api-web.nhle.com/v1/roster-season/{triCode}"

        r = requests.get(url)

        return r.json()
    
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

# with duckdb.connect('nhl.db') as con:
#     print(con.sql("SELECT * FROM teams"))

# with duckdb.connect('nhl.db') as con:
#     con.sql("""CREATE TABLE teams (
#             id integer,
#             franchiseId integer,
#             fullName text,
#             leagueId integer,
#             rawTricode text,
#             triCode text,
#             primary key (id))""")
#     con.sql(
#     """
#     insert into teams
#     (select * from df)
#     """)
