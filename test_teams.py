from typing import Generator

import pytest

from teams import Teams, StartEndSeason

class TestTeams:

    @pytest.fixture
    def teams(self) -> Generator: 
        yield Teams()

    def test_season(self, teams):
        team_seasons = teams.split_season_years(20202021)

        assert team_seasons.start == 2020
        assert team_seasons.end == 2021
