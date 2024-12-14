from typing import Generator

import pytest

from teams import TeamsAPI, TeamsData, StartEndSeason

class TestTeams:

    @pytest.fixture
    def teams_api(self) -> Generator:
        yield TeamsAPI()

    @pytest.fixture
    def teams(self) -> Generator: 
        yield TeamsData(TeamsAPI())

    def test_season(self, teams):
        team_seasons = teams.split_season_years(20202021)

        assert team_seasons.start == 2020
        assert team_seasons.end == 2021

    def test_pull_teams(self, teams_api, mocker):

        mock_data = {"data":
                     [
                         {"id":32,"franchiseId":27,"fullName":"Quebec Nordiques","leagueId":133,"rawTricode":"QUE","triCode":"QUE"},
                         {"id":8,"franchiseId":1,"fullName":"Montréal Canadiens","leagueId":133,"rawTricode":"MTL","triCode":"MTL"},
                         {"id":58,"franchiseId":5,"fullName":"Toronto St. Patricks","leagueId":133,"rawTricode":"TSP","triCode":"TSP"},
                         {"id":7,"franchiseId":19,"fullName":"Buffalo Sabres","leagueId":133,"rawTricode":"BUF","triCode":"BUF"},],
        }

        mock_response = mocker.MagicMock()
        mock_response.json.return_value = mock_data

        mocker.patch('requests.get', return_value=mock_response)

        result = teams_api.pull_teams()

        assert len(result['data']) == 4

    def test_pull_teams_missing_data_key(self, teams_api, mocker):

        mock_data = {"missing key":
                     [
                         {"id":32,"franchiseId":27,"fullName":"Quebec Nordiques","leagueId":133,"rawTricode":"QUE","triCode":"QUE"},
                         {"id":8,"franchiseId":1,"fullName":"Montréal Canadiens","leagueId":133,"rawTricode":"MTL","triCode":"MTL"},
                         {"id":58,"franchiseId":5,"fullName":"Toronto St. Patricks","leagueId":133,"rawTricode":"TSP","triCode":"TSP"},
                         {"id":7,"franchiseId":19,"fullName":"Buffalo Sabres","leagueId":133,"rawTricode":"BUF","triCode":"BUF"},],
        }

        mock_response = mocker.MagicMock()
        mock_response.json.return_value = mock_data

        mocker.patch('requests.get', return_value=mock_response)

        with pytest.raises(ValueError) as test:
            result = teams_api.pull_teams()
