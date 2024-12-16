from abc import ABC, abstractmethod
import requests
import logging

import globals

logging.basicConfig(level=logging.INFO)


class ISkaters(ABC):

    @abstractmethod
    def pull_skaters(self):
        raise NotImplementedError()

class Skater(ISkaters):

    def __init__(self, base_url: str = globals.BASEURL) -> None:
        self.base_url: str = base_url

    def pull_skaters(self):
        pass

url = globals.BASEURL + '/v1/skater-stats-leaders/current'

r: requests.Response = requests.get(url)

print(r.json())
