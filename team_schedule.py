import json

import requests

team = 'NYR'

r = requests.get(f'https://api-web.nhle.com/v1/club-schedule/{team}/month/now')

print(r.json())
