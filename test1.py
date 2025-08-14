import requests

recommendations_url = 'http://127.0.0.1:8000/recommendations'

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
params = {"user_id": 0}

resp = requests.post(recommendations_url, headers=headers, params=params)

if resp.status_code == 200:
    recs = resp.json()
else:
    recs = []
    print(f"status code: {resp.status_code}")

print(recs)

# {'recs': []}
