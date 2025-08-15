import requests

# URL сервиса Event Store
events_store_url = "http://127.0.0.1:8020"

# Заголовки запроса
headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

# Последовательность событий для пользователя 1337055
user_id = 1127794
item_ids = [18734992, 18734992, 7785, 4731479]

# Отправляем запросы для каждого item_id
results = []
for item_id in item_ids:
    params = {"user_id": user_id, "item_id": item_id}
    resp = requests.post(events_store_url + "/put",
                         headers=headers, params=params)

    if resp.status_code == 200:
        results.append(resp.json())
    else:
        results.append(f"Ошибка: {resp.status_code}")

# Выводим результаты
print(results)
