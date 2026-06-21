import requests
import time

s = requests.Session()
s.proxies = {'http': 'socks5h://127.0.0.1:9050', 'https': 'socks5h://127.0.0.1:9050'}
s.headers.update({"User-Agent": "Mozilla/5.0"})

try:
    url = "https://dblp.org/search/publ/api?q=venue:SIGIR+year:2024&format=json&h=10&f=0"
    response = s.get(url, timeout=30)
    data = response.json()
    print("Found:", data['result']['hits']['@total'])
except Exception as e:
    print(e)
