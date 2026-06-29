import json

with open('download_progress.json', 'r') as f:
    data = json.load(f)
print(len(data["processed_titles"]))