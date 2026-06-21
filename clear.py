import json

with open('download_progress.json', 'r') as f:
    data = json.load(f)

    data['not_found_titles'] = []

with open('download_progress.json', 'w') as f:
    json.dump(data, f, indent=4)