import json

with open('tex_pipeline_checkpoint.json', 'r') as f:
    data = json.load(f)
print(len(data["processed"]))