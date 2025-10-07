import json
from flask import Flask

app = Flask(__name__)

@app.template_filter('from_json')
def from_json_filter(value):
    try:
        return json.loads(value)
    except:
        return {}