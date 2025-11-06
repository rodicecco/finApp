from flask_cors import CORS
from flask import Flask, render_template, jsonify, redirect, url_for, request
import datamgr as dm

app = Flask(__name__)
# enable CORS for the econdata endpoint so the frontend dev server can call it
CORS(app, resources={r"/econdata": {"origins": "*"}})


@app.route('/econdata', methods=['POST'])
def econdata():
    data = request.get_json()
    series_ids = [x for x in data.get("series_ids", []) if x != '']

    obj = dm.EconData(series_ids)
    obj.apply_transformations()

    api = obj.api_json()

    return jsonify(api)

if __name__=="__main__":
    app.run(debug=True)

