import csv
import json
import os

from flask import Response

from settings import DATA_DIR


def get_reader_data(data_name: str):
    data_path = os.path.join(DATA_DIR, data_name)

    if not os.access(data_path, os.F_OK):
        raise FileNotFoundError

    file = open(data_path, 'r', encoding='utf-8')
    reader = csv.reader(file, delimiter=',')
    _ = next(reader)
    return reader


def is_param_valid(param: str or None, columns: list):
    if param is None:
        return True

    return param in columns


def format_error(text):
    return {
        'status': 'error',
        'detail': text
    }


def jsonify(data: dict or list, status_code: int):
    return Response(
        response=json.dumps(data, indent=2, ensure_ascii=False),
        status=status_code,
        mimetype='application/json'
    )
