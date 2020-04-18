from flask import Flask, render_template, request

from service.objects import Types
from service.processor import Processor
from service.utils import format_error, jsonify

app = Flask(__name__)


@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')


@app.route('/data', methods=['GET'])
def sorted_data():
    sort_column = request.args.get('sort')
    max_column = request.args.get('max')
    min_column = request.args.get('min')

    columns = {
        'sale_date': {
            'type': Types.date,
            'date_format': '%Y-%m-%d',
            'index': 0,
        },
        'product_group': {
            'type': Types.string,
            'index': 1
        },
        'product_name': {
            'type': Types.string,
            'index': 2
        },
        'cost': {
            'type': Types.integer,
            'index': 3
        },
        'quantity': {
            'type': Types.integer,
            'index': 4
        },
    }

    processor = Processor(data_name='datasource.csv', columns=columns, params={
        'sort': sort_column,
        'max': max_column,
        'min': min_column
    })

    try:
        processor.is_valid_params(exception_raise=True)

    except ValueError as exception:
        param = exception.args[0]
        error = format_error("The '%s' parameter is not valid" % param)
        return jsonify(error, status_code=400)

    try:
        data = processor.get_processed_data()

    except FileNotFoundError:
        error = format_error('The data file is not found')
        return jsonify(error, status_code=422)

    except TypeError as error:
        error = format_error('Invalid parameter type')
        return jsonify(error, status_code=400)

    return jsonify(data, status_code=200)


if __name__ == '__main__':
    app.run(debug=True)
