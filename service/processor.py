from datetime import datetime

from service.objects import Types
from service.utils import get_reader_data


class Processor:
    def __init__(self, data_name: str, columns: dict, params: dict):
        self.data_name = data_name
        self.columns = columns
        self.params = params

    def is_valid_params(self, exception_raise=False):
        for key in self.params:
            status = self.params[key] is None or self.params[key] in self.columns

            if not status and exception_raise:
                raise ValueError(key)

        return True

    @staticmethod
    def format_element(column, element):
        data_type = column['type']

        if data_type == Types.date:
            return datetime.strptime(element, column['date_format'])

        elif data_type == Types.datetime:
            return datetime.strptime(element, column['date_format'])

        elif data_type == Types.integer:
            return int(element)

        elif data_type == Types.float:
            return float(element)

        elif data_type == Types.string:
            return str(element)

        else:
            return element

    @staticmethod
    def to_json(column, element):
        data_type = column['type']

        if data_type == Types.date:
            return element.strftime(column['date_format'])

        elif data_type == Types.datetime:
            return element.strftime(column['date_format'])

        else:
            return element

    def data_to_json(self, data):
        columns = list(self.columns.keys())

        return [
            [
                self.to_json(self.columns[columns[index]], element)
                for index, element in enumerate(row)
            ] for row in data
        ]

    def format_row(self, row):
        for column in self.columns.values():
            index = column['index']
            row[index] = self.format_element(column, row[index])
        return row

    def load_data(self):
        data = get_reader_data(self.data_name)
        return [self.format_row(row) for row in data]

    def get_processed_data(self):
        data = self.load_data()

        sort_column = self.params.get('sort')
        max_column = self.params.get('max')
        min_column = self.params.get('min')

        extreme_types = [Types.integer, Types.float, Types.date, Types.datetime]

        if max_column is not None:
            if self.columns[max_column]['type'] not in extreme_types:
                raise TypeError

            data = self.max_column_value(data, column=max_column)

        elif min_column is not None:
            if self.columns[min_column]['type'] not in extreme_types:
                raise TypeError

            data = self.min_column_value(data, column=min_column)

        elif sort_column is not None:
            data = self.sort_column(data, column=sort_column)

        else:
            data = self.data_to_json(data=data)

        return data

    def sort_column(self, data, column: str):
        column_data = self.columns[column]
        column_index = column_data['index']

        return self.data_to_json(data=sorted(
            data, key=lambda e: e[column_index],
            reverse=True
        ))

    def max_column_value(self, data, column: str):
        column_data = self.columns[column]
        column_index = column_data['index']
        max_element = max(data, key=lambda element: element[column_index])

        return {
            column: self.to_json(column_data, max_element[column_index]),
        }

    def min_column_value(self, data, column: str):
        column_data = self.columns[column]
        column_index = column_data['index']
        min_element = min(data, key=lambda element: element[column_index])

        return {
            column: self.to_json(column_data, min_element[column_index]),
        }
