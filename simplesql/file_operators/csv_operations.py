from ..sql_decorators.singleton_decorator import singleton
from .namefile import Nameconfig
from ..sql_exceptions.my_exceptions import EmptyFieldError, FieldLengthError
import csv


@singleton
class Csv_operations(Nameconfig):
    closed = False

    def __init__(self, file_name, patch='.csv'):
        Nameconfig.__init__(self, file_name, patch)

    def read(self, *args, **kwargs):
        with open(self.file_name, 'r') as csvFile:
            dl = kwargs.get('dict_val')
            data = csv.reader(csvFile) if not dl else csv.DictReader(csvFile)
            read_data = [row for row in data]
            return read_data
        csvFile.close()

    def write(self, *args, **kwargs):
        data = args[0]
        if kwargs.get('file_name') and data and len(args) == 1:
            self.file_name = kwargs.get('file_name')
        else:
            if not kwargs.get('file_name') or data:
                raise EmptyFieldError('data' if not data else 'file_name')
            else:
                raise FieldLengthError(len(args), 1)
        new_file = kwargs.get('new_file')
        with open(self.file_name, 'w' if not new_file else 'a') as writeFile:
            writer = csv.writer(writeFile)
            if kwargs['dictionary_option']:
                fields = [membr for membr in data[0]]
                writer = csv.DictWriter(writeFile, fieldnames=fields)
                writer.writeheader()
                writer.writerows(data)

            else:
                rows = kwargs.get('many_rows')
                writer.writerow(data) if not rows else writer.writerows(data)
        writeFile.close()
