import json
import urllib.request as rq
from lxml import html
import os
from ..sql_decorators.singleton_decorator import singleton
from .namefile import Nameconfig
from ..sql_exceptions.my_exceptions import EmptyFieldError, FieldLengthError


@singleton
class Json_operations(Nameconfig):
    closed = False

    def __init__(self, file_name, patch='.json'):
        Nameconfig.__init__(self, file_name, patch)

    def read(self, display=0):
        with open(self.file_name) as jsonfile:
            data = json.load(jsonfile)
        jsonfile.close()
        for i in data:
            print(data[i])
        return None if display else data

    def write(self, *args, **kwargs):
        data = args[0]
        if kwargs.get('file_name') and data and len(args) == 1:
            self.file_name = kwargs.get('file_name')
        else:
            if not kwargs.get('file_name') or data:
                raise EmptyFieldError('data' if not data else 'file_name')
            else:
                raise FieldLengthError(len(args), 1)

        with open(self.file_name, 'w') as jsonfile:
            json.dump(data, jsonfile)
        jsonfile.close()

    def append(self, *args, **kwargs):
        data = args[0]
        key = args[1]
        if kwargs.get('file_name') and data and key and len(args) == 2:
            self.file_name = kwargs.get('file_name')
        else:
            if not (kwargs.get('file_name') or data or key):
                raise EmptyFieldError('data' if not data else 'file_name')
            else:
                raise FieldLengthError(len(args), 2)
        json_data = self.read(self.file_name)
        json_data.setdefault(key, data)
        self.write(self.file_name, json_data)


def html_file_read(file_name, folder='', option=1):
    path = os.path.dirname(os.path.realpath(__file__))
    filepath = path + folder + '\\' + file_name
    filepath = filepath.replace(':', '|')
    file = 'file:\\\\'
    file += filepath
    if option:
        filedata = str(rq.urlopen(file).read())
    else:
        html_byte = html.parse(file_name)
        filedata = str(html.tostring(html_byte))
    return filedata


def html_online_read(url):
    filedata = str(rq.urlopen(url).read())
    return filedata
