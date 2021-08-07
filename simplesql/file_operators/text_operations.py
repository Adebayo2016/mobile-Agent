from ..sql_decorators.singleton_decorator import singleton
from .namefile import Nameconfig
from ..sql_exceptions.my_exceptions import EmptyFieldError, FieldLengthError


@singleton
class Text_operations(Nameconfig):
    closed = False

    def __init__(self, file_name, patch='.txt'):
        Nameconfig.__init__(self, file_name, patch)

    def read(self):
        with open(self.file_name, 'r') as txtreader:
            text = ''
            for line in txtreader.readlines():
                line = line.replace('\n', ' ')
                text += line
            return text
        self.closed = True
        txtreader.close()
        print('-- closed from db', self.closed)

    def write(self, *args, **kwargs):
        data = args[0]
        if kwargs.get('file_name') and data and len(args) == 1:
            self.file_name = kwargs.get('file_name')
        else:
            if not kwargs.get('file_name')or data:
                raise EmptyFieldError('data' if not data else 'file_name')
            else:
                raise FieldLengthError(len(args), 1)

        with open(self.file_name, 'w') as writer:
            for row in reversed(data):
                writer.write(row)
        writer.close()

    def __del__(self):
        print('Destructor called, Text_operations deleted.')
