import pickle
import shelve
from ..sql_decorators.singleton_decorator import singleton
from .namefile import Nameconfig
from ..sql_exceptions.my_exceptions import EmptyFieldError, FieldLengthError


@singleton
class Pickle_operations(Nameconfig):
    closed = False

    def __init__(self, file_name, patch='.pickle'):
        Nameconfig.__init__(self, file_name, patch)

    def write(self, *args, **kwargs):
        data = args[0]
        if kwargs.get('file_name') and data and len(args) == 1:
            self.file_name = kwargs.get('file_name')
        else:
            if not kwargs.get('file_name')or data:
                raise EmptyFieldError('data' if not data else 'file_name')
            else:
                raise FieldLengthError(len(args), 1)

        with open(self.file_name, 'wb') as picklefile:
            pickle.dump(data, picklefile)
        picklefile.close()
        print(' Data saved ')

    def read(self):
        picklefile = open(self.file_name, 'rb')
        data = pickle.load(picklefile)
        picklefile.close()
        return data


@singleton
class Shelve_operations(Nameconfig):
    closed = False

    def __init__(self, file_name, patch=''):
        Nameconfig.__init__(self, file_name, patch)

    def write(self, *args, **kwargs):
        data = args[0]
        if kwargs.get('file_name') and data and len(args) == 1:
            self.file_name = kwargs.get('file_name')
        else:
            if not kwargs.get('file_name') or data:
                raise EmptyFieldError('data' if not data else 'file_name')
            else:
                raise FieldLengthError(len(args), 1)

        db = shelve.open(self.file_name)  # Filename where objects are stored
        for key in data:  # Use object's name attr as key
            db[key] = data[key]  # Store object on shelve by key
        db.close()

    def read(self):
        db = shelve.open(self.filename)
        return db
