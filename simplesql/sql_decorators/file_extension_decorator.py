from ..sql_exceptions.my_exceptions import WrongFileError
import functools


def extension_decorator(function):

    @functools.wraps(function)
    def proxy(*args, **kwargs):
        if kwargs.get('querry_file'):
            querry_file = kwargs.get('querry_file')
            file_ending = querry_file.split('.')[-1]
            if file_ending != 'txt':
                raise WrongFileError('.'+file_ending, '.txt')

        elif kwargs.get('data_file'):
            querry_file = kwargs.get('querry_file')
            file_ending = querry_file.split('.')[-1]
            if file_ending != 'json':
                raise WrongFileError('.'+file_ending, '.json')

        return function(*args, **kwargs)
    return proxy
