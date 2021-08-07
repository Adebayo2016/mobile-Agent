import functools


def column_decorator(function):
    @functools.wraps(function)
    def proxy(*args, **kwargs):
        if kwargs.get('col_names'):
            if len(kwargs['col_names']) > 1:
                data = str(tuple(kwargs.get('col_names')))
                data = data.replace("'", '').replace('(', '').replace(')', '')
                kwargs['col_names'] = data
            else:
                data = str(tuple(kwargs.get('col_names')))
                data = data.replace("'", '').replace('(', '')
                data = data.replace(')', '').replace(',', '')
                kwargs['col_names'] = data
        else:
            kwargs.setdefault('col_names', '*')
        return function(*args, **kwargs)
    return proxy
