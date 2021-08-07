from ..sql_exceptions.my_exceptions import EmptyFieldError


class Nameconfig:
    def __init__(self, filename, patch):
        self.patch = patch
        self.filename = filename

    def __getattribute__(self, attr):
        if attr == 'file_name':
            if not self.filename:
                raise EmptyFieldError()
            flip = self.patch not in self.filename
            return (self.filename + self.patch) if flip else self.filename
        else:
            return object.__getattribute__(self, attr)
