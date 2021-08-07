

class ConnectionFieldError(Exception):

    def __init__(self, *args):
        Exception.__init__(self)
        self.data = args

    def __str__(self):
        tag = 'supplied fields %s, error fields %s, required fields %s'
        tag = tag % self.data
        return tag


class DbConnectionError(Exception):

    def __init__(self, error):
        Exception.__init__(self)
        self.error = error

    def __str__(self):
        tag = 'error summary: {}'
        return tag.format(self.error)


class FieldError(Exception):

    def __init__(self, *args):
        Exception.__init__(self)
        self.data = args

    def __str__(self):
        tag = 'error field %s, allowed fields %s' % self.data
        return tag


class EmptyFieldError(Exception):

    def __init__(self, arg):
        Exception.__init__(self)
        self.data = arg

    def __str__(self):
        return f'Hello user {self.data} cannot be empty, \
        please give {self.data} field and argument !!!'


class FieldLengthError(Exception):

    def __init__(self, *args):
        Exception.__init__(self)
        self.supplied = args[0]
        self.needed = args[1]

    def __str__(self):
        return f'Expected {self.needed} arguments , \
        you supplied {self.supplied}'


class NoFileError(Exception):

    def __init__(self,):
        Exception.__init__(self)

    def __str__(self):

        return f'you gave me no query to process, \n please i need \
            a you to give me a query as a string or a file to read \
            your query from'


class WrongFileError(Exception):

    def __init__(self, *args):
        Exception.__init__(self)
        self.wrong_format = args[0]
        self.right_format = args[1]

    def __str__(self):

        return f' you gave me a {self.wrong_format}, i need a {self.right_format} file'


class MissingKeywordArgumentError(FieldLengthError):

    def __init__(self, *args):
        FieldLengthError.__init__(self, *args)
        self.function = args[0]
        self.keywordarg = args[1]

    def __str__(self):
        FieldLengthError.__str__(self)
        return f" ERROR!!!, {self.function.__name__} function: is missing the \
            following required arguments {self.keywordarg} "
