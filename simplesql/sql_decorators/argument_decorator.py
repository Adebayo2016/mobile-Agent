from ..sql_exceptions.my_exceptions import MissingKeywordArgumentError
import functools


def argumentPresence(failIf):
    class ArgDecorator (object):
        """ this decorator is used to time how long
            it takes for a function to run """

        def __init__(self, func):
            self.attributes = failIf[1]
            self.attribute_check = failIf[0]
            self.func = func  # function to be timed

        def __call__(self, *args, **kwargs):
            """ this is called when used to
                decorate an ordinary function """
            trace = [1 for i in list(kwargs.keys()) if kwargs.get(i) and self.attribute_check(i)]
            trace = sum(trace)
            missing_args = [i for i in self.attributes if i in list(kwargs.keys())]
            if trace != len(self.attributes):
                raise MissingKeywordArgumentError(self.func, missing_args)
            print(self.func.__name__)
            result = self.func(*args, **kwargs)
            return result

        def __get__(self, instance, owner):
            """ this is called when used to decorate a
                methods attached to a class """
            def wrapper(*args, **kwargs):
                return self(instance, *args, **kwargs)
            return wrapper
    return ArgDecorator


def argumentPresenceV2(failIf):
    def ArgDecorator(func):
        """ this decorator is used to time how long
            it takes for a function to run """

        @functools.wraps(func)
        def processWrapper(*args, **kwargs):
            attr_chk = processWrapper.attribute_check
            trace = sum([1 for i in list(kwargs.keys()) if kwargs.get(i) and attr_chk(i)])
            missing_args = [i for i in processWrapper.attributes if i in list(kwargs.keys())]
            if trace != len(processWrapper.attributes):
                raise MissingKeywordArgumentError(func, missing_args)
            print(func.__name__)
            result = func(*args, **kwargs)
            return result

        processWrapper.attributes = failIf[1]
        processWrapper.attribute_check = failIf[0]
        return processWrapper
    return ArgDecorator


def argumentPresenceV3(failIf):
    def ArgDecorator(func):
        """ this decorator is used to time how long
            it takes for a function to run """
        @functools.wraps(func)
        def processWrapper(*args, **kwargs):
            supplied_keywords = tuple(kwargs.keys())
            atleast_one = 0
            for att in processWrapper.attributes:
                check = set(supplied_keywords).intersection(set(att))
                if len(check) == len(att):
                    atleast_one += 1
            if (atleast_one < 1) and (not processWrapper.attribute_check(supplied_keywords)):
                raise MissingKeywordArgumentError(func, processWrapper.attributes)
            print(func.__name__)
            result = func(*args, **kwargs)
            return result

        processWrapper.attributes = failIf[1]
        processWrapper.attribute_check = failIf[0]
        return processWrapper
    return ArgDecorator


def Required(*attributes):
    return argumentPresence(failIf=(lambda attr: attr in attributes, attributes))


def Optional(*attributes):
    return argumentPresence(failIf=(lambda attr: attr not in attributes, attributes))


def RequiredV2(*attributes):
    return argumentPresenceV2(failIf=(lambda attr: attr in attributes, attributes))


def OptionalV2(*attributes):
    return argumentPresenceV2(failIf=(lambda attr: attr not in attributes, attributes))


def RequiredV3(*attributes):
    return argumentPresenceV3(failIf=(lambda attr: attr in attributes, attributes))


def OptionalV3(*attributes):
    return argumentPresenceV3(failIf=(lambda attr: attr not in attributes, attributes))
