class BaseStruct:
    def __init__(self):
        pass

    def data(self, target_columns: list = None):
        return tuple([self.__dict__[x] for x in self.__dict__.keys() if (target_columns is None or x[1:] in target_columns)])

    def columns(self, target_columns: list = None):
        return [k[1:] for k in self.__dict__.keys() if (target_columns is None or k[1:] in target_columns)]

    def show(self):
        return dict(self.__dict__)

    def value_of(self, field):
        return self.__dict__[f'_{field}']
