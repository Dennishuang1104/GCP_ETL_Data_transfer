class BaseStruct:
    def __init__(self):
        pass

    def data(self):
        return tuple([self.__dict__[x] for x in self.__dict__.keys()])

    def columns(self):
        return [k[1:] for k in self.__dict__.keys()]

    def show(self):
        return dict(self.__dict__)

    def value_of(self, field):
        return self.__dict__[f'_{field}']
