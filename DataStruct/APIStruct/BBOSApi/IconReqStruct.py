from DataStruct import BaseStruct


class IconReqStruct(BaseStruct):
    def __init__(self):
        super(IconReqStruct).__init__()
        self.role: int = 0
