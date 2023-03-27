from DataStruct import BaseStruct


class TokenReqStruct(BaseStruct):
    def __init__(self):
        super(TokenReqStruct).__init__()
        self.grant_type: str = ''
        self.code: str = ''
        self.client_id: str = ''
