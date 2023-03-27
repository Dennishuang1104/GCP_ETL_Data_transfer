from Adaptors.TelegramAdaptor import BaseTelegramBot


class Notify(BaseTelegramBot):
    def __init__(self):
        super().__init__()
        # self.target_chat_id: str = '-604828140'
        self.target_chat_id: str = '-741670908'   # Test (Dennis)
