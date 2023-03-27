from telegram.ext import Updater


class BaseTelegramBot:
    def __init__(self):
        self.token: str = '1047796666:AAHg9kEWCCaOxCoI195PnR2CsQGU5bdDNcg'  # GHR-BOT
        self.updater: Updater = Updater(self.token, use_context=True)
        self.message: str = ''
        self.target_chat_id: str = ''

    def send_message(self):
        self.updater.bot.send_message(self.target_chat_id, self.message)
