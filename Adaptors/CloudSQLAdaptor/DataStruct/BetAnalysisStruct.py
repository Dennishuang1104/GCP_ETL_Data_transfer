from . import BaseStruct


class BetAnalysisStruct(BaseStruct):
    def __init__(self, user_id, lobby, kind, game_code, bet, payoff, valid_bet, currency, commision, device, data_date,
                 financial_year, financial_month, financial_week):
        super(BetAnalysisStruct, self).__init__()

        self._user_id = user_id
        self._lobby = lobby
        self._kind = kind
        self._game_code = game_code
        self._bet = bet
        self._payoff = payoff
        self._valid_bet = valid_bet
        self._currency = currency
        self._commision = commision
        self._device = device
        self._data_date = data_date
        self._financial_year = financial_year
        self._financial_month = financial_month
        self._financial_week = financial_week

    @property
    def user_id(self):
        return self._user_id

    @property
    def lobby(self):
        return self._lobby

    @property
    def kind(self):
        return self._kind

    @property
    def game_code(self):
        return self._game_code

    @property
    def bet(self):
        return self._bet

    @property
    def payoff(self):
        return self._payoff

    @property
    def valid_bet(self):
        return self._valid_bet

    @property
    def currency(self):
        return self._currency

    @property
    def commision(self):
        return self._commision

    @property
    def device(self):
        return self._device

    @property
    def data_date(self):
        return self._data_date

    @property
    def financial_year(self):
        return self._financial_year

    @property
    def financial_month(self):
        return self._financial_month

    @property
    def financial_week(self):
        return self._financial_week

    # def columns(self):
    #     return ['promotion_id', 'hall_id', 'domain_id', 'user_id', 'coupon', 'verify']
    #
    # def data(self):
    #     return tuple([self.promotion_id, self.hall_id, self.domain_id, self.user_id, self.coupon, self.verify])
