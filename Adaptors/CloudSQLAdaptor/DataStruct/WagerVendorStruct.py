from . import BaseStruct


class WagerVendorStruct(BaseStruct):
    def __init__(self, domain, at_begin, at_end, user_id, vendor, kind, game_code, bet, payoff, valid_bet,
                 currency, commision, device, modify_at_begin, modify_at_end):
        super(WagerVendorStruct, self).__init__()

        self._domain = domain
        self._user_id = user_id
        self._at_begin = at_begin
        self._at_end = at_end
        self._vendor = vendor
        self._kind = kind
        self._game_code = game_code
        self._bet = bet
        self._payoff = payoff
        self._valid_bet = valid_bet
        self._currency = currency
        self._commision = commision
        self._device = device
        self._modify_at_begin = modify_at_begin
        self._modify_at_end = modify_at_end

    @property
    def domain(self):
        return self._domain

    @property
    def at_begin(self):
        return self._at_begin

    @property
    def at_end(self):
        return self._at_end

    @property
    def user_id(self):
        return self._user_id

    @property
    def vendor(self):
        return self._vendor

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
    def modify_at_begin(self):
        return self._modify_at_begin

    @property
    def modify_at_end(self):
        return self._modify_at_end
