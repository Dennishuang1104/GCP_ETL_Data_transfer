from Adaptors.Hall.BBOS import Hall


class Hall_241(Hall):
    def __init__(self):
        super(Hall_241, self).__init__()
        self.db_name = 'cdp_bbos_test'
        self.hall_id = 3820325
        self.hall_name = 'bbos'
        self.domain_id = 241
        self.domain_name = '241'
        self.currency = 'VND'
        self.boss_tag_threshold = 10000000
        # self.ga_resource_id = 264491371   # 待確認
        # self.ga_firebase_id = 0
