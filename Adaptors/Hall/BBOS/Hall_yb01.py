from Adaptors.Hall.BBOS import Hall


class Hall_yb01(Hall):
    def __init__(self):
        super(Hall_yb01, self).__init__()
        self.db_name = 'cdp'
        self.hall_id = 3820325
        self.hall_name = 'bbos'
        self.domain_id = 67
        self.domain_name = 'yb01'
        self.currency = 'RMB'
        self.boss_tag_threshold = 100000
        self.ga_resource_id = 208413528
        self.ga_firebase_id = 224745072
