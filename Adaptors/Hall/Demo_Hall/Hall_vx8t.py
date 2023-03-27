from Adaptors.Hall.BBOS import Hall


class Hall_vx8t(Hall):
    def __init__(self):
        super(Hall_vx8t, self).__init__()
        self.db_name = 'cdp_bbos_demo'
        self.hall_name = 'bbos'
        self.hall_id = 3820325
        self.domain_id = 73
        self.domain_name = 'vx8t'
        self.currency = 'VND'              # 待釐清
        self.boss_tag_threshold = 10000000
        self.ga_resource_id = 208383666
        self.ga_firebase_id = 0