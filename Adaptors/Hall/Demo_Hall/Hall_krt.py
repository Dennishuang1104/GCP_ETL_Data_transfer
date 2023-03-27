from Adaptors.Hall.BBOS import Hall


class Hall_krt(Hall):
    def __init__(self):
        super(Hall_krt, self).__init__()
        self.db_name = 'cdp_bbos_demo'
        self.hall_name = 'bbos'
        self.hall_id = 3820325
        self.domain_id = 83
        self.domain_name = 'krt'
        self.currency = 'KRW'  # 待釐清
        self.boss_tag_threshold = 10000000
        self.ga_resource_id = 208415210
        self.ga_firebase_id = 0