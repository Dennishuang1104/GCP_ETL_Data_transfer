from Adaptors.Hall.BBOS import Hall


class Hall_178t(Hall):
    def __init__(self):
        super(Hall_178t, self).__init__()
        self.db_name = 'cdp_bbos_demo'
        self.hall_name = 'bbos'
        self.hall_id = 3820325
        self.domain_id = 72
        self.domain_name = '178t'
        self.currency = 'RMB'  # 待釐清
        self.boss_tag_threshold = 100000
        self.ga_resource_id = 208415210
        self.ga_firebase_id = 0
