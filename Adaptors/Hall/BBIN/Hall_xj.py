from Adaptors.Hall import Hall


class Hall_xj(Hall):
    def __init__(self):
        super(Hall_xj, self).__init__()
        self.db_name = 'cdp_1'
        self.demo_db_name = 'cdp_demo'
        self.hall_id = 3820420
        self.hall_name = 'xj'
        self.currency = 'RMB'
        self.boss_tag_threshold = 100000
        self.ga_resource_id = 246509261
        self.ga_firebase_id = 0
