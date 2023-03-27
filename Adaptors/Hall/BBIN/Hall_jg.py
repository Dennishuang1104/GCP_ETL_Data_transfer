from Adaptors.Hall import Hall


class Hall_jg(Hall):
    def __init__(self):
        super(Hall_jg, self).__init__()
        self.db_name = 'cdp_jg'
        self.demo_db_name = 'cdp_demo'
        self.hall_id = 281
        self.hall_name = 'jg'
        self.currency = 'RMB'
        self.boss_tag_threshold = 100000
        self.ga_resource_id = 246509261
        self.ga_firebase_id = 0
