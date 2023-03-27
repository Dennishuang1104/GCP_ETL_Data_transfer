from Adaptors.Hall import Hall


class Hall_esb(Hall):
    def __init__(self):
        super(Hall_esb, self).__init__()
        self.db_name = 'cdp_esb'
        self.demo_db_name = 'cdp_demo'
        self.hall_id = 6
        self.hall_name = 'esb'
        self.currency = 'RMB'
        self.boss_tag_threshold = 100000
        self.ga_resource_id = 187889297
        self.ga_firebase_id = 0
