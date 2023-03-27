from Adaptors.Hall import Hall


class Hall_hh(Hall):
    def __init__(self):
        super(Hall_hh, self).__init__()
        self.db_name = 'cdp_hh'
        self.demo_db_name = 'cdp_demo'
        self.hall_id = 3819497
        self.hall_name = 'hh'
        self.currency = 'RMB'
        self.boss_tag_threshold = 100000
        self.ga_resource_id = 187889297
        self.ga_firebase_id = 0
