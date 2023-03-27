from Adaptors.Hall import Hall


class Hall_710(Hall):
    def __init__(self):
        super(Hall_710, self).__init__()
        self.db_name = 'cdp_710'
        self.demo_db_name = 'cdp_demo'
        self.hall_id = 3819496
        self.hall_name = '710'
        self.currency = 'RMB'
        self.boss_tag_threshold = 100000
        self.ga_resource_id = 187889297
        self.ga_firebase_id = 0
