from Adaptors.Hall import Hall


class Hall_b9(Hall):
    def __init__(self):
        super(Hall_b9, self).__init__()
        self.db_name = 'cdp_b9'
        self.demo_db_name = 'cdp_demo'
        self.hall_id = 98
        self.hall_name = 'b9'
        self.currency = 'RMB'
        self.boss_tag_threshold = 100000
        self.ga_resource_id = 206775505
        self.ga_firebase_id = 0
