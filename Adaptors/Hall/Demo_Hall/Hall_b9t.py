from Adaptors.Hall.BBOS import Hall


class Hall_b9t(Hall):
    def __init__(self):
        super(Hall_b9t, self).__init__()
        self.db_name = 'cdp_bbin_demo'
        self.hall_name = 'b9'
        self.hall_id = 98
        self.hall_id = 3820325
        self.domain_name = '178t'
        self.currency = 'RMB'
        self.boss_tag_threshold = 100000
        self.ga_resource_id = 206775505
        self.ga_firebase_id = 0