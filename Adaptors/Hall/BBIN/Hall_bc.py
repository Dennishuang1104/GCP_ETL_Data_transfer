from Adaptors.Hall import Hall


class Hall_bc(Hall):
    def __init__(self):
        super(Hall_bc, self).__init__()
        self.db_name = 'cdp_bc'
        self.demo_db_name = 'cdp_demo'
        self.hall_id = 3817629
        self.hall_name = 'bc'
        self.currency = 'RMB'
        self.boss_tag_threshold = 100000
        self.ga_resource_id = 187889297
        self.ga_firebase_id = 0
