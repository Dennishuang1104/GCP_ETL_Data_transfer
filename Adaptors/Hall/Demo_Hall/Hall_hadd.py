from Adaptors.Hall.BBOS import Hall


class Hall_hadd(Hall):
    def __init__(self):
        super(Hall_hadd, self).__init__()
        self.db_name = 'cdp_bbos_demo'
        self.hall_name = 'bbos'
        self.domain_id = 103
        self.hall_id = 3820325
        self.domain_name = 'hadd'
        self.currency = 'PHP'
        self.boss_tag_threshold = 15000
        self.ga_resource_id = 208415210
        self.ga_firebase_id = 0