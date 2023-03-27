from Adaptors.Hall.BBOS import Hall


class Hall_mat(Hall):
    def __init__(self):
        super(Hall_mat, self).__init__()
        self.db_name = 'cdp_bbos_test'
        self.hall_id = 3820325
        self.hall_name = 'bbos'
        self.domain_id = 9999907
        self.domain_name = 'mat'
        self.currency = 'RMB'
        self.boss_tag_threshold = 100000
        # self.ga_resource_id = 264491371   # 待確認
        # self.ga_firebase_id = 0
