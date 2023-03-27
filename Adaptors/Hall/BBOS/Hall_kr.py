from Adaptors.Hall.BBOS import Hall


class Hall_kr(Hall):
    def __init__(self):
        super(Hall_kr, self).__init__()
        self.db_name = 'cdp'
        self.hall_id = 3820325
        self.hall_name = 'bbos'
        self.domain_id = 82
        self.domain_name = 'kr'
        self.currency = 'KRW'
        self.boss_tag_threshold = 10000000
        self.ga_resource_id = 208415210
        self.ga_firebase_id = 0
        self.code = 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJkb21haW4iOiI4MiJ9.RtTmH_pKOQSbu1yYkrjQ07acDpliaQXO9JvUVNy-ufHHjIEkrqIy0npFPEfi50opboFUsm9Vsaab4g9RkmisjomLWqkDsWweA3hcHt2w-n5X7J_FzR50pZj6GHzM_O7Wki7_OrAQZM2OdHdns4mUKiO2nrQfu1vYj0-AEWkckezfkiEjuMGR5QYscmmTCz_ALgNmLQz1HAIJeMBclhcWq-D5pbRbtWRQAyHRdpbjhKgWsfkFUi6_ok5ivzK631oEAFgj-6iBcXcxlwc9V5wToN4vzfRMuBjDndjv5vQVSfhQzNUIgGu6TGpEXjZk_6qzWcTCTg5MjLzEW6VHNQN-3A'
        self.token = 'd3815a8563658d187419710e80d400dd27aae8610'
        self.client_id = 'c0a6d891e372c0b9d5801df9a0253c373b1a864f'
        self.client_secret = 'ddeb08543bdda72d1e27ec5e073e933d04c270a5'
