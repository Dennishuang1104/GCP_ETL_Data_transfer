import time

from Tools.APICallerTools import APICallerTools
from util.Hall_dict import hall_dict
from Adaptors.SpannerAdaptor import CDP_BBOS
from Adaptors.TelegramAdaptor import Notify
from Adaptors.Hall import Hall
import pandas as pd
#####
import base64
from threading import Thread

notify_bot = Notify()


class Call_BBOS_API(CDP_BBOS):
    def __init__(self, spanner_db: str, bq_db):
        super(Call_BBOS_API, self).__init__(database=spanner_db)
        self.user_relative_df_column = None
        self.token = ''
        self.code = ''
        self.client_id = ''
        self.client_secret = ''
        self.domain_name = ''
        self.bq_db = bq_db
        self.api_url = 'https://api.bbin-fun.com/api/v1/oauth2/token'
        self.lobby_dict ={}
        self.user_relative_df_columns = ['hall_id', 'domain_id', 'user_id', 'external_lobby', 'external_lobby_name', 'external_user_name', 'external_currency']
        self.user_relative_df = pd.DataFrame(columns=self.user_relative_df_columns)

    def create_authorization(self, client_id, client_secret):
        return "Basic " + base64.b64encode((client_id + ":" + client_secret).encode("UTF-8")).decode('UTF-8')

    def get_service_token(self, hall):
        bbos_hall = hall_dict[hall]()
        # 先取得service token
        myAPI = APICallerTools()
        self.token = bbos_hall.token
        self.code = bbos_hall.code
        self.client_id = bbos_hall.client_id
        self.client_secret = bbos_hall.client_secret
        myAPI.set_url("https://api.bbin-fun.com/api/v1/oauth2/token")
        myAPI.insert_header("token", self.token)
        myAPI.insert_header("Authorization", self.create_authorization(self.client_id, self.client_secret))
        myAPI.insert_param("grant_type", "service_token")
        myAPI.insert_param("code", self.code)
        myAPI.insert_param("client_id", self.client_id)
        getAccessToken = myAPI.call_api("POST", "json")
        return getAccessToken

    def get_user_relative_data(self, user_id: str, user_name: str):
        # 先取得service token
        get_access_token = self.get_service_token(self.domain_name)
        myAPI = APICallerTools()
        myAPI.reset()
        myAPI.set_url(f'https://api.bbin-fun.com/api/v1/e/external/relative_name')
        myAPI.insert_param("username", user_name)
        myAPI.insert_header("access-token", get_access_token['access_token'])  # set api header
        myAPI.insert_header("token", self.token)
        resp_content = myAPI.call_api("GET", "json")  # get return data by json

        ###
        if 'code' in resp_content:
            print('API Error!')

        if resp_content['result'] == 'ok' and len(resp_content['ret']) > 0:
            hall_id = 3820325
            domain_id = int(resp_content['ret'][0]['domain'])
            # print(resp_content['ret'][0]['external'])
            for result in resp_content['ret'][0]['external']:
                # target_relative_external = resp_content['ret'][0]['external'][game_code]
                external_lobby = int(result['game_code'])
                external_lobby_name = self.lobby_dict[external_lobby]
                external_user_name = result['name']
                external_currency = '' if result.get('currency') is None else \
                    result['currency']
                # method2
                new_row = pd.DataFrame({'hall_id' : hall_id,
                                        'domain_id' : domain_id,
                                        'user_id' : user_id,
                                        'external_lobby' : external_lobby,
                                        'external_lobby_name' : external_lobby_name,
                                        'external_user_name' : external_user_name,
                                        'external_currency': external_currency}, index=[0])
                self.user_relative_df = pd.concat([self.user_relative_df, new_row])

                # method1
                # self.user_relative_df_columns = ['hall_id', 'domain_id', 'user_id', 'external_lobby',
                #                                  'external_lobby_name', 'external_user_name', 'external_currency']
                # self.user_relative_df.loc[len(self.user_relative_df)] = [hall_id, domain_id, user_id, external_lobby,
                #                                                          external_lobby_name,
                #                                                          external_user_name, external_currency]
        self.replace_data_with('member_relative_info', self.user_relative_df)

    def get_member_data(self, hall: Hall, start, end):
        self.domain_name = hall.domain_name
        self.bq_db.statement = (
            f"""
        select 
          master.user_id, username as user_name
        from `{self.bq_db.project_id}.ssr_dw_{self.domain_name}.User_View` master
        LEFT JOIN `{self.bq_db.project_id}.ssr_dw_{self.domain_name}.User_Created_View` created ON master.user_id = created.user_id
        where role = 1 
              AND (DATE(created.`at`) BETWEEN '{start}' AND '{end}' 
              OR DATE(master.created_time) BETWEEN '{start}' AND '{end}')
        """)
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data
        insert_df['user_id'] = insert_df['user_id'].astype(int)
        insert_df['user_name'] = insert_df['user_name'].astype(str)

        # 取lobby_name
        self.bq_db.statement = (
            f'''SELECT lobby, lobby_name FROM `{self.bq_db.project_id}.general_information.lobby_dict` '''
        )
        self.bq_db.exec()
        df_ = self.bq_db.fetch_data
        df_['lobby'] = df_['lobby'].astype(int)
        df_['lobby_name'] = df_['lobby_name']
        self.lobby_dict = dict(zip(df_.lobby, df_.lobby_name))

        # Call api
        threads =[]
        for cnt, row in insert_df.iterrows():
            user_id = row['user_id']
            user_name = row['user_name']
            # self.get_user_relative_data(user_id, user_name)
            # MultiThread
            threads.append(Thread(target=self.get_user_relative_data, args=(user_id, user_name)))
        for t in threads:
            t.start()
            time.sleep(0.5)
            t.join()
            # t.run()





