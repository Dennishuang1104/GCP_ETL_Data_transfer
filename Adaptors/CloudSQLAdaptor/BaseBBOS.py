from datetime import date, timedelta
from os import path
import pandas as pd

import Environment
from Environment import cfg
from mysql.MySQL import DBParams, MySqlAdaptor, MySqlDBConnection
from util.dataframe_process import df_type_format


class BaseBBOS(MySqlAdaptor):
    def __init__(self, db_config: str):

        db_params = DBParams(
            host=cfg.get(db_config, 'HOST'),
            user=cfg.get(db_config, 'USER'),
            password=cfg.get(db_config, 'PASSWORD'),
            db=cfg.get(db_config, 'DB_NAME'),
            port=int(cfg.get(db_config, 'PORT'))
        )

        super(BaseBBOS, self).__init__(MySqlDBConnection(db_params))
        self.hall_id = 3820325
        self.hall_name = 'BBOS'
        self.code = ''
        self.domain_id = int(cfg.get(db_config, 'DOMAIN_ID'))
        self.domain_name = cfg.get(db_config, 'DOMAIN_NAME')
        self.base_bbos_db = 'ssr_bbos_dw'
        self.boss_threshold = int(cfg.get(db_config, 'BOSS_THRESHOLD'))
        self.cdp_schema = cfg.get(db_config, 'CDP_SCHEMA')
        self.data_date = date.today() - timedelta(days=1)  # yesterday
        self.bet_time_df = pd.DataFrame()

    def get_init_bet_time(self):  # 首次取得最後15日下注資料
        if path.exists(f'{Environment.ROOT_PATH}/files/{self.domain_name}_bet_time_15days.csv'):
            print('bet time file exist, ignore initial')
            return
        self.mode = self.QUERY_MODE
        try:
            self.statement = f'SELECT table_name FROM information_schema.tables ' \
                             f'WHERE table_schema = \'{self.base_bbos_db}\' ' \
                             f'AND table_name LIKE \'Wager_Vendor_%\' ' \
                             f'AND table_name <> \'Wager_Vendor_audit_log\' '
            self.exec()
            vendor_list = self.fetch_data
            sub_statement = ''
            for vendor in vendor_list:
                vendor_id = vendor[0].replace('Wager_Vendor_', '')

                sub_statement += f'SELECT {self.hall_id}, {self.domain_id}, bet.user_id, member.username, member.created_at, ' \
                                 f'HOUR(DATE_ADD(at, INTERVAL -12 HOUR)), ' \
                                 f'DATE(DATE_ADD(at, INTERVAL -12 HOUR)), ' \
                                 f'WEEKDAY(DATE_ADD(at, INTERVAL -12 HOUR)), ' \
                                 f'SUM(bet.valid_bet) ' \
                                 f'FROM {self.base_bbos_db}.Wager_Vendor_{vendor_id} bet ' \
                                 f'JOIN member_info member ON bet.user_id = member.user_id ' \
                                 f'WHERE bet.domain = {self.domain_id} ' \
                                 f'AND DATE(DATE_ADD(bet.at, INTERVAL -12 HOUR)) <= \'{self.data_date}\' ' \
                                 f'GROUP BY bet.user_id, HOUR(DATE_ADD(bet.at, INTERVAL -12 HOUR)), ' \
                                 f'DATE(DATE_ADD(bet.at, INTERVAL -12 HOUR)), ' \
                                 f'WEEKDAY(DATE_ADD(at, INTERVAL -12 HOUR)) ' \
                                 f'UNION ALL '
            self.statement = sub_statement
            self.statement = ''.join(self.statement.rsplit('UNION ALL', 1))  # replace last 'UNION'
            self.exec()
            sync_columns = ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date',
                            'bet_hour', 'data_date', 'data_weekday', 'commissionable']

            bet_df = pd.DataFrame(self.fetch_data, columns=sync_columns)

            bet_df = df_type_format(bet_df)

            activate_date_df = bet_df[
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'data_date']].drop_duplicates()

            latest_15_activate_date_df = activate_date_df.sort_values('data_date').groupby(
                ['hall_id', 'domain_id', 'user_id']).tail(15)
            top_activate_date_df = latest_15_activate_date_df.sort_values('data_date').groupby(
                ['hall_id', 'domain_id', 'user_id']).head(1).rename(columns={'data_date': 'top_activate_date'})
            activate_bet_df = pd.merge(left=bet_df, right=top_activate_date_df,
                                       on=['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'],
                                       how='left')
            result_df = activate_bet_df[
                activate_bet_df['data_date'] >= activate_bet_df['top_activate_date']].reset_index(drop=True)

            result_df.to_csv(f'{Environment.ROOT_PATH}/files/{self.domain_name}_bet_time_15days.csv', index=False)

        except Exception as e:
            raise

    def get_latest_bet_time(self):  # 將檔案concat最新一日下注資料，重新取得15天實動檔案
        try:
            self.bet_time_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/{self.domain_name}_bet_time_15days.csv')
            bet_time_df = df_type_format(self.bet_time_df)
            init_file_max_date = bet_time_df['data_date'].max()
            if init_file_max_date >= self.data_date:
                return
            self.mode = self.QUERY_MODE
            self.statement = f'SELECT table_name FROM information_schema.tables ' \
                             f'WHERE table_schema = \'{self.base_bbos_db}\' ' \
                             f'AND table_name LIKE \'Wager_Vendor_%\' ' \
                             f'AND table_name <> \'Wager_Vendor_audit_log\' '
            self.exec()
            vendor_list = self.fetch_data
            sub_statement = ''
            for vendor in vendor_list:
                vendor_id = vendor[0].replace('Wager_Vendor_', '')

                sub_statement += f'SELECT {self.hall_id}, {self.domain_id}, bet.user_id, member.username, member.created_at, ' \
                                 f'HOUR(DATE_ADD(at, INTERVAL -12 HOUR)), ' \
                                 f'DATE(DATE_ADD(at, INTERVAL -12 HOUR)), ' \
                                 f'WEEKDAY(DATE_ADD(at, INTERVAL -12 HOUR)), ' \
                                 f'SUM(bet.valid_bet) ' \
                                 f'FROM {self.base_bbos_db}.Wager_Vendor_{vendor_id} bet ' \
                                 f'JOIN member_info member ON bet.user_id = member.user_id ' \
                                 f'WHERE bet.domain = {self.domain_id} ' \
                                 f'AND DATE(DATE_ADD(at, INTERVAL -12 HOUR)) = \'{self.data_date}\' ' \
                                 f'GROUP BY bet.user_id, HOUR(DATE_ADD(bet.at, INTERVAL -12 HOUR)), ' \
                                 f'DATE(DATE_ADD(bet.at, INTERVAL -12 HOUR)), ' \
                                 f'WEEKDAY(DATE_ADD(at, INTERVAL -12 HOUR)) ' \
                                 f'UNION ALL '
            self.statement = sub_statement
            self.statement = ''.join(self.statement.rsplit('UNION ALL', 1))  # replace last 'UNION'
            self.exec()
            sync_columns = ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date',
                            'bet_hour', 'data_date', 'data_weekday', 'commissionable']

            bet_df = pd.DataFrame(self.fetch_data, columns=sync_columns)

            bet_df = df_type_format(bet_df)

            new_bet_time_df = pd.concat([bet_time_df, bet_df], ignore_index=True).drop(columns=['top_activate_date'])

            activate_date_df = new_bet_time_df[
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'data_date']].drop_duplicates()

            latest_15_activate_date_df = activate_date_df.sort_values('data_date').groupby(
                ['hall_id', 'domain_id', 'user_id']).tail(15)
            top_activate_date_df = latest_15_activate_date_df.sort_values('data_date').groupby(
                ['hall_id', 'domain_id', 'user_id']).head(1).rename(columns={'data_date': 'top_activate_date'})
            activate_bet_df = pd.merge(left=new_bet_time_df, right=top_activate_date_df,
                                       on=['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'],
                                       how='left')
            activate_bet_df = activate_bet_df[
                activate_bet_df['data_date'] >= activate_bet_df['top_activate_date']].reset_index(drop=True)
            activate_bet_df.to_csv(f'{Environment.ROOT_PATH}/files/{self.domain_name}_bet_time_15days.csv', index=False)
            self.bet_time_df = activate_bet_df
            """
            # 計算每位使用者 實動天數 至少超過3天(包含3天)
            data_df = activate_bet_df[['hall_id', 'domain_id', 'user_id', 'data_date']].drop_duplicates()
            filter_bet_df = \
                data_df[data_df.groupby(by=['hall_id', 'domain_id', 'user_id'])['hall_id'].transform('size') >= 3][
                    ['hall_id', 'domain_id', 'user_id']].drop_duplicates()
            self.target_bet_time_df = activate_bet_df[activate_bet_df['user_id'].isin(filter_bet_df['user_id'])]
            """
        except Exception as e:
            raise
