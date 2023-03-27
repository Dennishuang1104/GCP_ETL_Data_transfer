from datetime import date, timedelta
from os import path
import pandas as pd

import Environment
from Adaptors.BQAdaptor.BQ_SSR import BQ_SSR
from Adaptors.Hall import Hall
from util.dataframe_process import df_type_format_tag, df_timezone_format


class BQ_BaseBBIN(BQ_SSR):
    def __init__(self, hall: Hall):
        super().__init__()
        self.hall_id = hall.hall_id
        self.hall_name = hall.hall_name
        self.domain_id = hall.domain_id
        self.domain_name = hall.domain_name
        self.boss_threshold = hall.boss_tag_threshold
        self.data_date = date.today() - timedelta(days=1)  # yesterday
        self.bet_time_df = pd.DataFrame()
        self.cash_df = pd.DataFrame()
        self.opcode_df = pd.DataFrame()

    def get_init_bet_time(self):  # 首次取得最後15日下注資料
        if path.exists(f'{Environment.ROOT_PATH}/files/spanner/{self.hall_name}_bet_time_15days.csv'):
            print('bet time file exist, ignore initial')
            return
        self.mode = self.QUERY_MODE
        try:
            self.statement = (
                f"""
                SELECT {self.hall_id} AS hall_id, {self.domain_id} AS domain_id, bet.user_id, 
                  member.username AS user_name,
                  DATE_ADD(member.created_at, INTERVAL 12 HOUR) AS register_date, 
                  bet.bet_hour, bet.data_date, bet.data_weekday,
                  SUM(bet.commissionable) AS commissionable
                FROM (
                  SELECT 
                    {self.hall_id} AS hall_id, user_id,
                    EXTRACT(HOUR FROM `at`) AS bet_hour,
                    DATE(`at`) AS data_date,
                    EXTRACT(DAYOFWEEK FROM `at`) AS data_weekday,
                    valid_bet AS commissionable
                  FROM `{self.project_id}.ssr_bbin_dw_{self.hall_name}.Wager_At_View`
                  WHERE DATE(`at`) <= '{self.data_date}' and self_result = 1
                
                  UNION ALL
                
                  SELECT 
                    {self.hall_id} AS hall_id, user_id,
                    EXTRACT(HOUR FROM `settle_at`) AS bet_hour,
                    DATE(`settle_at`) AS data_date,
                    EXTRACT(DAYOFWEEK FROM `settle_at`) AS data_weekday,
                    valid_bet AS commissionable
                  FROM `{self.project_id}.ssr_bbin_dw_{self.hall_name}.Wager_Settle_At_View`
                  WHERE DATE(`settle_at`) <= '{self.data_date}' and self_result = 1
                ) AS bet
                JOIN `{self.project_id}.ssr_bbin_dw_{self.hall_name}.User_View` member 
                ON bet.hall_id = member.hall_id
                AND bet.user_id = member.user_id
                WHERE bet.hall_id = {self.hall_id}
                GROUP BY
                bet.user_id,
                member.username,
                member.created_at,
                bet.bet_hour,
                bet.data_date,
                bet.data_weekday
                """
            )
            self.exec()
            bet_df = self.fetch_data

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
            result_df = df_timezone_format(result_df, 'Asia/Taipei')  # 設定時區

            result_df.to_csv(f'{Environment.ROOT_PATH}/files/spanner/{self.hall_name}_bet_time_15days.csv', index=False)

        except Exception as e:
            raise

    def get_latest_bet_time(self):  # 將檔案concat最新一日下注資料，重新取得15天實動檔案
        try:
            self.bet_time_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/spanner/{self.hall_name}_bet_time_15days.csv')
            bet_time_df = df_type_format_tag(self.bet_time_df)
            init_file_max_date = bet_time_df['data_date'].max()
            if init_file_max_date >= self.data_date:
                return
            self.mode = self.QUERY_MODE
            self.statement = (
                f"""
                SELECT {self.hall_id} AS hall_id, {self.domain_id} AS domain_id, bet.user_id, 
                  member.username AS user_name,
                  DATE_ADD(member.created_at, INTERVAL 12 HOUR) AS register_date, 
                  bet.bet_hour, bet.data_date, bet.data_weekday,
                  SUM(bet.commissionable) AS commissionable
                FROM (
                  SELECT 
                    {self.hall_id} AS hall_id, user_id,
                    EXTRACT(HOUR FROM `at`) AS bet_hour,
                    DATE(`at`) AS data_date,
                    EXTRACT(DAYOFWEEK FROM `at`) AS data_weekday,
                    valid_bet AS commissionable
                  FROM `{self.project_id}.ssr_bbin_dw_{self.hall_name}.Wager_At_View`
                  WHERE DATE(`at`) = '{self.data_date}' and self_result = 1

                  UNION ALL

                  SELECT 
                    {self.hall_id} AS hall_id, user_id,
                    EXTRACT(HOUR FROM `settle_at`) AS bet_hour,
                    DATE(`settle_at`) AS data_date,
                    EXTRACT(DAYOFWEEK FROM `settle_at`) AS data_weekday,
                    valid_bet AS commissionable
                  FROM `{self.project_id}.ssr_bbin_dw_{self.hall_name}.Wager_Settle_At_View`
                  WHERE DATE(`settle_at`) = '{self.data_date}' and self_result = 1
                ) AS bet
                JOIN `{self.project_id}.ssr_bbin_dw_{self.hall_name}.User_View` member 
                ON bet.hall_id = member.hall_id
                AND bet.user_id = member.user_id
                WHERE bet.hall_id = {self.hall_id}
                GROUP BY
                bet.user_id,
                member.username,
                member.created_at,
                bet.bet_hour,
                bet.data_date,
                bet.data_weekday
                """
            )
            self.exec()
            bet_df = self.fetch_data

            bet_df = df_type_format_tag(bet_df)
            bet_df = df_timezone_format(bet_df, 'Asia/Taipei')  # 設定時區

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
            activate_bet_df.to_csv(f'{Environment.ROOT_PATH}/files/spanner/{self.hall_name}_bet_time_15days.csv', index=False)
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

    def get_init_cash(self):  # 首次取得最後15日現金資料
        if path.exists(f'{Environment.ROOT_PATH}/files/spanner/{self.hall_name}_cash_15days.csv'):
            print('cash file exist, ignore initial')
            return
        self.mode = self.QUERY_MODE
        try:
            self.statement = (
                f"""
                SELECT {self.hall_id} AS hall_id, {self.domain_id} AS domain_id, cash.user_id, 
                member.username AS user_name, 
                DATE_ADD(member.created_at, INTERVAL 12 HOUR) AS register_date, 
                opcode, 
                DATE(cash.`at`) AS data_date, 
                SUM(amount) AS amount 
                FROM `{self.project_id}.ssr_bbin_dw_{self.hall_name}.Cash_Entry_View` cash 
                JOIN `{self.project_id}.ssr_bbin_dw_{self.hall_name}.User_View` member 
                ON cash.hall_id = member.hall_id 
                AND cash.user_id = member.user_id 
                WHERE cash.hall_id = {self.hall_id} 
                AND DATE(cash.`at`) <= '{self.data_date}' 
                GROUP BY cash.user_id, 
                member.username, 
                DATE_ADD(member.created_at, INTERVAL 12 HOUR), 
                DATE(cash.`at`), 
                opcode 
                """
            )
            self.exec()
            cash_df = self.fetch_data

            activate_date_df = cash_df[
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'data_date']].drop_duplicates()

            latest_15_activate_date_df = activate_date_df.sort_values('data_date').groupby(
                ['hall_id', 'domain_id', 'user_id']).tail(15)
            top_activate_date_df = latest_15_activate_date_df.sort_values('data_date').groupby(
                ['hall_id', 'domain_id', 'user_id']).head(1).rename(columns={'data_date': 'top_activate_date'})
            activate_cash_df = pd.merge(left=cash_df, right=top_activate_date_df,
                                        on=['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'],
                                        how='left')
            result_df = activate_cash_df[
                activate_cash_df['data_date'] >= activate_cash_df['top_activate_date']].reset_index(drop=True)
            result_df = df_timezone_format(result_df, 'Asia/Taipei')  # 設定時區

            result_df.to_csv(f'{Environment.ROOT_PATH}/files/spanner/{self.hall_name}_cash_15days.csv', index=False)

        except Exception as e:
            raise

    def get_latest_cash(self):  # 將檔案concat最新一日下注資料，重新取得15天實動檔案
        try:
            self.cash_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/spanner/{self.hall_name}_cash_15days.csv')
            cash_file_df = df_type_format_tag(self.cash_df)
            init_file_max_date = cash_file_df['data_date'].max()
            if init_file_max_date >= self.data_date:
                return
            self.mode = self.QUERY_MODE
            self.statement = (
                f"""
                SELECT {self.hall_id} AS hall_id, {self.domain_id} AS domain_id, cash.user_id, 
                member.username AS user_name, 
                DATE_ADD(member.created_at, INTERVAL 12 HOUR) AS register_date, 
                opcode, 
                DATE(cash.`at`) AS data_date, 
                SUM(amount) AS amount 
                FROM `{self.project_id}.ssr_bbin_dw_{self.hall_name}.Cash_Entry_View` cash 
                JOIN `{self.project_id}.ssr_bbin_dw_{self.hall_name}.User_View` member 
                ON cash.hall_id = member.hall_id 
                AND cash.user_id = member.user_id 
                WHERE cash.hall_id = {self.hall_id} 
                AND DATE(cash.`at`) = '{self.data_date}' 
                GROUP BY cash.user_id, 
                member.username, 
                DATE_ADD(member.created_at, INTERVAL 12 HOUR), 
                DATE(cash.`at`), 
                opcode 
                """
            )
            self.exec()
            cash_df = self.fetch_data

            cash_df = df_type_format_tag(cash_df)
            cash_df = df_timezone_format(cash_df, 'Asia/Taipei')  # 設定時區

            new_cash_df = pd.concat([cash_file_df, cash_df], ignore_index=True).drop(columns=['top_activate_date'])

            activate_date_df = new_cash_df[
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'data_date']].drop_duplicates()

            latest_15_activate_date_df = activate_date_df.sort_values('data_date').groupby(
                ['hall_id', 'domain_id', 'user_id']).tail(15)
            top_activate_date_df = latest_15_activate_date_df.sort_values('data_date').groupby(
                ['hall_id', 'domain_id', 'user_id']).head(1).rename(columns={'data_date': 'top_activate_date'})
            activate_cash_df = pd.merge(left=new_cash_df, right=top_activate_date_df,
                                        on=['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'],
                                        how='left')
            activate_cash_df = activate_cash_df[
                activate_cash_df['data_date'] >= activate_cash_df['top_activate_date']].reset_index(drop=True)
            activate_cash_df.to_csv(f'{Environment.ROOT_PATH}/files/spanner/{self.hall_name}_cash_15days.csv', index=False)
            self.cash_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/spanner/{self.hall_name}_cash_15days.csv')
        except Exception as e:
            raise

    def get_opcode(self):  # 取得最新opcode
        try:
            self.mode = self.QUERY_MODE
            self.statement = (
                f"""
                SELECT opcode, type  
                FROM `{self.project_id}.general_information_bbin.Opcode_View` 
                """
            )
            self.exec()
            opcode_df = self.fetch_data
            self.opcode_df = opcode_df
        except Exception as e:
            raise
