from os import path

import pandas as pd
import numpy as np
from google.cloud import spanner

import Environment
from Adaptors.SpannerAdaptor import CDP_BBIN
from Adaptors.BQAdaptor import BQ_SSR
from Adaptors.Hall import Hall
from datetime import timedelta, date
from util.dataframe_process import df_type_format, df_type_format_tag


class RuleTag:
    def __init__(self, spanner_db: CDP_BBIN, spanner_config):
        self.spanner_db = spanner_db
        self.spanner_config_db = spanner_config
        self.sv_begin_time = None
        self.sv_end_time = None
        self.telegram_message = ''
        self._tag_type = 3
        self.target_bet_df: pd.DataFrame() = pd.DataFrame()
        self.target_deposit_df: pd.DataFrame() = pd.DataFrame()
        self.target_cash_df: pd.DataFrame() = pd.DataFrame()
        self.target_opcode_df: pd.DataFrame() = pd.DataFrame()
        self.target_deposit_page_path_df: pd.DataFrame() = pd.DataFrame()
        self.data_date = date.today() - timedelta(days=1)  # yesterday
        self.ods_data_columns = ['hall_id', 'domain_id', 'user_id', 'user_name',
                                 'tag_code', 'tag_type', 'tag_enabled', 'register_date', 'updated_time']
        self._code_dict = {
            'win_lose_reaction': {
                1: {'code': 30004, 'name': '贏了會衝'},
                2: {'code': 30005, 'name': '贏了就跑'},
                3: {'code': 30006, 'name': '輸了會衝'},
                4: {'code': 30007, 'name': '輸了就跑'},
            },
            'game_lover': {
                1: {'code': 30012, 'name': '體育客'},
                2: {'code': 30013, 'name': '彩票客'},
                3: {'code': 30009, 'name': '視訊客'},
                5: {'code': 30010, 'name': '電子客'},
                6: {'code': 30011, 'name': '棋牌客'},
            },
            'boss': {
                0: {'code': 30015, 'name': '好客'}
            },
            'deposit': {
                0: {'code': 30016, 'name': '再存客', 'deposit_min': 2}
            },
            'churned': {
                0: {'code': 30017, 'name': '已流失客'}
            },
            'huge_winner': {
                0: {'code': 30019, 'name': '已流失客'}
            },
            'frequent_loser': {
                0: {'code': 30020, 'name': '最近常輸客'}
            },
            'app_user': {
                0: {'code': 30021, 'name': 'APP'}
            },
            'huge_loser': {
                0: {'code': 30022, 'name': '最近大輸客'}
            },
            'bet_weekday': {
                2: {'code': 30023, 'name': '週一客'},
                3: {'code': 30024, 'name': '週二客'},
                4: {'code': 30025, 'name': '週三客'},
                5: {'code': 30026, 'name': '週四客'},
                6: {'code': 30027, 'name': '週五客'},
                7: {'code': 30028, 'name': '週六客'},
                1: {'code': 30029, 'name': '週日客'},
            },
            'bet_time': {
                0: {'code': 30030, 'name': '00:00客'}, 1: {'code': 30031, 'name': '01:00客'},
                2: {'code': 30032, 'name': '02:00客'}, 3: {'code': 30033, 'name': '03:00客'},
                4: {'code': 30034, 'name': '04:00客'}, 5: {'code': 30035, 'name': '05:00客'},
                6: {'code': 30036, 'name': '06:00客'}, 7: {'code': 30037, 'name': '07:00客'},
                8: {'code': 30038, 'name': '08:00客'}, 9: {'code': 30039, 'name': '09:00客'},
                10: {'code': 30040, 'name': '10:00客'}, 11: {'code': 30041, 'name': '11:00客'},
                12: {'code': 30042, 'name': '12:00客'}, 13: {'code': 30043, 'name': '13:00客'},
                14: {'code': 30044, 'name': '14:00客'}, 15: {'code': 30045, 'name': '15:00客'},
                16: {'code': 30046, 'name': '16:00客'}, 17: {'code': 30047, 'name': '17:00客'},
                18: {'code': 30048, 'name': '18:00客'}, 19: {'code': 30049, 'name': '19:00客'},
                20: {'code': 30050, 'name': '20:00客'}, 21: {'code': 30051, 'name': '21:00客'},
                22: {'code': 30052, 'name': '22:00客'}, 23: {'code': 30053, 'name': '23:00客'},
            },
            'arbitrage_suspect': {
                0: {'code': 30054, 'name': '疑似刷水客'}
            },
            'login_ub': {
                0: {'code': 30055, 'name': '寰宇端'}
            },
            'login_pc': {
                0: {'code': 30056, 'name': '電腦端'}
            },
            'continuous_deposit': {
                0: {'code': 30057, 'name': '常存款客'}
            },
            'deposit_latest_average_record': {
                10000: {'code': 30069, 'name': '最後平均單筆存款_1萬'},
                50000: {'code': 30070, 'name': '最後平均單筆存款_5萬'},
                100000: {'code': 30071, 'name': '最後平均單筆存款_10萬'},
                150000: {'code': 30072, 'name': '最後平均單筆存款_15萬'},
                200000: {'code': 30073, 'name': '最後平均單筆存款_20萬'},
            },
            'max_bet_per_game': {
                10000: {'code': 30074, 'name': '最大有效投注金額_1萬'},
                50000: {'code': 30075, 'name': '最大有效投注金額_5萬'},
                100000: {'code': 30076, 'name': '最大有效投注金額_10萬'},
                500000: {'code': 30077, 'name': '最大有效投注金額_50萬'},
                1000000: {'code': 30078, 'name': '最大有效投注金額_100萬'},
            },
            'week_valid_bet_descend': {
                1000000: {'code': 30079, 'name': '有效投注下降幅度_100萬'},
                2000000: {'code': 30080, 'name': '有效投注下降幅度_200萬'},
                3000000: {'code': 30081, 'name': '有效投注下降幅度_300萬'},
                5000000: {'code': 30082, 'name': '有效投注下降幅度_500萬'},
                7000000: {'code': 30083, 'name': '有效投注下降幅度_700萬'},
                9000000: {'code': 30084, 'name': '有效投注下降幅度_900萬'},
                12000000: {'code': 30085, 'name': '有效投注下降幅度_1200萬'},
                15000000: {'code': 30086, 'name': '有效投注下降幅度_1500萬'},
                20000000: {'code': 30087, 'name': '有效投注下降幅度_2000萬'},
                30000000: {'code': 30088, 'name': '有效投注下降幅度_3000萬'},
                40000000: {'code': 30089, 'name': '有效投注下降幅度_4000萬'},
                50000000: {'code': 30090, 'name': '有效投注下降幅度_5000萬'},
            },
            'deposit_method': {
                1: {'code': 30200, 'opcode_list': ['CD'], 'name': '常用入款_公司入款'},
                2: {'code': 30201, 'opcode_list': ['CryptoD'], 'name': '常用入款_加密貨幣入款'},
                3: {'code': 30202, 'opcode_list': ['MD'], 'name': '常用入款_人工存入'},
                4: {'code': 30203, 'opcode_list': [2096, 2097], 'name': '常用入款_購寶錢包'},
                5: {'code': 30204, 'opcode_list': [2099, 2100], 'name': '常用入款_CGPAY支付'},
                6: {'code': 30205, 'opcode_list': [1039, 1040], 'name': '常用入款_線上存款'},
                7: {'code': 30206, 'opcode_list': [2508, 2509], 'name': '常用入款_e點付'},
                8: {'code': 30207, 'opcode_list': [2525, 2526], 'name': '常用入款_e點富'},
                9: {'code': 30208, 'opcode_list': [2514, 2515], 'name': '常用入款_OSPay'},
            },
            'hesitate': {
                0: {'code': 30091, 'name': '猶豫客'}
            },
            'certification': {
                0: {'code': 30092, 'name': '實名驗證'},
            },
            'term': {
                0: {'code': 30093, 'name': '新會員'},
                1: {'code': 30094, 'name': '初期會員'},
                2: {'code': 30095, 'name': '中期會員'},
                3: {'code': 30096, 'name': '長期會員'},
            },
        }

    def get_init_activated(self, hall: Hall):  # 首次取得15日內實動資料
        if path.exists(f'{Environment.ROOT_PATH}/files/spanner/{hall.hall_name}_activated_bet_data_15days.csv'):
            print('activated file exist, ignore initial')
            return
        try:
            self.spanner_db.statement = (
                "SELECT bet.hall_id, bet.domain_id, bet.user_id, member.user_name, member.register_date, "
                "lobby_group, lobby, game_type, "
                "SUM(wagers_total), SUM(bet_amount), SUM(commissionable), SUM(payoff), data_date "
                "FROM member_info member "
                "JOIN bet_analysis bet "
                "    ON member.hall_id = bet.hall_id "
                "    AND member.domain_id = bet.domain_id "
                "    AND member.user_id = bet.user_id "
                f"WHERE member.hall_id = {hall.hall_id} AND member.domain_id = {hall.domain_id} "
                "AND member.user_name IS NOT NULL "
                "AND commissionable > 0 "
                "GROUP BY bet.hall_id, bet.domain_id, bet.user_id, member.user_name, member.register_date, "
                "lobby_group, lobby, game_type, data_date "
            )
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.exec()
            sync_columns = ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date',
                            'lobby_group', 'lobby', 'game_type',
                            'wagers_total', 'bet_amount', 'commissionable', 'payoff', 'data_date']
            bet_df = pd.DataFrame(self.spanner_db.fetch_data, columns=sync_columns)
            bet_df = df_type_format_tag(bet_df)

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
            result_df.to_csv(f'{Environment.ROOT_PATH}/files/spanner/{hall.hall_name}_activated_bet_data_15days.csv', index=False)

        except Exception as e:
            raise

    def get_latest_activated(self, hall: Hall):  # 將實動檔案concat最新一日下注資料，重新取得15天實動檔案
        try:
            activated_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/spanner/{hall.hall_name}_activated_bet_data_15days.csv')
            max_date = \
                activated_df.sort_values('data_date').groupby(['hall_id', 'domain_id']).tail(1)['data_date'].values[0]
            activated_df = df_type_format_tag(activated_df)
            self.spanner_db.statement = (
                "SELECT bet.hall_id, bet.domain_id, bet.user_id, member.user_name, member.register_date, "
                "lobby_group, lobby, game_type, "
                "SUM(wagers_total), SUM(bet_amount), SUM(commissionable), SUM(payoff), data_date "
                "FROM member_info member "
                "JOIN bet_analysis bet "
                "    ON member.hall_id = bet.hall_id "
                "    AND member.domain_id = bet.domain_id "
                "    AND member.user_id = bet.user_id "
                f"WHERE member.hall_id = {hall.hall_id} AND member.domain_id = {hall.domain_id} "
                "AND member.user_name IS NOT NULL "
                "AND commissionable > 0 "
                f"AND data_date > \'{max_date}\' "
                "GROUP BY bet.hall_id, bet.domain_id, bet.user_id, member.user_name, member.register_date, "
                "lobby_group, lobby, game_type, data_date"
            )
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.exec()
            sync_columns = ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date',
                            'lobby_group', 'lobby', 'game_type',
                            'wagers_total', 'bet_amount', 'commissionable', 'payoff', 'data_date']
            bet_df = pd.DataFrame(self.spanner_db.fetch_data, columns=sync_columns)
            bet_df = df_type_format_tag(bet_df)

            new_activate_df = pd.concat([activated_df, bet_df], ignore_index=True).drop(columns=['top_activate_date'])

            activate_date_df = new_activate_df[
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'data_date']].drop_duplicates()

            latest_15_activate_date_df = activate_date_df.sort_values('data_date').groupby(
                ['hall_id', 'domain_id', 'user_id']).tail(15)
            top_activate_date_df = latest_15_activate_date_df.sort_values('data_date').groupby(
                ['hall_id', 'domain_id', 'user_id']).head(1).rename(columns={'data_date': 'top_activate_date'})
            activate_bet_df = pd.merge(left=new_activate_df, right=top_activate_date_df,
                                       on=['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'],
                                       how='left')
            activate_bet_df = activate_bet_df[
                activate_bet_df['data_date'] >= activate_bet_df['top_activate_date']].reset_index(drop=True)
            activate_bet_df.to_csv(f'{Environment.ROOT_PATH}/files/spanner/{hall.hall_name}_activated_bet_data_15days.csv', index=False)
            self.target_bet_df = activate_bet_df

        except Exception as e:
            raise

    def get_init_deposit(self, hall: Hall):  # 首次取得15日內存款資料
        if path.exists(f'{Environment.ROOT_PATH}/files/spanner/{hall.hall_name}_deposit_data_15days.csv'):
            print('deposit file exist, ignore initial')
            return
        try:
            self.spanner_db.statement = (
                "SELECT depo.hall_id, depo.domain_id, depo.user_id, member.user_name, member.register_date, "
                "deposit_amount, deposit_count, data_date "
                "FROM member_info member "
                "JOIN deposit_withdraw_record depo "
                "    ON member.hall_id = depo.hall_id "
                "    AND member.domain_id = depo.domain_id "
                "    AND member.user_id = depo.user_id "
                f"WHERE member.hall_id = {hall.hall_id} AND member.domain_id = {hall.domain_id} "
                "AND member.user_name IS NOT NULL "
                "AND deposit_amount > 0 "
            )
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.exec()
            sync_columns = ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date',
                            'deposit_amount', 'deposit_count', 'data_date']
            deposit_df = pd.DataFrame(self.spanner_db.fetch_data, columns=sync_columns)
            deposit_df = df_type_format_tag(deposit_df)

            activate_date_df = deposit_df[
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'data_date']].drop_duplicates()

            latest_15_activate_date_df = activate_date_df.sort_values('data_date').groupby(
                ['hall_id', 'domain_id', 'user_id']).tail(15)
            top_activate_date_df = latest_15_activate_date_df.sort_values('data_date').groupby(
                ['hall_id', 'domain_id', 'user_id']).head(1).rename(columns={'data_date': 'top_activate_date'})
            activate_deposit_df = pd.merge(left=deposit_df, right=top_activate_date_df,
                                           on=['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'],
                                           how='left')
            result_df = activate_deposit_df[
                activate_deposit_df['data_date'] >= activate_deposit_df['top_activate_date']].reset_index(drop=True)
            result_df.to_csv(f'{Environment.ROOT_PATH}/files/spanner/{hall.hall_name}_deposit_data_15days.csv', index=False)

        except Exception as e:
            raise

    def get_latest_deposit(self, hall: Hall):  # 將實動檔案concat最新一日存款資料，重新取得15天實動檔案
        try:
            activated_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/spanner/{hall.hall_name}_deposit_data_15days.csv')
            max_date = \
                activated_df.sort_values('data_date').groupby(['hall_id', 'domain_id']).tail(1)['data_date'].values[0]
            activated_df = df_type_format_tag(activated_df)
            self.spanner_db.statement = (
                "SELECT depo.hall_id, depo.domain_id, depo.user_id, member.user_name, member.register_date, "
                "deposit_amount, deposit_count, data_date "
                "FROM member_info member "
                "JOIN deposit_withdraw_record depo "
                "    ON member.hall_id = depo.hall_id "
                "    AND member.domain_id = depo.domain_id "
                "    AND member.user_id = depo.user_id "
                f"WHERE member.hall_id = {hall.hall_id} AND member.domain_id = {hall.domain_id} "
                "AND member.user_name IS NOT NULL "
                "AND deposit_amount > 0 "
                f"AND data_date > \'{max_date}\' "
            )
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.exec()
            sync_columns = ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date',
                            'deposit_amount', 'deposit_count', 'data_date']
            deposit_df = pd.DataFrame(self.spanner_db.fetch_data, columns=sync_columns)
            deposit_df = df_type_format_tag(deposit_df)

            new_activate_df = pd.concat([activated_df, deposit_df], ignore_index=True).drop(
                columns=['top_activate_date'])

            activate_date_df = new_activate_df[
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'data_date']].drop_duplicates()

            latest_15_activate_date_df = activate_date_df.sort_values('data_date').groupby(
                ['hall_id', 'domain_id', 'user_id']).tail(15)
            top_activate_date_df = latest_15_activate_date_df.sort_values('data_date').groupby(
                ['hall_id', 'domain_id', 'user_id']).head(1).rename(columns={'data_date': 'top_activate_date'})
            activate_deposit_df = pd.merge(left=new_activate_df, right=top_activate_date_df,
                                           on=['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'],
                                           how='left')
            activate_deposit_df = activate_deposit_df[
                activate_deposit_df['data_date'] >= activate_deposit_df['top_activate_date']].reset_index(drop=True)
            activate_deposit_df.to_csv(f'{Environment.ROOT_PATH}/files/spanner/{hall.hall_name}_deposit_data_15days.csv', index=False)
            self.target_deposit_df = activate_deposit_df

        except Exception as e:
            raise

    def get_deposit_page_path(self, hall: Hall, day_count: int):
        try:
            self.spanner_db.statement = (
                f"""
                SELECT ga.hall_id, ga.domain_id, ga.user_id, member.user_name, member.register_date, 
                ga.page_path, ga.data_date
                FROM member_info member 
                JOIN ga_page_path ga 
                    ON member.hall_id = ga.hall_id 
                    AND member.domain_id = ga.domain_id 
                    AND member.user_id = ga.user_id 
                WHERE member.hall_id = {hall.hall_id} AND member.domain_id = {hall.domain_id}
                AND member.user_name IS NOT NULL 
                AND ga.page_path LIKE '%deposit%'
                AND ga.data_date >= '{self.data_date - timedelta(days=day_count)}'
                GROUP BY ga.hall_id, ga.domain_id, ga.user_id, member.user_name, member.register_date, 
                ga.page_path, ga.data_date
                """
            )
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.exec()
            sync_columns = ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'page_path', 'data_date']
            page_path_df = pd.DataFrame(self.spanner_db.fetch_data, columns=sync_columns)
            page_path_df.fillna(value=0, inplace=True)
            page_path_df = df_type_format(page_path_df)
            self.target_deposit_page_path_df = page_path_df
        except Exception as e:
            raise

    def ods_to_dw(self, hall: Hall, tag_code=None):
        print('ods_to_dw begin')
        tag_condition = f"AND tag_code = {tag_code} " if tag_code is not None else "AND 1=1 "
        try:
            local_tag_name, dev_tag_name, prod_tag_name, \
            local_tag_name_cn, dev_tag_name_cn, prod_tag_name_cn, \
            local_tag_name_en, dev_tag_name_en, prod_tag_name_en = self.spanner_config_db.get_config_tag(hall)
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.statement = (
                f"""
                with temp_date as (
                    select DATE_SUB(max(DATE(updated_time, 'America/New_York')), INTERVAL 1 DAY) as max_updated_date
                    FROM user_tag_ods_data
                    where hall_id = {hall.hall_id} and domain_id = {hall.domain_id}
                )
                select
                    ods.hall_id, ods.domain_id, ods.user_id, member.user_name, member.ag_name, 
                    ods.tags as tag_str, 
                    ods.local_tag_names as local_tag_name_str, 
                    ods.dev_tag_names as demo_tag_name_str, 
                    ods.prod_tag_names as prod_tag_name_str, 
                    ods.local_tag_names_cn as local_tag_name_str_cn, 
                    ods.dev_tag_names_cn as demo_tag_name_str_cn, 
                    ods.prod_tag_names_cn as prod_tag_name_str_cn, 
                    ods.local_tag_names_en as local_tag_name_str_en, 
                    ods.dev_tag_names_en as demo_tag_name_str_en, 
                    ods.prod_tag_names_en as prod_tag_name_str_en, 
                    member.user_level_id, member.user_level, member.register_date
                from (
                    SELECT
                        hall_id, domain_id, user_id, 
                        STRING_AGG(Cast (tag_code as STRING)) as tags,
                        STRING_AGG(Cast ((case  {local_tag_name} end) as STRING)) as local_tag_names,
                        STRING_AGG(Cast ((case  {dev_tag_name} end) as STRING)) as dev_tag_names,
                        STRING_AGG(Cast ((case  {prod_tag_name} end) as STRING)) as prod_tag_names,
                        STRING_AGG(Cast ((case  {local_tag_name_cn} end) as STRING)) as local_tag_names_cn,
                        STRING_AGG(Cast ((case  {dev_tag_name_cn} end) as STRING)) as dev_tag_names_cn,
                        STRING_AGG(Cast ((case  {prod_tag_name_cn} end) as STRING)) as prod_tag_names_cn,
                        STRING_AGG(Cast ((case  {local_tag_name_en} end) as STRING)) as local_tag_names_en,
                        STRING_AGG(Cast ((case  {dev_tag_name_en} end) as STRING)) as dev_tag_names_en,
                        STRING_AGG(Cast ((case  {prod_tag_name_en} end) as STRING)) as prod_tag_names_en
                    FROM user_tag_ods_data
                    WHERE (hall_id, domain_id, user_id, tag_enabled) in 
                            (select 
                                (hall_id, domain_id, user_id, TRUE)
                            from user_tag_ods_data
                            where hall_id = {hall.hall_id} and domain_id = {hall.domain_id}
                            {tag_condition}
                            AND tag_enabled IS TRUE
                            and DATE(updated_time, 'America/New_York') >= (select max_updated_date from temp_date))
                    GROUP BY hall_id, domain_id, user_id
                ) ods
                JOIN member_info member ON ods.hall_id = member.hall_id and ods.domain_id = member.domain_id and ods.user_id = member.user_id
            """
            )
            self.spanner_db.exec()
            [x.append(spanner.COMMIT_TIMESTAMP) for x in self.spanner_db.fetch_data]
            self.spanner_db.columns = [
                'hall_id', 'domain_id', 'user_id', 'user_name', 'ag_name',
                'tag_str',
                'local_tag_name_str', 'demo_tag_name_str', 'prod_tag_name_str',
                'local_tag_name_str_cn', 'demo_tag_name_str_cn', 'prod_tag_name_str_cn',
                'local_tag_name_str_en', 'demo_tag_name_str_en', 'prod_tag_name_str_en',
                'user_level_id', 'user_level', 'register_date', 'updated_time']

            tag_dw_df = pd.DataFrame(self.spanner_db.fetch_data, columns=self.spanner_db.columns)

            if tag_code:
                self.spanner_db.statement = (
                    "SELECT ods.hall_id, ods.domain_id, ods.user_id, bet.last_activated_date "
                    "FROM user_tag_ods_data ods "
                    "JOIN ( "
                    "    SELECT hall_id, domain_id, user_id, MAX(data_date) AS last_activated_date "
                    "    FROM bet_analysis "
                    f"    WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
                    "    GROUP BY hall_id, domain_id, user_id "
                    "    ) bet "
                    "ON ods.hall_id = bet.hall_id "
                    "AND ods.domain_id = bet.domain_id "
                    "AND ods.user_id = bet.user_id "
                    f"WHERE ods.tag_code = {tag_code} AND ods.tag_enabled IS TRUE "
                )
                self.spanner_db.mode = self.spanner_db.QUERY_MODE
                self.spanner_db.exec()
                activated_date_df = pd.DataFrame(
                    self.spanner_db.fetch_data, columns=['hall_id', 'domain_id', 'user_id', 'last_activated_date'])
                tag_dw_df = pd.merge(left=tag_dw_df, right=activated_date_df,
                                     on=['hall_id', 'domain_id', 'user_id'],
                                     how='left')
                tag_dw_df = tag_dw_df.fillna(np.nan).replace([np.nan], [None])
        except Exception as e:
            raise
        return tag_dw_df

    def call_etl_process_dw_date(self, hall: Hall):
        try:
            # member
            self.spanner_db.statement = f"""
                    select 
                       STRING_AGG(CAST(user_id as STRING))
                    FROM user_tag_dw_data
                    where hall_id = {hall.hall_id} and domain_id = {hall.domain_id}
                """
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.exec()
            member_df = pd.DataFrame(data=self.spanner_db.fetch_data, columns=["user_id"])
            member_df = pd.DataFrame(data=member_df['user_id'][0].split(','), columns=["user_id"])
            member_df['user_id'] = member_df['user_id'].astype(int)

            # 轉置 最大實動日期 與 最大登入日期
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.statement = f"""
                select 
                    hall_id, domain_id, user_id, 
                    max(last_activated_date) as last_activated_date, max(last_login_date) as last_login_date
                from (
                    select 
                        hall_id, domain_id, user_id, max(data_date) as last_activated_date, null as last_login_date
                    from bet_analysis
                    where hall_id = {hall.hall_id} and domain_id = {hall.domain_id} and 
                          data_date >= (
                              select 
                                   ifnull(DATE_SUB(max(last_activated_date), INTERVAL 60 DAY), '1990-01-01')
                              FROM user_tag_dw_data
                              where hall_id = {hall.hall_id} and domain_id = {hall.domain_id} and last_activated_date is not null
                          )
                    group by hall_id, domain_id, user_id
                    Union ALL
                        select 
                        hall_id, domain_id, user_id,null as last_activated_date, max(data_date) as last_login_date
                    from login_log
                    where hall_id = {hall.hall_id} and domain_id = {hall.domain_id}
                        and data_date >= (
                            select 
                                ifnull(DATE_SUB(max(last_login_date), INTERVAL 60 DAY), '1990-01-01')
                            FROM user_tag_dw_data
                            where hall_id = {hall.hall_id} and domain_id = {hall.domain_id} and last_login_date is not null
                        )
                    group by hall_id, domain_id, user_id
                ) group by hall_id, domain_id, user_id
            """
            self.spanner_db.exec()
            tag_dw_df = pd.DataFrame(data=self.spanner_db.fetch_data,
                                     columns=["hall_id", "domain_id", "user_id", "last_activated_date",
                                              "last_login_date"])
            tag_dw_df[['hall_id', 'domain_id', 'user_id']] = tag_dw_df[['hall_id', 'domain_id', 'user_id']].astype(int)
            tag_dw_df = tag_dw_df[tag_dw_df['user_id'].isin(member_df['user_id'])].reset_index(drop=True)

        except Exception as e:
            raise
        return tag_dw_df

    def call_etl_delete_dw_data(self, hall: Hall):
        try:
            # delete dw data from ods all false user
            self.spanner_db.statement = f"""
                select 
                    hall_id, domain_id, user_id
                from (
                    select 
                        hall_id, domain_id, user_id, 
                        sum(case when tag_enabled then 1 else 0 end) tag_code_total
                    from user_tag_ods_data
                    where hall_id = {hall.hall_id} and domain_id = {hall.domain_id} and 
                    (hall_id, domain_id, user_id) in (
                        select (hall_id, domain_id, user_id) from user_tag_dw_data
                    )
                    group by hall_id, domain_id, user_id
                ) where tag_code_total = 0
                """
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.exec()
            tag_dw_df = pd.DataFrame(data=self.spanner_db.fetch_data, columns=["hall_id", "domain_id", "user_id"])
            tag_dw_df[['hall_id', 'domain_id', 'user_id']] = tag_dw_df[['hall_id', 'domain_id', 'user_id']].astype(int)
        except Exception as e:
            raise
        return tag_dw_df

    def tag_edit(self, hall: Hall):
        print(f"{hall.hall_name} tag_edit begin")
        try:
            tag_code_list = list(tag['code'] for tag_group in list(self._code_dict.values()) for key, tag in tag_group.items())
            update_df = pd.DataFrame()
            for tag_code in tag_code_list:
                self.spanner_db.statement = (
                    f"""
                    SELECT 
                      hall_id, domain_id, user_id, tag_code, 
                      CASE WHEN edit_type = 1 THEN True WHEN edit_type = 2 THEN False END 
                    FROM 
                      user_tag_edit_records 
                    WHERE (hall_id, domain_id, user_id, tag_code, updated_time) IN 
                    (SELECT 
                      (hall_id, domain_id, user_id, tag_code, MAX(updated_time)) 
                    FROM 
                      user_tag_edit_records 
                    WHERE hall_id =  {hall.hall_id} AND domain_id = {hall.domain_id} 
                    AND tag_code = {tag_code} 
                    GROUP BY 
                    hall_id, domain_id, user_id, tag_code)
                    """
                )
                self.spanner_db.mode = self.spanner_db.QUERY_MODE
                self.spanner_db.exec()
                [x.append(spanner.COMMIT_TIMESTAMP) for x in self.spanner_db.fetch_data]
                tag_code_df = pd.DataFrame(data=self.spanner_db.fetch_data, columns=[
                    "hall_id", "domain_id", "user_id", "tag_code", "tag_enabled", "updated_time"])
                update_df = pd.concat([update_df, tag_code_df], ignore_index=True)
        except Exception as e:
            raise
        return update_df

    def tag_reset(self, hall: Hall, tag_code: int):  # 重置tag_enabled為0
        try:
            self.spanner_db.column_times_setter(column_times=2)
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.statement = (
                f"""
                SELECT hall_id, 
                domain_id, 
                user_id, 
                tag_code, 
                FALSE AS tag_enabled 
                FROM user_tag_ods_data 
                WHERE hall_id = {hall.hall_id} 
                AND domain_id = {hall.domain_id} 
                AND tag_code = {tag_code} 
                """
            )
            self.spanner_db.exec()
            self.spanner_db.update_data = self.spanner_db.fetch_data
            [x.append(spanner.COMMIT_TIMESTAMP) for x in self.spanner_db.update_data]

            self.spanner_db.columns = ['hall_id', 'domain_id', 'user_id', 'tag_code', 'tag_enabled', 'updated_time']
            self.spanner_db.mode = self.spanner_db.UPDATE_MUTATION_MODE
            self.spanner_db.table_name = 'user_tag_ods_data'
            self.spanner_db.exec()

        except Exception as e:
            raise

    def churned(self, hall: Hall) -> pd.DataFrame:  # 流失客
        print('tag_churned begin')
        try:
            self.spanner_db.statement = (
                f"""
                SELECT 
                    member.hall_id, member.domain_id, member.user_id AS uid, member.user_name, 
                    {self._code_dict['churned'][0]['code']} AS tag_code, 
                    {self._tag_type} AS tag_type, 
                    login_log.tag_enabled, member.register_date,
                FROM (
                  SELECT 
                  hall_id, domain_id, user_id,
                  if(DATE_DIFF(MAX(data_date), '{self.data_date}',DAY) <= -30, True, FALSE) AS tag_enabled 
                  FROM login_log 
                  WHERE data_date BETWEEN date_add('{self.data_date}', INTERVAL -31 DAY) AND '{self.data_date}'
                        AND hall_id = {hall.hall_id}  
                        AND domain_id = {hall.domain_id}  
                  GROUP BY hall_id, domain_id, user_id
                ) login_log
                LEFT JOIN member_info member ON 
                login_log.hall_id = member.hall_id 
                AND login_log.domain_id = member.domain_id 
                AND login_log.user_id = member.user_id 
                WHERE member.hall_id = {hall.hall_id} AND member.domain_id = {hall.domain_id} AND member.ag_name IS NOT NULL
                """
            )
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.exec()
            [x.append(spanner.COMMIT_TIMESTAMP) for x in self.spanner_db.fetch_data]
            churned_df = pd.DataFrame(self.spanner_db.fetch_data, columns=self.ods_data_columns)

        except Exception as e:
            raise

        return churned_df

    def boss(self, hall: Hall) -> pd.DataFrame:  # 好客
        print('tag_boss begin')
        try:
            tag_code = self._code_dict['boss'][0]['code']
            game_sum_df = self.target_bet_df.groupby(['hall_id', 'domain_id', 'lobby', 'lobby_group', 'game_type'])[
                'wagers_total', 'commissionable', 'payoff'].sum().reset_index()
            game_sum_df['rate'] = (game_sum_df['payoff'] / game_sum_df['commissionable']) * 100
            top_wager_game_code_df = pd.DataFrame()
            for game_kind in range(1, 7, 1):
                game_kind_df = game_sum_df.loc[game_sum_df['lobby_group'] == game_kind]
                game_kind_df = game_kind_df.sort_values('wagers_total').groupby(
                    ['hall_id', 'domain_id', 'lobby_group']).tail(len(game_kind_df) / 2)
                top_wager_game_code_df = pd.concat([top_wager_game_code_df, game_kind_df], ignore_index=True)
            bite_game_df = top_wager_game_code_df
            bite_game_df = bite_game_df[['hall_id', 'domain_id', 'lobby', 'lobby_group', 'game_type']].loc[
                bite_game_df['rate'] <= -0.2]
            bite_game_df['is_bite'] = 1

            # 計算每位使用者 實動天數 至少超過3天(包含3天)
            data_df = self.target_bet_df[['hall_id', 'domain_id', 'user_id', 'data_date']].drop_duplicates()
            filter_bet_df = \
                data_df[data_df.groupby(by=['hall_id', 'domain_id', 'user_id'])['hall_id'].transform('size') >= 3][
                    ['hall_id', 'domain_id', 'user_id']].drop_duplicates()
            target_bet_df = self.target_bet_df[self.target_bet_df['user_id'].isin(filter_bet_df['user_id'])]

            merged_df = pd.merge(left=target_bet_df, right=bite_game_df,
                                 on=['hall_id', 'domain_id', 'lobby', 'lobby_group', 'game_type'],
                                 how='left')

            bite_df = merged_df[merged_df['is_bite'] == 1].groupby(['hall_id', 'domain_id', 'user_id'])[
                'commissionable'].sum().reset_index().rename(columns={'commissionable': 'bite_commissionable'})

            sum_df = merged_df.groupby(['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'])[
                'commissionable'].sum().reset_index()

            self.spanner_db.statement = (
                f"""
                SELECT 
                hall_id, domain_id, user_id, 
                SUM(deposit_count) AS deposit_count, 
                SUM(deposit_amount) AS deposit_amount, 
                SUM(deposit_amount) - SUM(withdraw_amount) AS deposit_withdraw 
                FROM deposit_withdraw_record 
                WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} 
                AND data_date <= '{self.data_date}' 
                GROUP BY hall_id , domain_id , user_id
                """
            )
            sync_columns = ['hall_id', 'domain_id', 'user_id', 'deposit_count', 'deposit_amount', 'deposit_withdraw']
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.exec()
            deposit_df = pd.DataFrame(self.spanner_db.fetch_data, columns=sync_columns)

            payoff_day_df = target_bet_df.groupby(['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'data_date'])['payoff'].sum().reset_index()
            payoff_day_df['total_day'] = 1
            payoff_day_df['lose_day'] = np.where(payoff_day_df['payoff'] < 0, 1, 0)
            payoff_day_df = payoff_day_df.groupby(['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'])['total_day', 'lose_day'].sum().reset_index()
            payoff_day_df['lose_rate'] = payoff_day_df['lose_day'] / payoff_day_df['total_day']
            lose_day_df = payoff_day_df.loc[payoff_day_df['lose_rate'] >= 0.6][['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']]

            lobby_df = target_bet_df.groupby(['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'lobby'])['payoff'].sum().reset_index()
            lobby_df['win_count'] = np.where(lobby_df['payoff'] > 0, 1, 0)
            lobby_df['lose_count'] = np.where(lobby_df['payoff'] <= 0, 1, 0)
            lobby_df = lobby_df.groupby(['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'])['win_count', 'lose_count'].sum().reset_index()
            lose_lobby_df = lobby_df.loc[lobby_df['win_count'] < lobby_df['lose_count']][['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']]

            lose_rate_df = pd.merge(left=lose_day_df, right=lose_lobby_df, on=['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'], how='inner')
            lose_rate_df['tag_enabled'] = True

            good_boss_df = pd.merge(left=sum_df, right=bite_df, on=['hall_id', 'domain_id', 'user_id'], how='left')
            good_boss_df = pd.merge(left=good_boss_df, right=deposit_df, on=['hall_id', 'domain_id', 'user_id'],
                                    how='left')
            good_boss_df.fillna(0, inplace=True)
            good_boss_df['tag_enabled'] = np.where(
                ((good_boss_df['bite_commissionable'] / good_boss_df['commissionable']) > 0.5)
                & (good_boss_df['deposit_count'] >= 2)
                & (good_boss_df['deposit_amount'] >= hall.boss_tag_threshold)
                & (good_boss_df['deposit_withdraw'] > 0)
                , True, False)
            good_boss_df = pd.concat([good_boss_df, lose_rate_df], ignore_index=True)

            good_boss_df['tag_code'] = tag_code
            good_boss_df['tag_type'] = self._tag_type
            good_boss_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            good_boss_df = good_boss_df[self.ods_data_columns].loc[good_boss_df['tag_enabled'] == 1]

        except Exception as e:
            raise

        self.tag_reset(hall, tag_code)
        return good_boss_df

    def deposit(self, hall: Hall) -> pd.DataFrame:  # 再存客
        print('tag_deposit begin')
        try:
            self.spanner_db.statement = (
                f"""
                SELECT 
                    member.hall_id, member.domain_id, member.user_id, member.user_name, 
                    {self._code_dict['deposit'][0]['code']} AS tag_code, 
                    {self._tag_type} AS tag_type, 
                    TRUE AS tag_enabled, member.register_date 
                FROM (
                  SELECT 
                  hall_id, domain_id, user_id
                  FROM deposit_withdraw_record 
                  WHERE hall_id = {hall.hall_id}  
                        AND domain_id = {hall.domain_id}  
                        AND data_date <= '{self.data_date}' 
                  GROUP BY hall_id, domain_id, user_id
                  HAVING SUM(deposit_count) >= {self._code_dict['deposit'][0]['deposit_min']} 
                ) depo
                LEFT JOIN member_info member ON 
                depo.hall_id = member.hall_id 
                AND depo.domain_id = member.domain_id 
                AND depo.user_id = member.user_id 
                WHERE member.hall_id = {hall.hall_id} AND member.domain_id = {hall.domain_id} 
                AND member.ag_name IS NOT NULL
                """
            )
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.exec()
            [x.append(spanner.COMMIT_TIMESTAMP) for x in self.spanner_db.fetch_data]
            deposit_df = pd.DataFrame(self.spanner_db.fetch_data, columns=self.ods_data_columns)

        except Exception as e:
            raise

        return deposit_df

    def huge_winner(self, hall: Hall) -> pd.DataFrame:  # 最近大贏
        print('tag_huge_winner begin')
        try:
            tag_code = self._code_dict['huge_winner'][0]['code']

            # 計算每位使用者 實動天數 至少超過3天(包含3天)
            data_df = self.target_bet_df[['hall_id', 'domain_id', 'user_id', 'data_date']].drop_duplicates()
            filter_bet_df = \
                data_df[data_df.groupby(by=['hall_id', 'domain_id', 'user_id'])['hall_id'].transform('size') >= 3][
                    ['hall_id', 'domain_id', 'user_id']].drop_duplicates()
            target_bet_df = self.target_bet_df[self.target_bet_df['user_id'].isin(filter_bet_df['user_id'])]

            huge_winner_df = target_bet_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']).sum().reset_index()
            huge_winner_df['tag_code'] = tag_code
            huge_winner_df['tag_type'] = self._tag_type
            huge_winner_df['tag_enabled'] = False
            huge_winner_df['tag_enabled'] = np.where(
                (huge_winner_df['payoff'] > huge_winner_df['commissionable'] * 0.5)
                , True, False)
            huge_winner_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            huge_winner_df = huge_winner_df[self.ods_data_columns].loc[huge_winner_df['tag_enabled'] == 1]

        except Exception as e:
            raise

        self.tag_reset(hall, tag_code)
        return huge_winner_df

    def huge_loser(self, hall: Hall) -> pd.DataFrame:  # 最近大輸
        print('tag_huge_loser begin')
        try:
            tag_code = self._code_dict['huge_loser'][0]['code']

            # 計算每位使用者 實動天數 至少超過3天(包含3天)
            data_df = self.target_bet_df[['hall_id', 'domain_id', 'user_id', 'data_date']].drop_duplicates()
            filter_bet_df = \
                data_df[data_df.groupby(by=['hall_id', 'domain_id', 'user_id'])['hall_id'].transform('size') >= 3][
                    ['hall_id', 'domain_id', 'user_id']].drop_duplicates()
            target_bet_df = self.target_bet_df[self.target_bet_df['user_id'].isin(filter_bet_df['user_id'])]

            huge_loser_df = target_bet_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']).sum().reset_index()
            huge_loser_df['tag_code'] = tag_code
            huge_loser_df['tag_type'] = self._tag_type
            huge_loser_df['tag_enabled'] = False
            huge_loser_df['tag_enabled'] = np.where(
                (huge_loser_df['payoff'] < 0) & (huge_loser_df['payoff'] < (huge_loser_df['commissionable'] * -0.5))
                , True, False)
            huge_loser_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            huge_loser_df = huge_loser_df[self.ods_data_columns].loc[huge_loser_df['tag_enabled'] == 1]

        except Exception as e:
            raise

        self.tag_reset(hall, tag_code)
        return huge_loser_df

    def frequent_loser(self, hall: Hall) -> pd.DataFrame:  # 最近常輸
        print('tag_frequent_loser begin')
        try:
            tag_code = self._code_dict['frequent_loser'][0]['code']

            # 計算每位使用者 實動天數 至少超過3天(包含3天)
            data_df = self.target_bet_df[['hall_id', 'domain_id', 'user_id', 'data_date']].drop_duplicates()
            filter_bet_df = \
                data_df[data_df.groupby(by=['hall_id', 'domain_id', 'user_id'])['hall_id'].transform('size') >= 3][
                    ['hall_id', 'domain_id', 'user_id']].drop_duplicates()
            target_bet_df = self.target_bet_df[self.target_bet_df['user_id'].isin(filter_bet_df['user_id'])]

            win_lose_count_df = target_bet_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'data_date']).sum().reset_index()
            win_lose_count_df['win_count'] = 0
            win_lose_count_df['win_count'].loc[win_lose_count_df['payoff'] >= 0] = 1
            win_lose_count_df['lose_count'] = 0
            win_lose_count_df['lose_count'].loc[win_lose_count_df['payoff'] < 0] = 1
            win_lose_count_df = win_lose_count_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']).sum().reset_index()

            win_lose_count_df['tag_code'] = tag_code
            win_lose_count_df['tag_type'] = self._tag_type
            win_lose_count_df['tag_enabled'] = False
            win_lose_count_df['tag_enabled'] = np.where(
                (win_lose_count_df['win_count'] < win_lose_count_df['lose_count'])
                & (win_lose_count_df['payoff'] < (win_lose_count_df['commissionable'] * -0.05))
                , True, False)
            win_lose_count_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            frequent_loser_df = win_lose_count_df[self.ods_data_columns].loc[win_lose_count_df['tag_enabled'] == 1]

        except Exception as e:
            raise

        self.tag_reset(hall, tag_code)
        return frequent_loser_df

    def game_lover(self, hall: Hall) -> pd.DataFrame:  # 各種遊戲客
        print('tag_game_lover begin')
        try:
            tag_code_dict = self._code_dict['game_lover']

            # 計算每位使用者 實動天數 至少超過3天(包含3天)
            data_df = self.target_bet_df[['hall_id', 'domain_id', 'user_id', 'data_date']].drop_duplicates()
            filter_bet_df = \
                data_df[data_df.groupby(by=['hall_id', 'domain_id', 'user_id'])['hall_id'].transform('size') >= 3][
                    ['hall_id', 'domain_id', 'user_id']].drop_duplicates()
            target_bet_df = self.target_bet_df[self.target_bet_df['user_id'].isin(filter_bet_df['user_id'])]

            game_kind_df = target_bet_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'lobby_group']).sum().reset_index()
            top_game_kind_df = game_kind_df.sort_values('commissionable').groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']).tail(1)
            top_game_kind_df['tag_code'] = 0
            top_game_kind_df['tag_type'] = self._tag_type
            top_game_kind_df['tag_enabled'] = True
            for game_kind, tag in tag_code_dict.items():
                top_game_kind_df['tag_code'].loc[top_game_kind_df['lobby_group'] == game_kind] = tag['code']
            top_game_kind_df['tag_type'] = self._tag_type
            top_game_kind_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            game_lover_df = top_game_kind_df[self.ods_data_columns]

        except Exception as e:
            raise

        for game_kind, tag in tag_code_dict.items():
            self.tag_reset(hall, tag['code'])
        return game_lover_df

    def win_lose_reaction(self, hall: Hall) -> pd.DataFrame:  # 輸贏後衝或跑
        print('tag_win_lose_reaction begin')
        try:
            tag_code_list = list(tag_code['code'] for key, tag_code in self._code_dict['win_lose_reaction'].items())

            # 計算每位使用者 實動天數 至少超過3天(包含3天)
            data_df = self.target_bet_df[['hall_id', 'domain_id', 'user_id', 'data_date']].drop_duplicates()
            filter_bet_df = \
                data_df[data_df.groupby(by=['hall_id', 'domain_id', 'user_id'])['hall_id'].transform('size') >= 3][
                    ['hall_id', 'domain_id', 'user_id']].drop_duplicates()
            target_bet_df = self.target_bet_df[self.target_bet_df['user_id'].isin(filter_bet_df['user_id'])]

            user_daily_df = target_bet_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'data_date']).sum().reset_index()
            user_daily_df['commissionable_diff'] = user_daily_df.groupby(['hall_id', 'domain_id', 'user_id'])[
                'commissionable'].diff()
            user_daily_df['payoff_diff'] = user_daily_df.groupby(['hall_id', 'domain_id', 'user_id'])[
                'payoff'].diff()

            tag_conditions = [
                # 贏了就衝
                ((user_daily_df['payoff'] - user_daily_df['payoff_diff']) > 0) & (user_daily_df['commissionable_diff'] > 0),
                # 贏了就跑
                ((user_daily_df['payoff'] - user_daily_df['payoff_diff']) > 0) & (user_daily_df['commissionable_diff'] < 0),
                # 輸了就衝
                ((user_daily_df['payoff'] - user_daily_df['payoff_diff']) < 0) & (user_daily_df['commissionable_diff'] > 0),
                # 輸了就跑
                ((user_daily_df['payoff'] - user_daily_df['payoff_diff']) < 0) & (user_daily_df['commissionable_diff'] < 0)
            ]
            user_daily_df['tag_code'] = np.select(tag_conditions, tag_code_list, default=0)

            user_daily_df = user_daily_df[
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'commissionable', 'tag_code', 'register_date',
                 'data_date']].loc[
                user_daily_df['tag_code'] != 0]
            user_daily_df['tag_count'] = \
                user_daily_df.groupby(by=['hall_id', 'domain_id', 'user_id', 'user_name', 'tag_code', 'register_date'])[
                    'commissionable'].transform('size')
            tag_result_df = user_daily_df.sort_values(['tag_count', 'data_date'], ascending=[True, True]).groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']).tail(1)
            tag_result_df['tag_type'] = self._tag_type
            tag_result_df['tag_enabled'] = True
            tag_result_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            win_lose_df = tag_result_df[self.ods_data_columns]
        except Exception as e:
            raise

        for tag_code in tag_code_list:
            self.tag_reset(hall, tag_code)
        return win_lose_df

    def app_user(self, hall: Hall) -> pd.DataFrame:  # app會員
        print('tag_app_user begin')
        try:
            self.spanner_db.statement = (
                f"""
                SELECT login.hall_id, login.domain_id, login.user_id, member.user_name, 
                {self._code_dict['app_user'][0]['code']} AS tag_code, 
                {self._tag_type} AS tag_type, 
                TRUE AS tag_enabled, 
                member.register_date 
                FROM login_log login 
                JOIN member_info member ON login.hall_id = member.hall_id 
                AND login.domain_id = member.domain_id 
                AND login.user_id = member.user_id 
                AND member.user_id > 0 
                WHERE login.hall_id = {hall.hall_id} AND login.domain_id = {hall.domain_id} AND 
                login.host = 'App' AND data_date = '{self.data_date}' 
                """
            )
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.exec()
            [x.append(spanner.COMMIT_TIMESTAMP) for x in self.spanner_db.fetch_data]
            app_user_df = pd.DataFrame(self.spanner_db.fetch_data, columns=self.ods_data_columns)

        except Exception as e:
            raise

        return app_user_df

    def bet_time(self, hall: Hall, source_df: pd.DataFrame()) -> pd.DataFrame():  # 下注週期標
        print('tag_bet_time begin')
        try:
            weekday_code_dict = self._code_dict['bet_weekday']
            time_code_dict = self._code_dict['bet_time']
            # 週幾標
            bet_weekday_df = source_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'data_weekday']).sum().reset_index()
            top_weekday_df = bet_weekday_df.sort_values(
                ['commissionable', 'data_weekday'], ascending=[True, True]).groupby(
                ['hall_id', 'domain_id', 'user_id']).tail(1)

            top_weekday_df['tag_code'] = 0
            for weekday, tag in weekday_code_dict.items():
                top_weekday_df['tag_code'].loc[top_weekday_df['data_weekday'] == weekday] = tag['code']
            top_weekday_df['tag_type'] = self._tag_type
            top_weekday_df['tag_enabled'] = True
            top_weekday_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            final_weekday_df = top_weekday_df[self.ods_data_columns]
            for weekday, tag in weekday_code_dict.items():
                self.tag_reset(hall, tag['code'])

            # 小時標
            bet_hour_df = source_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'bet_hour']).sum().reset_index()
            top_bet_hour_df = bet_hour_df.sort_values(['commissionable', 'bet_hour'], ascending=[True, True]).groupby(
                ['hall_id', 'domain_id', 'user_id']).tail(1)

            top_bet_hour_df['tag_code'] = 0
            for bet_hour, tag in time_code_dict.items():
                top_bet_hour_df['tag_code'].loc[top_bet_hour_df['bet_hour'] == bet_hour] = tag['code']
            top_bet_hour_df['tag_type'] = self._tag_type
            top_bet_hour_df['tag_enabled'] = True
            top_bet_hour_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            final_bet_hour_df = top_bet_hour_df[self.ods_data_columns]
            for bet_hour, tag in time_code_dict.items():
                self.tag_reset(hall, tag['code'])

            result_df = pd.concat([final_bet_hour_df, final_weekday_df], ignore_index=True)

        except Exception as e:
            raise

        return result_df

    def login_location_browser(self, hall: Hall, tag_code: int) -> pd.DataFrame:  # 登入城市&瀏覽器
        try:
            pass
        except Exception as e:
            raise

        return pd.DataFrame()

    def arbitrage_suspect(self, hall: Hall) -> pd.DataFrame:  # 疑似刷水
        print('tag_arbitrage_suspect begin')
        try:
            tag_code = self._code_dict['arbitrage_suspect'][0]['code']
            bet_df = self.target_bet_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']).sum().reset_index()

            deposit_df = self.target_deposit_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']).sum().reset_index()

            result_df = pd.merge(left=bet_df, right=deposit_df,
                                 on=['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'],
                                 how='outer')
            result_df['tag_enabled'] = np.where((result_df['commissionable'] < (result_df['deposit_amount'] * 2)), True,
                                                False)
            result_df['tag_code'] = tag_code
            result_df['tag_type'] = self._tag_type
            result_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            arbitrage_suspect_df = result_df[self.ods_data_columns].loc[result_df['tag_enabled'] == 1]
            self.tag_reset(hall, tag_code)
        except Exception as e:
            raise

        return arbitrage_suspect_df

    def login_ub(self, hall: Hall) -> pd.DataFrame:  # 寰宇瀏覽器
        print('tag_login_ub begin')
        try:
            self.spanner_db.statement = (
                f"""
                SELECT login.hall_id, login.domain_id, login.user_id, member.user_name, 
                {self._code_dict['login_ub'][0]['code']} AS tag_code, 
                {self._tag_type} AS tag_type, 
                TRUE AS tag_enabled, 
                member.register_date 
                FROM login_log login 
                JOIN member_info member ON login.hall_id = member.hall_id 
                AND login.domain_id = member.domain_id 
                AND login.user_id = member.user_id 
                AND member.user_id > 0 
                WHERE login.hall_id = {hall.hall_id} AND login.domain_id = {hall.domain_id} 
                AND login.host ='UB' AND data_date = '{self.data_date}' 
                """
            )
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.exec()
            [x.append(spanner.COMMIT_TIMESTAMP) for x in self.spanner_db.fetch_data]
            login_ub_df = pd.DataFrame(self.spanner_db.fetch_data, columns=self.ods_data_columns)
        except Exception as e:
            raise

        return login_ub_df

    def login_pc(self, hall: Hall) -> pd.DataFrame:  # PC登入
        print('tag_login_pc begin')
        try:
            self.spanner_db.statement = (
                f"""
                SELECT login.hall_id, login.domain_id, login.user_id, member.user_name, 
                {self._code_dict['login_pc'][0]['code']} AS tag_code, 
                {self._tag_type} AS tag_type, 
                TRUE AS tag_enabled, 
                member.register_date 
                FROM login_log login 
                JOIN member_info member ON login.hall_id = member.hall_id 
                AND login.domain_id = member.domain_id 
                AND login.user_id = member.user_id 
                AND member.user_id > 0 
                WHERE login.hall_id = {hall.hall_id} AND login.domain_id = {hall.domain_id} 
                AND login.host ='PC' AND data_date = '{self.data_date}' 
                """
            )
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.exec()
            [x.append(spanner.COMMIT_TIMESTAMP) for x in self.spanner_db.fetch_data]
            login_pc_df = pd.DataFrame(self.spanner_db.fetch_data, columns=self.ods_data_columns)
        except Exception as e:
            raise

        return login_pc_df

    def continuous_deposit(self, hall: Hall) -> pd.DataFrame:  # 近連三天存款
        print('tag_continuous_deposit begin')
        try:
            tag_code = self._code_dict['continuous_deposit'][0]['code']
            data_range = (self.target_deposit_df['data_date'] >= str(self.data_date - timedelta(days=2))) & (
                        self.target_deposit_df['data_date'] <= str(self.data_date))
            recent_deposit_df = self.target_deposit_df.loc[data_range]
            match_user = recent_deposit_df['user_id'].value_counts()
            match_user_df = match_user.reset_index(drop=False).rename(columns={'user_id': 'count', 'index': 'user_id'})
            match_user_df = match_user_df.loc[match_user_df['count'] == 3]

            deposit_df = recent_deposit_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']).sum().reset_index()
            deposit_df['tag_enabled'] = True
            deposit_df['tag_code'] = tag_code
            deposit_df['tag_type'] = self._tag_type
            deposit_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            continuous_deposit_df = pd.merge(left=match_user_df, right=deposit_df, on=['user_id'], how='left')
            continuous_deposit_df = continuous_deposit_df[self.ods_data_columns]
            self.tag_reset(hall, tag_code)
        except Exception as e:
            raise

        return continuous_deposit_df

    def deposit_method(self, hall: Hall) -> pd.DataFrame:  # 存款方式
        print('tag_deposit_method begin')
        try:
            tag_dict = self._code_dict['deposit_method']
            method_tag_result_df = pd.DataFrame()
            sum_amount_df = self.target_cash_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'opcode'])['amount'].sum().reset_index()
            for index, tag in tag_dict.items():
                tag_code = tag['code']
                opcode_list = tag['opcode_list']
                if type(opcode_list[0]) == str:
                    optype_df = self.target_opcode_df.loc[self.target_opcode_df['type'] == opcode_list[0]]
                    opcode_list = optype_df['opcode'].values.tolist()
                    tag_code_df = sum_amount_df[sum_amount_df['opcode'].isin(opcode_list)]
                else:
                    tag_code_df = sum_amount_df[sum_amount_df['opcode'].isin(opcode_list)]
                tag_code_df = tag_code_df.groupby(
                    ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'])['amount'].sum().reset_index()
                tag_code_df['tag_code'] = tag_code
                method_tag_result_df = pd.concat([method_tag_result_df, tag_code_df], ignore_index=True)

            method_tag_result_df = method_tag_result_df.sort_values(by='amount', ascending=True).groupby(
                ['hall_id', 'domain_id', 'user_id']).tail(1)
            method_tag_result_df = method_tag_result_df[
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'tag_code', 'register_date'
                 ]].loc[method_tag_result_df['amount'] > 0]
            method_tag_result_df['tag_enabled'] = True
            method_tag_result_df['tag_type'] = self._tag_type
            method_tag_result_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            top_tag_code_df = df_type_format_tag(method_tag_result_df)
            for index, tag in tag_dict.items():
                self.tag_reset(hall, tag['code'])

        except Exception as e:
            raise
        return top_tag_code_df

    def deposit_latest_average_record(self, hall: Hall) -> pd.DataFrame:  # 近一實動日單筆入款
        print('tag_deposit_latest_average_record begin')
        try:
            tag_dict = self._code_dict['deposit_latest_average_record']
            top_deposit_date_df = self.target_deposit_df.sort_values('data_date').groupby(
                ['hall_id', 'domain_id', 'user_id']).tail(1)

            top_deposit_date_df['tag_code'] = 0
            top_deposit_date_df['average_amount'] = top_deposit_date_df['deposit_amount'] / top_deposit_date_df['deposit_count']
            for amount, tag in tag_dict.items():
                top_deposit_date_df['tag_code'].loc[top_deposit_date_df['average_amount'] >= amount] = tag['code']
            top_deposit_date_df = top_deposit_date_df[
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'tag_code', 'register_date'
                 ]].loc[top_deposit_date_df['tag_code'] != 0]
            top_deposit_date_df['tag_enabled'] = True
            top_deposit_date_df['tag_type'] = self._tag_type
            top_deposit_date_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            for amount, tag in tag_dict.items():
                self.tag_reset(hall, tag['code'])
        except Exception as e:
            raise
        return top_deposit_date_df

    def max_bet_per_game(self, hall: Hall) -> pd.DataFrame:  # 近15實動日單日單款遊戲最大有效投注金額
        print('tag_max_bet_per_game begin')
        try:
            tag_dict = self._code_dict['max_bet_per_game']
            top_bet_amount_df = self.target_bet_df.sort_values('commissionable').groupby(
                ['hall_id', 'domain_id', 'user_id']).tail(1)
            top_bet_amount_df['tag_code'] = 0
            for amount, tag in tag_dict.items():
                top_bet_amount_df['tag_code'].loc[top_bet_amount_df['commissionable'] >= amount] = tag['code']
            top_bet_amount_df = top_bet_amount_df[
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'tag_code', 'register_date'
                 ]].loc[top_bet_amount_df['tag_code'] != 0]
            # print(top_bet_amount_df.groupby('tag_code').count())
            top_bet_amount_df['tag_enabled'] = True
            top_bet_amount_df['tag_type'] = self._tag_type
            top_bet_amount_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            for bet_amount, tag in tag_dict.items():
                self.tag_reset(hall, tag['code'])
        except Exception as e:
            raise
        return top_bet_amount_df

    def week_valid_bet_descend(self, hall: Hall) -> pd.DataFrame:  # 本週與上週 有效投注下降幅度
        print('tag_week_valid_bet_descend begin')
        try:
            tag_dict = self._code_dict['week_valid_bet_descend']
            last_week_range = (self.target_bet_df['data_date'] >= str(self.data_date - timedelta(days=13))) & (
                    self.target_bet_df['data_date'] <= str(self.data_date - timedelta(days=7)))
            last_week_bet_df = self.target_bet_df.loc[last_week_range]
            last_week_bet_df = last_week_bet_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']).sum().reset_index()
            last_week_bet_df = last_week_bet_df.rename(columns={'commissionable': 'last_sum_valid_bet'})
            last_week_bet_df = last_week_bet_df[
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'last_sum_valid_bet']]

            this_week_range = (self.target_bet_df['data_date'] >= str(self.data_date - timedelta(days=6))) & (
                    self.target_bet_df['data_date'] <= str(self.data_date))
            this_week_bet_df = self.target_bet_df.loc[this_week_range]
            this_week_bet_df = this_week_bet_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']).sum().reset_index()
            this_week_bet_df = this_week_bet_df.rename(columns={'commissionable': 'this_sum_valid_bet'})
            this_week_bet_df = this_week_bet_df[
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'this_sum_valid_bet']]

            result_df = pd.merge(left=last_week_bet_df, right=this_week_bet_df,
                                 on=['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'],
                                 how='left')
            result_df['valid_bet_diff'] = result_df['last_sum_valid_bet'] - result_df['this_sum_valid_bet']
            result_df['tag_code'] = 0
            for amount, tag in tag_dict.items():
                result_df['tag_code'].loc[result_df['valid_bet_diff'] >= amount] = tag['code']

            result_df = result_df[
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'tag_code', 'register_date'
                 ]].loc[result_df['tag_code'] != 0]
            # print(result_df.groupby('tag_code').count())
            result_df['tag_enabled'] = True
            result_df['tag_type'] = self._tag_type
            result_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            for valid_bet_amount, tag in tag_dict.items():
                self.tag_reset(hall, tag['code'])
        except Exception as e:
            raise
        return result_df

    def tag_of_disjoint(self, hall: Hall) -> pd.DataFrame:  # bad_tag priority > good_tag priority
        print('tag_of_disjoint begin')
        bad_tag_dict = {
            40001: '壞壞客',
            40006: 'AG視訊疑似對打客', 40007: 'AG視訊對打客', 40008: 'BB視訊疑似對打客', 40009: 'BB視訊對打客',
            40010: 'XBB視訊疑似對打客', 40011: 'XBB視訊對打客', 40012: 'EVO視訊疑似對打客', 40013: 'EVO視訊對打客',
            # 40032: '疑似壞壞客',
        }
        bad_tag_list = ",".join(str(key) for key in list(bad_tag_dict.keys()))

        good_tag_dict = {
            30015: '好客',
        }
        good_tag_list = ",".join(str(key) for key in list(good_tag_dict.keys()))

        bad_tag_user_df = self.spanner_db.select_data_with(
            columns=['hall_id', 'domain_id', 'user_id'],
            statement=
            f"""
            SELECT hall_id, domain_id, user_id 
            FROM user_tag_ods_data 
            WHERE 
            hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} 
            AND tag_code IN ({bad_tag_list}) AND tag_enabled = True 
            """
        )
        bad_tag_user_df = bad_tag_user_df.drop_duplicates()

        good_tag_user_df = self.spanner_db.select_data_with(
            columns=['hall_id', 'domain_id', 'user_id', 'tag_code'],
            statement=
            f"""
            SELECT hall_id, domain_id, user_id, tag_code 
            FROM user_tag_ods_data 
            WHERE 
            hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} 
            AND tag_code IN ({good_tag_list}) AND tag_enabled = True 
            """
        )
        good_bad_intersection_df = pd.merge(
            left=good_tag_user_df, right=bad_tag_user_df,
            how='inner', on=['hall_id', 'domain_id', 'user_id'])
        good_bad_intersection_df['tag_enabled'] = False
        good_bad_intersection_df['updated_time'] = spanner.COMMIT_TIMESTAMP
        return good_bad_intersection_df

    def hesitate(self, hall: Hall, bq_db: BQ_SSR) -> pd.DataFrame:  # 猶豫客
        print('tag_hesitate begin')
        try:
            tag_code = self._code_dict['hesitate'][0]['code']
            deposit_user_latest_date_df = self.target_deposit_df.sort_values('data_date').groupby(['hall_id', 'domain_id', 'user_id']).tail(1)
            deposit_user_latest_date_df = deposit_user_latest_date_df[['hall_id', 'domain_id', 'user_id', 'data_date']].rename(columns={'data_date': 'latest_deposit_date'})
            bet_user_latest_date_df = self.target_bet_df.sort_values('data_date').groupby(['hall_id', 'domain_id', 'user_id']).tail(1)
            bet_user_latest_date_df = bet_user_latest_date_df[['hall_id', 'domain_id', 'user_id', 'data_date']].rename(columns={'data_date': 'latest_bet_date'})

            # 從來未存款，近二週內登入三次以上
            deposit_user_df = self.target_deposit_df.groupby(
                ['hall_id', 'domain_id', 'user_id'])['deposit_count', 'deposit_amount'].sum().reset_index()

            login_two_week_3_times_df = self.spanner_db.select_data_with(
                ['hall_id', 'domain_id', 'user_id', 'login_count', 'user_name', 'register_date'],
                f"""
                SELECT login.hall_id, login.domain_id, login.user_id, SUM(login.login_count), 
                member.user_name, member.register_date
                FROM login_log login
                JOIN member_info member
                ON login.hall_id = member.hall_id AND login.domain_id = member.domain_id AND login.user_id = member.user_id
                WHERE
                login.hall_id = {hall.hall_id} AND login.domain_id = {hall.domain_id}
                AND login.data_date >= '{self.data_date - timedelta(days=14)}'
                GROUP BY login.hall_id, login.domain_id, login.user_id, member.user_name, member.register_date
                HAVING SUM(login.login_count) >= 3
                """
            )
            deposit_df = pd.merge(
                left=login_two_week_3_times_df, right=deposit_user_df,
                on=['hall_id', 'domain_id', 'user_id'],
                how='left'
            )
            deposit_df.fillna(value=0, inplace=True)
            login_no_deposit_df = deposit_df.loc[deposit_df['deposit_count'] == 0][
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']].drop_duplicates()

            # 從未存款，但兩週內有看過存款頁三次以上
            self.get_deposit_page_path(hall, 14)
            deposit_page_path_df = self.target_deposit_page_path_df
            deposit_page_path_df['visit_count'] = 1
            deposit_page_path_df = deposit_page_path_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'])['visit_count'].sum().reset_index()
            deposit_df = pd.merge(
                left=deposit_page_path_df, right=deposit_user_df,
                on=['hall_id', 'domain_id', 'user_id'],
                how='left'
            )
            deposit_df.fillna(value=0, inplace=True)
            visit_but_no_deposit_df = \
            deposit_df.loc[(deposit_df['deposit_count'] == 0) & (deposit_df['visit_count'] >= 3)][
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']].drop_duplicates()

            # 曾經有存款，倒數第五次的登入後無下注及存款
            login_top5_day_df = bq_db.select_data_with(
                f"""
                SELECT {hall.hall_id} AS hall_id, {hall.domain_id} AS domain_id, user_id, data_date FROM (
                  SELECT user_id, ARRAY_AGG(data_date ORDER BY data_date DESC LIMIT 5) arr
                  FROM (
                      SELECT user_id, data_date 
                      FROM `{bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.login_log` 
                      WHERE result = 1 GROUP BY user_id, data_date
                  ) GROUP BY user_id
                ), UNNEST(arr) data_date
                """
            )

            login_top5_oldest_day = login_top5_day_df.sort_values('data_date').groupby(['hall_id', 'domain_id', 'user_id']).head(1)
            login_top5_oldest_day = login_top5_oldest_day.rename(columns={'data_date': 'oldest_date'})

            login_deposit_df = pd.merge(
                left=login_top5_oldest_day, right=deposit_user_latest_date_df,
                on=['hall_id', 'domain_id', 'user_id'],
                how='left'
            )

            login_activated_df = pd.merge(
                left=login_deposit_df, right=bet_user_latest_date_df,
                on=['hall_id', 'domain_id', 'user_id'],
                how='left'
            )
            login_activated_df['oldest_date'] = pd.to_datetime(login_activated_df['oldest_date'], format='%Y-%m-%d')
            login_activated_df['latest_deposit_date'] = pd.to_datetime(login_activated_df['latest_deposit_date'], format='%Y-%m-%d')
            login_activated_df['latest_bet_date'] = pd.to_datetime(login_activated_df['latest_bet_date'], format='%Y-%m-%d')
            login_no_activated_df = login_activated_df.loc[
                (login_activated_df['oldest_date'] >= login_activated_df['latest_deposit_date']) & (
                        login_activated_df['oldest_date'] >= login_activated_df['latest_bet_date'])].drop_duplicates()

            user_id_condition = ",".join(str(key) for key in login_no_activated_df['user_id'].values.tolist())

            login_no_activated_df = self.spanner_db.select_data_with(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'],
                f"""
                SELECT hall_id, domain_id, user_id, user_name, register_date
                FROM member_info member 
                WHERE
                hall_id = {hall.hall_id} AND domain_id = {hall.domain_id}
                AND user_id IN ({user_id_condition})
                """
            )
            # 條件取聯集
            result_df = pd.concat([login_no_deposit_df, login_no_activated_df, visit_but_no_deposit_df], ignore_index=True)
            result_df['tag_code'] = tag_code
            result_df['tag_enabled'] = True
            result_df['tag_type'] = self._tag_type
            result_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            self.tag_reset(hall, tag_code)

        except Exception as e:
            raise
        return result_df

    def certification(self, hall: Hall) -> pd.DataFrame:  # 實名驗證
        pass

    def term(self, hall) -> pd.DataFrame:  # 短中長期客
        print('tag_term begin')
        try:
            user_total_valid_bet_df = self.target_bet_df.groupby(['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'])['commissionable'].sum().reset_index()
            user_total_valid_bet_df = user_total_valid_bet_df.loc[user_total_valid_bet_df['commissionable'] > 0]
            user_total_valid_bet_df['today'] = pd.to_datetime(str(date.today()))
            user_total_valid_bet_df['today'] = user_total_valid_bet_df['today'].dt.tz_localize('UTC', ambiguous='infer')
            tag_code_list = list(tag_code['code'] for key, tag_code in self._code_dict['term'].items())

            tag_conditions = [
                # 新會員
                (((user_total_valid_bet_df['today'] - user_total_valid_bet_df['register_date']) / np.timedelta64(1, 'D')) < 30),
                # 初期會員
                (((user_total_valid_bet_df['today'] - user_total_valid_bet_df['register_date']) / np.timedelta64(1, 'D')) < 90),
                # 中期會員
                (((user_total_valid_bet_df['today'] - user_total_valid_bet_df['register_date']) / np.timedelta64(1, 'D')) < 180),
                # 長期會員
                (((user_total_valid_bet_df['today'] - user_total_valid_bet_df['register_date']) / np.timedelta64(1, 'D')) >= 180),
            ]
            user_total_valid_bet_df['tag_code'] = np.select(tag_conditions, tag_code_list, default=0)
            user_total_valid_bet_df['tag_enabled'] = True
            user_total_valid_bet_df['tag_type'] = self._tag_type
            user_total_valid_bet_df['updated_time'] = spanner.COMMIT_TIMESTAMP
            result_df = user_total_valid_bet_df[self.ods_data_columns]
            for tag_code in tag_code_list:
                self.tag_reset(hall, tag_code)
        except Exception as e:
            raise
        return result_df