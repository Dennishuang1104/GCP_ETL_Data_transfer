from google.cloud import spanner
import os
import pandas as pd
import numpy as np
import time
import pytz
from datetime import datetime, timedelta
import Environment
from Adaptors.Hall import Hall
from Adaptors.BQAdaptor import BQ_BaseBBIN
from Adaptors.SpannerAdaptor import CDP_BBIN, CDP_Config ,CDP_XBB_BBIN
from Adaptors.CloudStorageAdaptor import GCS
from Adaptors.SpannerAdaptor import RuleTag, NAPL
from util.dataframe_process import df_timezone_format, df_type_format, df_type_format_tag
from Adaptors.TelegramAdaptor import Notify
from util.ip_database import search_ip_multiple

notify_bot = Notify()



def try_exc_func(func):
    def wrap_func(*args, **kwargs):
        exec_start_time = datetime.now()
        try:
            print(f'{func.__name__} begin...')
            func(*args, **kwargs)
        except Exception as e:
            notify_bot.message = (
                f"CDP ETL Demo: {kwargs['hall'].domain_name}, {func.__name__} error\n"
                f"Error Message: {e} "
            )
            notify_bot.send_message()
            raise
        finally:
            exec_end_time = datetime.now()
            difference = exec_end_time - exec_start_time
            timedelta(0, 8, 562000)
            seconds_in_day = 24 * 60 * 60
            result_time = divmod(difference.days * seconds_in_day + difference.seconds, 60)
            print(
                f"{kwargs['hall'].domain_name} {func.__name__} exec time: {result_time[0]} minutes, {result_time[1]} seconds")

    return wrap_func


class ETL_Spanner_BBIN_Demo_Service(CDP_BBIN):
    def __init__(self, spanner_db: str, bq_db):
        super(ETL_Spanner_BBIN_Demo_Service, self).__init__(database=spanner_db)
        self.bq_db = bq_db
        self.sv_begin_time = None
        self.sv_end_time = None
        self.telegram_message = ''

    def member_info(self, hall: Hall, start, end):
        # 將datetime加回12小時，以台北時間直接insert spanner，會自動轉為UTC時區
        self.bq_db.statement = (
            "SELECT user_id, "
            "parent_username AS ag_name, "
            "username AS user_name, "
            "name AS name_real, "
            "DATETIME_ADD(created_at, INTERVAL 12 HOUR) AS register_date, "
            "phone AS user_phone, "
            "email AS user_mail, "
            "balance, "
            "DATETIME_ADD(last_login, INTERVAL 12 HOUR) AS last_login, "
            "last_ip, "
            "qq_num, "
            "wechat, "
            "level_id AS user_level_id, "
            "level_name AS user_level "
            f"FROM `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.member_info`  "
            f"WHERE hall_id = {hall.hall_id} "
            f"AND (DATE(created_at) BETWEEN '{start}' AND '{end}' "
            f"OR "
            f"DATE(created_time) BETWEEN '{start}' AND '{end}') "
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data
        if len(insert_df) > 0:
            # print(f'member count:{len(insert_df)}')
            insert_df = df_timezone_format(insert_df, 'Asia/Taipei')  # 設定時區
            insert_df['last_login'] = insert_df['last_login'].fillna(pd.Timestamp.min)
            insert_df['register_date'] = insert_df['register_date'].fillna(pd.Timestamp.min)
            insert_df['user_level_id'] = insert_df['user_level_id'].fillna(0)
            insert_df['user_level_id'] = insert_df['user_level_id'].astype(int)
            insert_df = df_type_format(insert_df)  # 設定型態
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', 0)
            insert_df.insert(3, 'domain_name', None)
            self.upsert_data_with('member_info', insert_df)


    @try_exc_func
    def login_log(self, hall: Hall, start, end):
        self.bq_db.statement = (
            "SELECT "
            "  login.user_id,"
            "  login.parent_username AS ag_name, "
            "  COUNT(login.user_id) AS login_count, "
            "  CASE "
            "    WHEN login.ingress IN (11, 12, 13, 14, 15, 16) THEN 'App' "
            "    WHEN login.ingress = 1 THEN 'PC' "
            "    WHEN login.ingress IN (7, 8) THEN 'UB' "
            "  ELSE "
            "  'Web' "
            "END "
            "  AS host, "
            "  login.data_date, "
            "  financial_year, "
            "  financial_month, "
            "  financial_week "
            "FROM "
            f"  `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.login_log` login "
            "WHERE "
            "  1=1 "
            f"  AND login.hall_id = {hall.hall_id} "
            f"  AND login.data_date BETWEEN '{start}' AND '{end}' "
            "GROUP BY "
            "  login.user_id,"
            "  login.parent_username, "
            "  login.ingress, "
            "  login.data_date, "
            "  financial_year, "
            "  financial_month, "
            "  financial_week "
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data
        insert_df = df_type_format(insert_df)  # 設定型態
        if len(insert_df) > 0:
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', 0)
            insert_df.insert(3, 'domain_name', None)
            insert_df['data_date'] = insert_df['data_date'].fillna(0)
            insert_df['data_date'] = insert_df['data_date'].astype(str)
            self.replace_data_with('login_log', insert_df)

    @try_exc_func
    def game_dict(self, hall: Hall):
        self.bq_db.statement = (
            f"""
            SELECT
              vendor.lobby,
              vendor.tc_name AS lobby_name,
              vendor.sc_name AS lobby_name_cn,
              vendor.en_name AS lobby_name_en,
              game.lobby_group,
              (CASE
                  WHEN game.lobby_group = 1 THEN 'sport'
                  WHEN game.lobby_group = 2 THEN 'lottery'
                  WHEN game.lobby_group = 3 THEN 'live'
                  WHEN game.lobby_group = 5 THEN 'prob'
                  WHEN game.lobby_group = 6 THEN 'card'
              END
                ) AS lobby_group_name,
              game.game_code AS game_type,
              game.tc_name AS game_type_name,
              game.sc_name AS game_type_name_cn,
              game.en_name AS game_type_name_en,
              CASE
                WHEN game.tc_name LIKE '%百家%' THEN 1
                WHEN game.tc_name LIKE '%輪盤%' THEN 1
              ELSE
              0
            END
              AS is_pvp,
              CASE
                WHEN game.tc_name LIKE '%百家%' THEN 1
                WHEN game.tc_name LIKE '%21%' THEN 1
                WHEN game.tc_name LIKE '%二十一%' THEN 1
              ELSE
              0
            END
              AS is_pve
            FROM
            `{self.bq_db.project_id}.general_information_bbin.Game_Code_View` game 
            JOIN 
            `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.Lobby_View` vendor 
            ON
              game.lobby = vendor.lobby
              AND vendor.hall_id = {hall.hall_id}
        """
        )

        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data
        if len(insert_df) > 0:
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', 0)
            insert_df.insert(3, 'domain_name', None)
            insert_df['is_pvp'] = insert_df['is_pvp'].astype(bool)
            insert_df['is_pve'] = insert_df['is_pve'].astype(bool)
            self.replace_data_with('game_type_dict', insert_df)

    @try_exc_func
    def bet_analysis(self, hall: Hall, start, end):
        self.bq_db.statement = (
            f"""
            SELECT
              bet.user_id,
              bet.parent_username AS ag_name,
              bet.lobby,
              bet.lobby_name,
              vendor.sc_name AS lobby_name_cn,
              vendor.en_name AS lobby_name_en,
              IFNULL(bet.lobby_group, 0) AS lobby_group,
              bet.lobby_group_name,
              bet.game_code AS game_type,
              bet.game_name,
              game.sc_name AS game_name_cn,
              game.en_name AS game_name_en,
              SUM(bet.wagers_total) AS wagers_total,
              SUM(bet.bet) AS bet_amount,
              SUM(bet.valid_bet) AS commissionable,
              SUM(bet.payoff) AS payoff,
              AVG(bet.win_rate) AS win_rate,
              bet.platform,
              bet.data_date,
              bet.financial_year,
              bet.financial_month,
              bet.financial_week,
              IFNULL(bet.level_id,
                0) AS user_level_id,
              bet.level_name AS user_level
            FROM
              `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.bet_analysis` bet
            LEFT JOIN
              `{self.bq_db.project_id}.general_information_bbin.Game_Code_View` game
            ON
              bet.lobby = game.lobby AND bet.game_code = game.game_code
            LEFT JOIN
              `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.Lobby_View` vendor
            ON
              game.lobby = vendor.lobby
              AND vendor.hall_id = {hall.hall_id}
            WHERE
              1=1
              AND bet.hall_id = {hall.hall_id}
              AND data_date BETWEEN '{start}' AND '{end}'
            GROUP BY
              bet.user_id, bet.parent_username,
              bet.lobby, bet.lobby_name, vendor.sc_name, vendor.en_name,
              bet.lobby_group, bet.lobby_group_name,
              bet.game_name, game.sc_name, game.en_name, bet.game_code,
              bet.platform,
              bet.data_date, bet.financial_year, bet.financial_month, bet.financial_week,
              bet.level_id, bet.level_name
            """
        )
        # print(self.bq_db.statement)
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data
        # print(self.bq_db.statement)

        if len(insert_df) > 0:
            insert_df = insert_df.fillna(np.nan).replace([np.nan], [None])
            insert_df = df_type_format(insert_df)  # 設定型態
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', 0)
            insert_df.insert(3, 'domain_name', None)
            insert_df.insert(18, 'platform_name', '')
            for platform, platform_name in Environment.platform_name_dict.items():
                insert_df.loc[insert_df['platform'] == platform, 'platform_name'] = platform_name
            self.column_times_setter(column_times=2)
            insert_df['bet_amount'] = insert_df['bet_amount'].fillna(0)
            insert_df['bet_amount'] = insert_df['bet_amount'].astype(str)
            insert_df['commissionable'] = insert_df['commissionable'].fillna(0)
            insert_df['commissionable'] = insert_df['commissionable'].astype(str)
            insert_df['payoff'] = insert_df['payoff'].fillna(0)
            insert_df['payoff'] = insert_df['payoff'].astype(str)
            insert_df['data_date'] = insert_df['data_date'].fillna(0)
            insert_df['data_date'] = insert_df['data_date'].astype(str)
            self.replace_data_with('bet_analysis', insert_df)

    @try_exc_func
    def deposit_withdraw(self, hall: Hall, start, end):
        self.bq_db.statement = (
            f"""
            SELECT
              deposit.user_id,
              deposit.parent_username AS ag_name,
              IFNULL(SUM(deposit.amount), 0) AS deposit_amount,
              IFNULL(SUM(deposit.deposit_count), 0) AS deposit_count,
              IFNULL(SUM(withdraw.real_amount), 0) AS withdraw_amount,
              IFNULL(SUM(withdraw.withdraw_count), 0) AS withdraw_count,
              deposit.data_date,
              deposit.financial_year,
              deposit.financial_month,
              deposit.financial_week,
              deposit.level_id AS user_level_id,
              deposit.level_name AS user_level
            FROM (
              SELECT
                hall_id,
                user_id,
                parent_username,
                SUM(amount) AS amount,
                SUM(deposit_count) AS deposit_count,
                level_id,
                level_name,
                data_date,
                financial_year,
                financial_month,
                financial_week
              FROM
                `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.deposit_record`
              WHERE
                status <> 0
                AND hall_id = {hall.hall_id}
                AND data_date BETWEEN '{start}' AND '{end}'
                AND parent_username IS NOT NULL
              GROUP BY
                hall_id,
                user_id,
                parent_username,
                level_id,
                level_name,
                data_date,
                financial_year,
                financial_month,
                financial_week ) deposit
            LEFT JOIN (
              SELECT
                hall_id,
                user_id,
                parent_username,
                SUM(real_amount) AS real_amount,
                SUM(withdraw_count) AS withdraw_count,
                data_date
              FROM
                `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.withdraw_record`
              WHERE
                status <> 0
                AND hall_id = {hall.hall_id}
                AND data_date BETWEEN '{start}' AND '{end}'
                AND parent_username IS NOT NULL
              GROUP BY
                hall_id,
                user_id,
                parent_username,
                data_date ) withdraw
            ON
              deposit.hall_id = withdraw.hall_id
              AND deposit.user_id = withdraw.user_id
              AND deposit.data_date = withdraw.data_date
            GROUP BY
              deposit.user_id,
              deposit.parent_username,
              deposit.data_date,
              deposit.financial_year,
              deposit.financial_month,
              deposit.financial_week,
              deposit.level_id,
              deposit.level_name
            UNION DISTINCT
            SELECT
              withdraw.user_id,
              withdraw.parent_username AS ag_name,
              IFNULL(SUM(deposit.amount), 0) AS deposit_amount,
              IFNULL(SUM(deposit.deposit_count), 0) AS deposit_count,
              IFNULL(SUM(withdraw.real_amount), 0) AS withdraw_amount,
              IFNULL(SUM(withdraw.withdraw_count), 0) AS withdraw_count,
              withdraw.data_date,
              withdraw.financial_year,
              withdraw.financial_month,
              withdraw.financial_week,
              withdraw.level_id AS user_level_id,
              withdraw.level_name AS user_level
            FROM (
              SELECT
                hall_id,
                user_id,
                parent_username,
                SUM(amount) AS amount,
                SUM(deposit_count) AS deposit_count,
                data_date
              FROM
                `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.deposit_record`
              WHERE
                status <> 0
                AND hall_id = {hall.hall_id}
                AND data_date BETWEEN '{start}' AND '{end}'
                AND parent_username IS NOT NULL
              GROUP BY
                hall_id,
                user_id,
                parent_username,
                data_date) deposit
            RIGHT JOIN (
              SELECT
                hall_id,
                user_id,
                parent_username,
                SUM(real_amount) AS real_amount,
                SUM(withdraw_count) AS withdraw_count,
                level_id,
                level_name,
                data_date,
                financial_year,
                financial_month,
                financial_week
              FROM
                `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.withdraw_record`
              WHERE
                status <> 0
                AND hall_id = {hall.hall_id}
                AND data_date BETWEEN '{start}' AND '{end}'
                AND parent_username IS NOT NULL
              GROUP BY
                hall_id,
                user_id,
                parent_username,
                level_id,
                level_name,
                data_date,
                financial_year,
                financial_month,
                financial_week ) withdraw
            ON
              deposit.hall_id = withdraw.hall_id
              AND deposit.user_id = withdraw.user_id
              AND withdraw.data_date = deposit.data_date
            WHERE
              withdraw.hall_id = {hall.hall_id}
              AND withdraw.data_date BETWEEN '{start}' AND '{end}'
              AND withdraw.parent_username IS NOT NULL
            GROUP BY
              withdraw.user_id,
              withdraw.parent_username,
              withdraw.data_date,
              withdraw.financial_year,
              withdraw.financial_month,
              withdraw.financial_week,
              withdraw.level_id,
              withdraw.level_name
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data
        if len(insert_df) > 0:
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', 0)
            insert_df.insert(3, 'domain_name', None)
            insert_df['user_level_id'] = insert_df['user_level_id'].fillna(0)
            insert_df = df_type_format(insert_df)  # 設定型態
            self.column_times_setter(column_times=2)
            insert_df['data_date'] = insert_df['data_date'].fillna(0)
            insert_df['data_date'] = insert_df['data_date'].astype(str)
            insert_df['user_level_id'] = insert_df['user_level_id'].fillna(0)
            insert_df['user_level_id'] = insert_df['user_level_id'].astype(int)
            self.replace_data_with('deposit_withdraw_record', insert_df)

    @try_exc_func
    def offer(self, hall: Hall, start, end):
        self.bq_db.statement = (
            f"""
            SELECT offer.user_id, 
            offer.parent_username AS ag_name, 
            opcode, opcode_name, premium_total, premium_amount, 
            data_date, financial_year, financial_month, financial_week, 
            offer.level_id AS user_level_id, offer.level_name AS user_level 
            FROM `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.offer_info` offer 
            WHERE offer.hall_id = {hall.hall_id} 
            AND offer.data_date BETWEEN '{start}' AND '{end}'
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data
        if len(insert_df) > 0:
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', 0)
            insert_df.insert(3, 'domain_name', None)
            insert_df['user_level_id'] = insert_df['user_level_id'].fillna(0)
            insert_df = df_type_format(insert_df)  # 設定型態
            insert_df['user_level_id'] = insert_df['user_level_id'].astype(int)
            insert_df['data_date'] = insert_df['data_date'].fillna(0)
            insert_df['data_date'] = insert_df['data_date'].astype(str)
            self.replace_data_with('offer_info', insert_df)

    @try_exc_func
    def profit_loss(self, hall: Hall, start, end):
        self.bq_db.statement = (
            f"""
            SELECT user_id, SUM(premium_amount) AS premium_amount, 
            data_date, financial_year, financial_month, financial_week 
            FROM `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.offer_info` offer 
            WHERE offer.hall_id = {hall.hall_id} 
            AND data_date BETWEEN '{start}' AND '{end}' 
            GROUP BY user_id, data_date, financial_year, financial_month, financial_week 
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        offer_df = self.bq_db.fetch_data

        self.bq_db.statement = (
            f"""
            SELECT user_id, SUM(payoff) AS payoff, 
            data_date, financial_year, financial_month, financial_week 
            FROM `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.bet_analysis` bet 
            WHERE bet.hall_id = {hall.hall_id} 
            AND data_date BETWEEN '{start}' AND '{end}' 
            GROUP BY user_id, data_date, financial_year, financial_month, financial_week 
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        payoff_df = self.bq_db.fetch_data

        left_df = pd.merge(left=payoff_df, right=offer_df,
                           on=['user_id', 'data_date', 'financial_year', 'financial_month', 'financial_week'],
                           how='left')
        right_df = pd.merge(left=payoff_df, right=offer_df,
                            on=['user_id', 'data_date', 'financial_year', 'financial_month', 'financial_week'],
                            how='right')
        union_df = pd.merge(left=left_df, right=right_df,
                            on=['user_id', 'data_date', 'financial_year', 'financial_month', 'financial_week'],
                            how='outer')
        insert_df = union_df.drop(columns=['payoff_y', 'premium_amount_x']).rename(
            columns={'payoff_x': 'payoff', 'premium_amount_y': 'premium_amount'})
        if len(insert_df) > 0:
            insert_df['payoff'].fillna(0, inplace=True)
            insert_df['premium_amount'].fillna(0, inplace=True)
            insert_df['profit_loss'] = - insert_df['premium_amount'] - insert_df['payoff']
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', 0)
            insert_df.insert(3, 'domain_name', None)
            self.column_times_setter(column_times=2)
            self.replace_data_with('profit_loss', insert_df)

    @try_exc_func
    def vip_login(self, hall: Hall, start, end):
        print('vip_login starting')
        vip_df = self.select_data_with(
            ['hall_id', 'user_id'],
            "SELECT hall_id, user_id "
            "FROM user_tag_ods_data "
            "WHERE tag_code = 10001 AND tag_enabled = TRUE "
        )
        if len(vip_df) > 0:
            vip_list = vip_df['user_id'].values.tolist()
            vip_user_condition_str = ",".join(str(user_id) for user_id in vip_list)
            self.bq_db.statement = (
                "SELECT login.user_id, member.username AS user_name, "
                "EXTRACT(HOUR FROM login.at) AS login_hour, "
                "count(login.user_id) AS login_count, login.data_date,"
                "login.financial_year, login.financial_month, login.financial_week "
                f"FROM `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.login_log` login "
                f"JOIN `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.User_View` member "
                "ON login.hall_id = member.hall_id AND login.user_id = member.user_id "
                f"WHERE login.hall_id = {hall.hall_id} "
                f"AND login.user_id IN ({vip_user_condition_str}) "
                f"AND login.data_date BETWEEN '{start}' AND '{end}' "
                "GROUP BY login.user_id, member.username, login_hour, login.data_date, "
                "login.financial_year, login.financial_month, login.financial_week "
            )
            self.bq_db.mode = self.bq_db.QUERY_MODE
            self.bq_db.exec()
            df = self.bq_db.fetch_data
            df['data_date'] = pd.to_datetime(df['data_date'], format='%Y-%m-%d')
            df['weekday'] = df['data_date'].dt.dayofweek
            df['weekday'] = df['weekday'] + 1
            df['data_date'] = df['data_date'].astype(str)
            insert_df = df
            if len(insert_df) > 0:
                insert_df.insert(0, 'hall_id', hall.hall_id)
                insert_df.insert(1, 'hall_name', hall.hall_name)
                insert_df.insert(2, 'domain_id', 0)
                insert_df.insert(3, 'domain_name', None)
                self.replace_data_with('vip_login', insert_df)

    @try_exc_func
    def login_location(self, hall: Hall, start, end):
        self.bq_db.statement = (
            "SELECT login.user_id, "
            "login.parent_username AS ag_name, "
            "login.ip, "
            "login.country AS country_code, "
            "login.city, "
            "login.client_os, "
            "login.client_browser "
            f"FROM `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.login_log` login "
            f"JOIN "
            "( "
            "    SELECT hall_id, user_id, data_date, result, MAX(`at`) AS max_at"
            f"    FROM `{self.bq_db.project_id}.ssr_bbin_dw_{hall.hall_name}.login_log` "
            f"    WHERE hall_id = {hall.hall_id} "
            f"    AND data_date BETWEEN '{start}' AND '{end}' "
            f"    AND result = 1 "
            "    GROUP BY hall_id, user_id, data_date, result "
            ") latest_login "
            "ON login.hall_id = latest_login.hall_id "
            "AND login.user_id = latest_login.user_id "
            "AND login.data_date = latest_login.data_date "
            "AND login.result = latest_login.result "
            "AND login.`at` = latest_login.max_at "
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        login_ip_df = self.bq_db.fetch_data
        login_ip_list = login_ip_df['ip'].values.tolist()
        ip_location_df = search_ip_multiple(ip_lst=login_ip_list, database_type='2')
        insert_df = pd.concat([login_ip_df, ip_location_df], axis=1).drop(columns=['request_ip'])

        if len(insert_df) > 0:
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', 0)
            insert_df.insert(3, 'domain_name', None)
            self.replace_data_with('login_location', insert_df)

    @try_exc_func
    def ga_data(self, hall: Hall, data_type, start, end):
        delta = timedelta(days=2)
        query_date = datetime.strptime(start, '%Y-%m-%d').date() -delta
        self.bq_db.resource_id = hall.ga_resource_id
        self.bq_db.firebase_id = hall.ga_firebase_id
        self.bq_db.hall_id = hall.hall_id
        self.bq_db.hall_name = hall.hall_name
        data_date = str(query_date).replace('-', '')
        # data_date = (datetime(data_date) -delta).date()
        self.bq_db.data_date = data_date
        self.bq_db.query_by(action=data_type)
        try:
            self.bq_db.exec()

        except:
            # 再減一天
            for d in range(2,4500):
                try:
                    delta = timedelta(days=d)
                    query_start_date = datetime.strptime(start, '%Y-%m-%d').date() - delta
                    data_date = str(query_start_date).replace('-', '')
                    self.bq_db.data_date = data_date
                    self.bq_db.statement = self.bq_db.statement.replace(f'intraday_', '')
                    # print('-------')
                    # print(self.bq_db.statement)
                    self.bq_db.exec()
                    print(f'{data_date} finish')

                except:
                    # 再減一天
                    delta = timedelta(days=2)
                    query_start_date = datetime.strptime(start, '%Y-%m-%d').date() - delta
                    data_date = str(query_start_date).replace('-', '')
                    self.bq_db.data_date = data_date
                    self.bq_db.query_by(action=data_type)
                    self.bq_db.statement = self.bq_db.statement.replace(f'ga_sessions_intraday_', f'ga_sessions_')
                    self.bq_db.exec()
                    # print(data_date)

        ga_result_df = self.bq_db.fetch_data
        if data_type != 'firebase_page':
            for index, row in ga_result_df.iterrows():
                cid = str(row['user_id']).replace("'", "")
                uid = 0
                try:
                    uid = int(str(cid), 24)
                except Exception as e:
                    print(e)
                finally:
                    ga_result_df.loc[index, 'user_id'] = uid
        if len(ga_result_df) > 0:
            ga_result_df = ga_result_df[ga_result_df.user_id != 0]
            ga_result_df['data_date'] = ga_result_df['data_date'].astype(str)
            ga_result_df.insert(0, 'hall_id', hall.hall_id)
            ga_result_df.insert(1, 'hall_name', hall.hall_name)
            ga_result_df.insert(2, 'domain_id', 0)
            ga_result_df.insert(3, 'domain_name', None)
            ga_result_df = ga_result_df.merge(Environment.date_record, how='left', left_on='data_date',
                                                  right_on='date')
            ga_result_df = ga_result_df.drop(
                    columns=['fin_week_start', 'fin_week_end', 'fin_month_weeks', 'fin_M_ym01'])
            self.column_times_setter(column_times=2)
            self.replace_data_with(f'ga_{data_type}', ga_result_df)
            print(f"Update {data_type} GA Data")