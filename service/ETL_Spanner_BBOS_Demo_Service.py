from google.cloud import spanner
import os
import pandas as pd
import numpy as np
import time
import pytz
from datetime import datetime, timedelta, date
import Environment
from Adaptors.CloudStorageAdaptor import GCS
from Adaptors.Hall import Hall
from Adaptors.BQAdaptor import BQ_BaseBBOS
from Adaptors.SpannerAdaptor import CDP_DEMO_BBOS, CDP_Config, RuleTag, NAPL, SyncTag
from util.dataframe_process import df_timezone_format, df_type_format_tag
from Adaptors.TelegramAdaptor import Notify
from util.ip_database import search_ip_multiple
from Adaptors.APIAdaptor import BBOS as BBOSApi
from Adaptors.BQAdaptor import BQ_GH

notify_bot = Notify()


def try_exc_func(func):
    def wrap_func(*args, **kwargs):
        exec_start_time = datetime.now()
        try:
            print(f'{func.__name__} begin...')
            func(*args, **kwargs)
        except Exception as e:
            print(f"Error Message: {e} ")
            notify_bot.message = (
                f"CDP ETL Spanner: {kwargs['hall'].domain_name}, {func.__name__} error\n"
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
            print(f"{kwargs['hall'].domain_name} {func.__name__} exec time: {result_time[0]} minutes, {result_time[1]} seconds")

    return wrap_func


class ETL_Spanner_BBOS_Demo_Service(CDP_DEMO_BBOS):
    def __init__(self, spanner_db: str, bq_db):
        super(ETL_Spanner_BBOS_Demo_Service, self).__init__(database=spanner_db)
        self.bq_db = bq_db
        self.sv_begin_time = None
        self.sv_end_time = None
        self.telegram_message = ''

    @try_exc_func
    def member_info(self, hall: Hall, start, end):
        # 將datetime加回12小時，以台北時間直接insert spanner，會自動轉為UTC時區
        self.bq_db.statement = (
            f"""
            SELECT user_id, 
            parent_username AS ag_name, 
            username AS user_name, 
            name AS name_real, 
            DATETIME_ADD(created_at, INTERVAL 12 HOUR) AS register_date, 
            created_ip AS register_ip, 
            created_country AS register_country, 
            created_city AS register_city, 
            created_by AS register_by, 
            phone AS user_phone, 
            email AS user_mail, 
            balance, 
            DATETIME_ADD(last_login, INTERVAL 12 HOUR) AS last_login, 
            last_ip, 
            last_country, 
            last_city_id, 
            DATETIME_ADD(last_online, INTERVAL 12 HOUR) AS last_online, 
            zalo, 
            facebook, 
            0 AS arbitrage_flag, 
            level_id AS user_level_id, 
            level_name AS user_level, 
            email_confirm, 
            phone_confirm 
            FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.user_info`  
            WHERE
            domain_id = {hall.domain_id} 
            AND role = 1 
            AND (DATE(created_at) BETWEEN '{start}' AND '{end}' 
            OR 
            DATE(created_time) BETWEEN '{start}' AND '{end}'
            ) 
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        print(self.bq_db.statement)
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data
        if len(insert_df) > 0:
            insert_df[['email_confirm', 'phone_confirm']] = insert_df[['email_confirm', 'phone_confirm']].fillna(0)
            insert_df[['email_confirm', 'phone_confirm']] = insert_df[['email_confirm', 'phone_confirm']].astype(int)
            insert_df = df_timezone_format(insert_df, 'Asia/Taipei')  # 設定時區
            insert_df['last_online'] = insert_df['last_online'].fillna(pd.Timestamp.min)
            insert_df['last_login'] = insert_df['last_login'].fillna(pd.Timestamp.min)
            insert_df['register_date'] = insert_df['register_date'].fillna(pd.Timestamp.min)
            insert_df['last_city_id'] = insert_df['last_city_id'].where(
                pd.notnull(insert_df['last_city_id']), 0)
            insert_df['last_city_id'] = insert_df['last_city_id'].astype(int)
            insert_df['user_level_id'] = insert_df['user_level_id'].where(
                pd.notnull(insert_df['user_level_id']), 0)
            insert_df['user_level_id'] = insert_df['user_level_id'].astype(int)
            insert_df['register_by'] = insert_df['register_by'].where(
                pd.notnull(insert_df['register_by']), 0)
            insert_df['register_by'] = insert_df['register_by'].astype(int)

            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', hall.domain_id)
            insert_df.insert(3, 'domain_name', hall.domain_name)

            self.upsert_data_with('member_info', insert_df)

    @try_exc_func
    def login_log(self, hall: Hall, start, end):
        self.bq_db.statement = (
            f"""
            SELECT user_id, SUM(count) AS login_count, host, 
            data_date, financial_year, financial_month, financial_week 
            FROM (
                SELECT log.user_id, count(log.user_id) AS count, 
                    CASE WHEN app_user.host IS NOT NULL THEN 'App' ELSE 'Web' END AS host, 
                    log.data_date, financial_year, financial_month, financial_week 
                FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.login_log` log 
                JOIN `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.user_info` member 
                    ON log.user_id = member.user_id 
            LEFT JOIN (
                SELECT user_id, 'App' AS host, data_date 
                FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.login_log` 
                WHERE 1=1 AND data_date BETWEEN '{start}' AND '{end}' 
                AND LOWER(host) = 'app' 
                GROUP BY user_id, data_date 
            ) app_user ON log.user_id = app_user.user_id AND log.data_date = app_user.data_date 
                WHERE 1=1 AND member.role = 1 AND log.data_date BETWEEN '{start}' AND '{end}' 
                GROUP BY log.user_id, log.host, app_user.host, log.data_date, 
                    financial_year, financial_month, financial_week 
            ) login_result 
            GROUP BY user_id, host, data_date, financial_year, financial_month, financial_week 
            """

        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data

        if len(insert_df) > 0:
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', hall.domain_id)
            insert_df.insert(3, 'domain_name', hall.domain_name)
            self.replace_data_with('login_log', insert_df)

    @try_exc_func
    def game_dict(self, hall: Hall):
        self.bq_db.statement = (
            f"""
            SELECT
              vendor_id AS lobby,
              vendor.tc_name AS lobby_name,
              vendor.sc_name AS lobby_name_cn,
              vendor.en_name AS lobby_name_en,
              game.game_code AS game_type,
              game.zh_tw_name AS game_type_name,
              game.zh_cn_name AS game_type_name_cn,
              game.game_name AS game_type_name_en,
              game.kind AS game_kind,
              CASE
                WHEN game.zh_tw_name LIKE '%百家%' THEN 1
                WHEN game.zh_tw_name LIKE '%輪盤%' THEN 1
              ELSE
              0
            END
              AS is_pvp,
              CASE
                WHEN game.zh_tw_name LIKE '%百家%' THEN 1
                WHEN game.zh_tw_name LIKE '%21%' THEN 1
                WHEN game.zh_tw_name LIKE '%二十一%' THEN 1
              ELSE
              0
            END
              AS is_pve
            FROM
            `{self.bq_db.project_id}.general_information.game_code_dict` game 
            JOIN 
            `{self.bq_db.project_id}.general_information.Game_Vendor_View` vendor 
            ON
              game.vendor_id = vendor.id 
              AND game.kind = CASE WHEN vendor.tc_name LIKE '%體育%' THEN 1 
              WHEN vendor.tc_name LIKE '%視訊%' THEN 2
              WHEN vendor.tc_name LIKE '%電子%' THEN 3
              WHEN vendor.tc_name LIKE '%彩票%' THEN 4
              WHEN vendor.tc_name LIKE '%棋牌%' THEN 5
              WHEN vendor.tc_name LIKE '%麻將%' THEN 6
              ELSE game.kind END
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data
        if len(insert_df) > 0:
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', hall.domain_id)
            insert_df.insert(3, 'domain_name', hall.domain_name)
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
              Game_Vendor_View.tc_name AS lobby_name,
              Game_Vendor_View.sc_name AS lobby_name_cn,
              Game_Vendor_View.en_name AS lobby_name_en,
              bet.game_code AS game_type,
              bet.kind AS game_kind,
              CASE
                WHEN bet.kind = 1 THEN 'sport'
                WHEN bet.kind = 2 THEN 'live'
                WHEN bet.kind = 3 THEN 'prob'
                WHEN bet.kind = 4 THEN 'lottery'
                WHEN bet.kind = 5 THEN 'card'
                WHEN bet.kind = 6 THEN 'mahjong'
            END
              AS game_kind_name,
              game.zh_tw_name AS game_name,
              game.zh_cn_name AS game_name_cn,
              game.game_name AS game_name_en,
              SUM(bet.wagers_total) AS wagers_total,
              SUM(bet.bet) AS bet_amount,
              SUM(bet.valid_bet) AS commissionable,
              SUM(bet.payoff) AS payoff,
              AVG(bet.win_rate) AS win_rate,
              bet.device AS platform,
              bet.data_date,
              bet.financial_year,
              bet.financial_month,
              bet.financial_week,
              member.level_id AS user_level_id,
              member.level_name AS user_level
            FROM
              `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.bet_analysis_jackpot` bet
            JOIN
              `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.user_info` member
            ON
              member.user_id = bet.user_id
            LEFT JOIN
              `{self.bq_db.project_id}.general_information.Game_Vendor_View` Game_Vendor_View
            ON
              bet.lobby = Game_Vendor_View.id
              AND bet.kind = CASE WHEN Game_Vendor_View.tc_name LIKE '%體育%' THEN 1 
              WHEN Game_Vendor_View.tc_name LIKE '%視訊%' THEN 2
              WHEN Game_Vendor_View.tc_name LIKE '%電子%' THEN 3
              WHEN Game_Vendor_View.tc_name LIKE '%彩票%' THEN 4
              WHEN Game_Vendor_View.tc_name LIKE '%棋牌%' THEN 5
              WHEN Game_Vendor_View.tc_name LIKE '%麻將%' THEN 6
              ELSE bet.kind END
            LEFT JOIN
              `{self.bq_db.project_id}.general_information.game_code_dict` game
            ON
              bet.lobby = game.vendor_id
              AND bet.game_code = game.game_code
            WHERE
              member.role = 1 AND
              bet.game_code <> 'settlement01'
              AND data_date BETWEEN '{start}' AND '{end}' 
            GROUP BY
              bet.user_id,
              bet.parent_username,
              bet.lobby,
              Game_Vendor_View.tc_name,
              Game_Vendor_View.sc_name,
              Game_Vendor_View.en_name,
              bet.game_code,
              bet.kind,
              game.zh_tw_name,
              game.zh_cn_name,
              game.game_name,
              bet.device,
              bet.data_date,
              bet.financial_year,
              bet.financial_month,
              bet.financial_week,
              member.level_id,
              member.level_name
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data
        if len(insert_df) > 0:
            insert_df['user_level_id'] = insert_df['user_level_id'].where(
                pd.notnull(insert_df['user_level_id']), 0)
            insert_df['user_level_id'] = insert_df['user_level_id'].astype(int)
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', hall.domain_id)
            insert_df.insert(3, 'domain_name', hall.domain_name)
            insert_df.insert(18, 'platform_name', '')
            for platform, platform_name in Environment.platform_name_dict.items():
                insert_df.loc[insert_df['platform'] == platform, 'platform_name'] = platform_name
            self.column_times_setter(column_times=2)
            self.replace_data_with('bet_analysis', insert_df)

    @try_exc_func
    def deposit_withdraw(self, hall: Hall, start, end):
        self.bq_db.statement = (
            f"""
            SELECT deposit.user_id,  
            member.parent_username AS ag_name, 
            IFNULL(SUM(deposit.amount), 0) AS deposit_amount, 
            IFNULL(SUM(deposit.deposit_fee), 0) AS deposit_fee, 
            IFNULL(SUM(deposit.deposit_count), 0) AS deposit_count, 
            IFNULL(SUM(withdraw.real_amount), 0) AS withdraw_amount, 
            IFNULL(SUM(withdraw.withdraw_fee), 0) AS withdraw_fee, 
            IFNULL(SUM(withdraw.withdraw_count), 0) AS withdraw_count, 
            deposit.data_date, deposit.financial_year, deposit.financial_month, deposit.financial_week, 
            member.level_id AS user_level_id, member.level_name AS user_level 
            FROM ( 
                SELECT user_id, SUM(amount) AS amount, SUM(fee) AS deposit_fee, 
                SUM(deposit_count) AS deposit_count, 
                data_date, financial_year , financial_month , financial_week 
                FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.deposit_record`
                    WHERE status <> 0 
                    GROUP BY user_id, data_date, financial_year , financial_month , financial_week 
            ) deposit 
            JOIN `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.user_info` member 
            ON deposit.user_id = member.user_id 
            LEFT JOIN ( 
                SELECT user_id, SUM(real_amount) AS real_amount, SUM(fee) AS withdraw_fee, 
                SUM(withdraw_count) AS withdraw_count, 
                data_date 
                FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.withdraw_record` 
                    WHERE status <> 0 
                    GROUP BY user_id, data_date 
            ) withdraw ON deposit.user_id = withdraw.user_id 
                AND deposit.data_date = withdraw.data_date 
            WHERE deposit.data_date BETWEEN '{start}' AND '{end}' 
                AND member.parent_username IS NOT NULL 
                GROUP BY deposit.user_id , member.parent_username, 
                deposit.data_date, deposit.financial_year, deposit.financial_month, deposit.financial_week, 
                level_id, level_name 
            UNION DISTINCT 
            SELECT withdraw.user_id, 
            member.parent_username, 
            IFNULL(SUM(deposit.amount), 0) AS deposit_amount, 
            IFNULL(SUM(deposit.deposit_fee), 0) AS deposit_fee, 
            IFNULL(SUM(deposit.deposit_count), 0) AS deposit_count, 
            IFNULL(SUM(withdraw.real_amount), 0) AS withdraw_amount, 
            IFNULL(SUM(withdraw.withdraw_fee), 0) AS withdraw_fee, 
            IFNULL(SUM(withdraw.withdraw_count), 0) AS withdraw_count, 
            withdraw.data_date, withdraw.financial_year, withdraw.financial_month, withdraw.financial_week, 
            member.level_id AS user_level_id, member.level_name AS user_level 
            FROM ( 
                SELECT user_id, SUM(amount) AS amount, SUM(fee) AS deposit_fee, 
                SUM(deposit_count) AS deposit_count, data_date 
                FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.deposit_record` 
                    WHERE status <> 0 GROUP BY user_id, data_date) deposit 
            RIGHT JOIN ( 
                SELECT user_id, SUM(real_amount) AS real_amount, SUM(fee) AS withdraw_fee, 
                SUM(withdraw_count) AS withdraw_count, 
                data_date, financial_year , financial_month , financial_week 
                FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.withdraw_record` 
                    WHERE status <> 0 
                    GROUP BY user_id, data_date, financial_year , financial_month , financial_week 
            ) withdraw ON deposit.user_id = withdraw.user_id 
                AND withdraw.data_date = deposit.data_date 
            JOIN `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.user_info` member 
            ON withdraw.user_id = member.user_id 
                WHERE withdraw.data_date BETWEEN '{start}' AND '{end}' 
                AND member.parent_username IS NOT NULL 
                GROUP BY withdraw.user_id , member.parent_username, 
                withdraw.data_date, withdraw.financial_year, withdraw.financial_month, withdraw.financial_week, 
                level_id, level_name
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data
        if len(insert_df) > 0:
            insert_df['user_level_id'] = insert_df['user_level_id'].where(
                pd.notnull(insert_df['user_level_id']), 0)
            insert_df['user_level_id'] = insert_df['user_level_id'].astype(int)
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', hall.domain_id)
            insert_df.insert(3, 'domain_name', hall.domain_name)
            self.column_times_setter(column_times=2)
            # self.replace_data_with('deposit_withdraw_record', insert_df)

    @try_exc_func
    def offer(self, hall: Hall, start, end):
        self.bq_db.statement = (
            f"""
            SELECT offer.user_id, 
            member.parent_username AS ag_name, 
            opcode, opcode_name, premium_total, premium_amount, 
            data_date, financial_year, financial_month, financial_week, 
            member.level_id AS user_level_id, member.level_name AS user_level 
            FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.offer_info` offer 
            JOIN `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.user_info` member 
            ON member.user_id = offer.user_id 
            WHERE offer.data_date BETWEEN '{start}' AND '{end}' 
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data
        if len(insert_df) > 0:
            insert_df['user_level_id'] = insert_df['user_level_id'].where(
                pd.notnull(insert_df['user_level_id']), 0)
            insert_df['user_level_id'] = insert_df['user_level_id'].astype(int)
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', hall.domain_id)
            insert_df.insert(3, 'domain_name', hall.domain_name)
            self.replace_data_with('offer_info', insert_df)

    @try_exc_func
    def dispatch(self, hall: Hall, start, end):
        self.bq_db.statement = (
            f"""
            SELECT user_id, 
            promotion_id, promotion_name, premium_total, premium_amount, 
            data_date, financial_year, financial_month, financial_week 
            FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.offer_dispatch` dispatch 
            WHERE dispatch.data_date BETWEEN '{start}' AND '{end}' 
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data
        if len(insert_df) > 0:
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', hall.domain_id)
            insert_df.insert(3, 'domain_name', hall.domain_name)
            self.replace_data_with('offer_dispatch', insert_df)

    @try_exc_func
    def applicant(self, hall: Hall, start, end):
        self.bq_db.statement = (
            f"""
            SELECT applicant.user_id, 
            applicant.promotion_id , 
            promotion.name AS promotion_name, 
            count(applicant.user_id) AS application_count, 
            IFNULL(info.premium_total, 0) AS premium_total, 
            IFNULL(info.premium_amount, 0) AS premium_amount, 
            applicant.data_date, 
            applicant.financial_year, applicant.financial_month, applicant.financial_week 
            FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.offer_applicant` applicant 
            JOIN `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.offer_promotion_info` promotion 
                ON applicant.promotion_id = promotion.promotion_id 
            LEFT JOIN `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.offer_dispatch` info 
                ON promotion.promotion_id = info.promotion_id AND 
                applicant.user_id = info.user_id AND 
                applicant.data_date = info.data_date 
            WHERE applicant.data_date BETWEEN '{start}' AND '{end}' 
                GROUP BY applicant.user_id, applicant.promotion_id, promotion.name, 
                premium_total, premium_amount, applicant.data_date, 
                applicant.financial_year, applicant.financial_month, applicant.financial_week
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data
        if len(insert_df) > 0:
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', hall.domain_id)
            insert_df.insert(3, 'domain_name', hall.domain_name)
            self.replace_data_with('offer_applicant', insert_df)

    @try_exc_func
    def promotion(self, hall: Hall, start, end):
        self.bq_db.statement = (
            f"""
            SELECT promotion_id, 
            name AS promotion_name, 
            status AS promotion_status, 
            DATE(start_time) AS begin_date, 
            DATE(end_time) AS end_date 
            FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.offer_promotion_info` 
            WHERE DATE(start_time) >= '{start}' 
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        insert_df = self.bq_db.fetch_data
        if len(insert_df) > 0:
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', hall.domain_id)
            insert_df.insert(3, 'domain_name', hall.domain_name)
            self.replace_data_with('offer_promotion_info', insert_df)

    @try_exc_func
    def profit_loss(self, hall: Hall, start, end):
        self.bq_db.statement = (
            f"""
            SELECT offer.user_id, SUM(premium_amount) AS premium_amount, 
            data_date, financial_year, financial_month, financial_week 
            FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.offer_info` offer 
            JOIN `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.user_info` member 
            ON member.user_id = offer.user_id 
            WHERE data_date BETWEEN '{start}' AND '{end}' 
            GROUP BY offer.user_id, data_date, financial_year, financial_month, financial_week 
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        offer_df = self.bq_db.fetch_data

        self.bq_db.statement = (
            f"""
            SELECT bet.user_id, SUM(payoff) AS payoff, 
            data_date, financial_year, financial_month, financial_week 
            FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.bet_analysis_jackpot` bet 
            JOIN `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.member_info` member 
            ON member.user_id = bet.user_id 
            WHERE data_date BETWEEN '{start}' AND '{end}' 
            GROUP BY bet.user_id, data_date, financial_year, financial_month, financial_week 
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
            insert_df.insert(2, 'domain_id', hall.domain_id)
            insert_df.insert(3, 'domain_name', hall.domain_name)
            self.column_times_setter(column_times=2)
            self.replace_data_with('profit_loss', insert_df)

    @try_exc_func
    def vip_level(self, hall: Hall):
        if hall.domain_name == 'ma':
            return
        # self.bq_db.statement = (
        #     f"""
        #     SELECT
        #     user_vip_level.user_id, member.username as user_name,
        #     user_vip_level.vip_id, vip_level.name AS vip_name, user_vip_level.vip_level
        #     FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.user_vip_level` user_vip_level
        #     JOIN `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.vip_level` vip_level
        #     ON user_vip_level.vip_id = vip_level.vip_id
        #     LEFT JOIN `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.user_info` member
        #     ON user_vip_level.user_id = member.user_id
        #     WHERE user_vip_level.vip_level <> 0 AND member.role = 1
        #     """
        # )
        # self.bq_db.mode = self.bq_db.QUERY_MODE
        # self.bq_db.exec()
        # insert_df = self.bq_db.fetch_data

        self.bq_db.statement = (
            f"""
            SELECT 
            member.user_id, member.username as user_name
            FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.user_info` member 
            WHERE member.role = 1
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        member_df = self.bq_db.fetch_data

        bq_gh = BQ_GH()
        bq_gh.statement = (
            f"""
            SELECT 
            user_vip_level.user_id, 
            user_vip_level.vip_id, vip_level.name AS vip_name
            , user_vip_level.vip_level 
            FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.user_vip_level` user_vip_level 
            JOIN `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.vip_level` vip_level 
            ON user_vip_level.vip_id = vip_level.vip_id 
            WHERE 
            user_vip_level.vip_level <> 0 
            """
        )
        bq_gh.mode = self.bq_db.QUERY_MODE
        bq_gh.exec()
        vip_df = bq_gh.fetch_data

        insert_df = pd.merge(left=member_df, right=vip_df, left_on='user_id', right_on='user_id', how='inner')

        if len(insert_df) > 0:
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', hall.domain_id)
            insert_df.insert(3, 'domain_name', hall.domain_name)
            self.replace_data_with('user_vip_level', insert_df)

    @try_exc_func
    def vip_login(self, hall: Hall, start, end):
        self.bq_db.statement = (
            f"""            
            SELECT login.user_id, member.username AS user_name, 
            EXTRACT(HOUR FROM login.login_date_ae) AS login_hour, 
            count(login.user_id) AS login_count, DATE(login.login_date_ae) AS data_date
            FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.Login_View` login 
            JOIN `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.user_info` member 
            ON login.user_id = member.user_id 
            LEFT JOIN `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.user_vip_level` vip 
            ON login.user_id = vip.user_id 
            WHERE vip.vip_level >= 1
            AND DATE(login.login_date_ae) BETWEEN '{start}' AND '{end}' 
            GROUP BY login.user_id, member.username, login_hour, DATE(login.login_date_ae)
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        df = self.bq_db.fetch_data
        df['data_date'] = pd.to_datetime(df['data_date'], format='%Y-%m-%d')
        df['weekday'] = df['data_date'].dt.dayofweek
        df['weekday'] = df['weekday'] + 1
        df['data_date'] = df['data_date'].astype(str)
        insert_df = df.merge(Environment.date_record, how='left', left_on='data_date',
                             right_on='date')
        insert_df = insert_df.drop(
            columns=['fin_week_start', 'fin_week_end', 'fin_month_weeks', 'fin_M_ym01'])
        if len(insert_df) > 0:
            insert_df.insert(0, 'hall_id', hall.hall_id)
            insert_df.insert(1, 'hall_name', hall.hall_name)
            insert_df.insert(2, 'domain_id', hall.domain_id)
            insert_df.insert(3, 'domain_name', hall.domain_name)
            self.replace_data_with('vip_login', insert_df)

    @try_exc_func
    def login_location(self, hall: Hall, start, end):
        self.bq_db.statement = (
            f"""
            SELECT login.user_id, login.ip, 
            login.country_code, 
            login.city, 
            login.client_os, 
            login.client_browser 
            FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.Login_View` login 
            JOIN `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.user_info` member 
            ON login.user_id = member.user_id 
            JOIN 
            ( 
                SELECT user_id, DATE(login_date_ae) AS data_date, result, MAX(login_date_ae) AS max_at
                FROM `{self.bq_db.project_id}.ssr_dw_{hall.domain_name}.Login_View` 
                WHERE DATE(login_date_ae) BETWEEN '{start}' AND '{end}' AND result = 1 AND role = 1
                GROUP BY user_id, data_date, result 
            ) latest_login 
            ON login.user_id = latest_login.user_id 
            AND DATE(login.login_date_ae) = latest_login.data_date 
            AND login.result = latest_login.result 
            AND login.role = 1
            AND login.login_date_ae = latest_login.max_at 
            """
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
            insert_df.insert(2, 'domain_id', hall.domain_id)
            insert_df.insert(3, 'domain_name', hall.domain_name)
            self.replace_data_with('login_location', insert_df)

    @try_exc_func
    def ga_data(self, hall: Hall, data_type, start, end):
        delta = timedelta(days=1)
        query_start_date = datetime.strptime(start, '%Y-%m-%d').date() - delta  # 美東時間
        self.bq_db.resource_id = hall.ga_resource_id
        self.bq_db.firebase_id = hall.ga_firebase_id
        self.bq_db.hall_id = hall.hall_id
        self.bq_db.hall_name = hall.hall_name
        self.bq_db.domain_id = hall.domain_id
        self.bq_db.domain_name = hall.domain_name

        if data_type == 'firebase_page' and self.bq_db.firebase_id == 0:
            return
        data_date = str(query_start_date).replace('-', '')
        self.bq_db.data_date = data_date
        self.bq_db.query_by(action=data_type)
        try:
            self.bq_db.exec()

        except:
            # 再減一天
            delta = timedelta(days=2)
            query_start_date = datetime.strptime(start, '%Y-%m-%d').date() - delta
            data_date = str(query_start_date).replace('-', '')
            self.bq_db.data_date = data_date
            self.bq_db.statement = self.bq_db.statement.replace(f'ga_sessions_intraday_', f'ga_sessions_')
            self.bq_db.exec()

        ga_result_df = self.bq_db.fetch_data
        print(f"Get {data_type} Data")
        if data_type != 'firebase_page':
            for index, row in ga_result_df.iterrows():
                cid = row['user_id']
                uid = int(str(cid), 24)
                ga_result_df.loc[index, 'user_id'] = uid
        if len(ga_result_df) > 0:
            ga_result_df['data_date'] = ga_result_df['data_date'].astype(str)
            ga_result_df.insert(0, 'hall_id', hall.hall_id)
            ga_result_df.insert(1, 'hall_name', hall.hall_name)
            ga_result_df.insert(2, 'domain_id', hall.domain_id)
            ga_result_df.insert(3, 'domain_name', hall.domain_name)
            ga_result_df = ga_result_df.merge(Environment.date_record, how='left', left_on='data_date', right_on='date')
            ga_result_df = ga_result_df.drop(
                columns=['fin_week_start', 'fin_week_end', 'fin_month_weeks', 'fin_M_ym01'])
            self.column_times_setter(column_times=2)
            self.replace_data_with(f'ga_{data_type}', ga_result_df)
            print(f"Update {data_type} GA Data")

    # @try_exc_func
    # def tag_custom(self, hall: Hall, spanner_db: CDP_BBOS, spanner_config: CDP_Config):
    #     self.table_name = 'user_tag_custom_batch'
    #     self.statement = (
    #         f"""
    #         SELECT hall_id, hall_name, domain_id, domain_name,
    #         tag_code, file_name, file_path, operator
    #         FROM user_tag_custom_batch
    #         WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id}
    #         AND status = 0
    #         """
    #     )
    #     self.mode = self.QUERY_MODE
    #     self.exec()
    #     storage = GCS('ghr-cdp', f'{Environment.ROOT_PATH}/cert/gcp-ghr-storage.json')
    #     if len(self.fetch_data) > 0:
    #         rule_tag = RuleTag(spanner_db=spanner_db, spanner_config=spanner_config)
    #
    #         columns = ['hall_id', 'hall_name', 'domain_id', 'domain_name',
    #                    'tag_code', 'file_name', 'file_path', 'operator']
    #         for batch in self.fetch_data:
    #             self.column_times_setter(column_times=1)
    #             batch_df = pd.DataFrame([batch], columns=columns)
    #             tag_code = batch_df['tag_code'].values[0]
    #             file_name = batch_df['file_name'].values[0]
    #             file_path = batch_df['file_path'].values[0]
    #             # 更新狀態為進行中
    #             self.update_data_dml_with(
    #                 statement=
    #                 "UPDATE user_tag_custom_batch SET status = 3, "
    #                 "updated_time = PENDING_COMMIT_TIMESTAMP() "
    #                 f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #                 f"AND tag_code = {tag_code} "
    #             )
    #             self.update_data_dml_with(
    #                 statement=
    #                 "UPDATE user_tag_custom_batch_log SET status = 3, "
    #                 "updated_time = PENDING_COMMIT_TIMESTAMP() "
    #                 f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #                 f"AND tag_code = {tag_code} "
    #                 f"AND file_name = \'{file_name}\' "
    #             )
    #             try:
    #                 storage.blob = f'{file_path}{file_name}'
    #                 storage.file = f'{Environment.ROOT_PATH}/files/{file_name}'
    #                 storage.mode = storage.DOWNLOAD_MODE
    #                 storage.exec()
    #                 tag_file_df = pd.read_csv(storage.file)
    #                 tag_file_df.columns.values[0] = "user_name"
    #                 tag_file_df.columns.values[1] = "data_date"
    #                 tag_file_df.columns.values[2] = "enable"
    #                 # 日期格式標準化 "/" 改 "-" 並補0
    #                 tag_file_df['data_date'] = tag_file_df['data_date'].str.replace('/', '-')
    #                 tag_file_df['data_date'] = tag_file_df['data_date'].replace(
    #                     to_replace=r"(\d{4})(\-)(\d{1})(\-)(\d{1}$)",
    #                     value=r"\1-0\3-0\5", regex=True)
    #                 tag_file_df['data_date'] = tag_file_df['data_date'].replace(
    #                     to_replace=r"(\d{4})(\-)(\d{1})(\-)(\d{2}$)",
    #                     value=r"\1-0\3-\5", regex=True)
    #                 tag_file_df['data_date'] = tag_file_df['data_date'].replace(
    #                     to_replace=r"(\d{4})(\-)(\d{2})(\-)(\d{1}$)",
    #                     value=r"\1-\3-0\5", regex=True)
    #                 enable_count = tag_file_df['enable'].value_counts()
    #                 add_count = 0 if enable_count.get(1) is None else enable_count.get(1)
    #                 remove_count = 0 if enable_count.get(0) is None else enable_count.get(0)
    #                 username_condition = "','".join(tag_file_df['user_name'])
    #
    #                 update_df = self.select_data_with(
    #                     columns=['hall_id', 'hall_name', 'domain_id', 'domain_name',
    #                              'user_id', 'user_name', 'register_date'],
    #                     statement="SELECT hall_id, hall_name, domain_id, domain_name, "
    #                               "user_id, user_name, register_date "
    #                               "FROM member_info WHERE "
    #                               f"hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #                               f"AND user_name IN ('{username_condition}') "
    #                 )
    #                 update_df['tag_type'] = str(tag_code)[0]
    #                 update_df['tag_code'] = tag_code
    #                 update_df['updated_time'] = spanner.COMMIT_TIMESTAMP
    #                 merge_df = pd.merge(left=update_df, right=tag_file_df,
    #                                     on='user_name', how='left')
    #                 merge_df['enable'] = merge_df['enable'].astype('bool')
    #
    #                 date_log_columns = [
    #                     'hall_id', 'hall_name', 'domain_id', 'domain_name',
    #                     'user_id', 'tag_code', 'data_date', 'enable', 'updated_time']
    #                 self.replace_data_with('user_tag_custom_date_log', merge_df[date_log_columns])
    #
    #                 ods_df = merge_df.rename(columns={'enable': 'tag_enabled'})
    #                 ods_columns = [
    #                     'hall_id', 'domain_id', 'user_id', 'user_name',
    #                     'tag_code', 'tag_type', 'tag_enabled', 'register_date', 'updated_time']
    #                 self.replace_data_with('user_tag_ods_data', ods_df[ods_columns])
    #
    #                 # 更新總人數
    #                 row_count_df = self.select_data_with(
    #                     ['row_count'],
    #                     "SELECT COUNT(1) FROM user_tag_ods_data "
    #                     "WHERE "
    #                     f"hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #                     f"AND tag_code = {tag_code} AND tag_enabled IS TRUE "
    #                 )
    #                 self.update_data_dml_with(
    #                     "UPDATE user_tag_custom_batch "
    #                     f"SET row_count = {row_count_df['row_count'].values[0]} "
    #                     f"WHERE "
    #                     f"hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} AND tag_code = {tag_code} "
    #                 )
    #
    #                 # 更新狀態為完成
    #                 self.update_data_dml_with(
    #                     statement=
    #                     "UPDATE user_tag_custom_batch "
    #                     f"SET status = 1, "
    #                     "updated_time = PENDING_COMMIT_TIMESTAMP() "
    #                     f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #                     f"AND tag_code = {tag_code} "
    #                 )
    #                 self.update_data_dml_with(
    #                     statement=
    #                     "UPDATE user_tag_custom_batch_log "
    #                     f"SET status = 1, add_count = {add_count}, remove_count = {remove_count}, "
    #                     "updated_time = PENDING_COMMIT_TIMESTAMP() "
    #                     f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #                     f"AND tag_code = {tag_code} AND file_name = \'{file_name}\'"
    #                 )
    #
    #             except Exception as e:
    #                 self.update_data_dml_with(
    #                     statement=
    #                     "UPDATE user_tag_custom_batch "
    #                     f"SET status = 2, "
    #                     "updated_time = PENDING_COMMIT_TIMESTAMP() "
    #                     f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #                     f"AND tag_code = {tag_code} "
    #                 )
    #                 self.update_data_dml_with(
    #                     statement=
    #                     "UPDATE user_tag_custom_batch_log "
    #                     f"SET status = 2, "
    #                     "updated_time = PENDING_COMMIT_TIMESTAMP() "
    #                     f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #                     f"AND tag_code = {tag_code} AND file_name = \'{file_name}\' "
    #                 )
    #                 raise
    #             finally:
    #                 os.remove(storage.file)
    #             self.column_times_setter(column_times=3)
    #             self.replace_data_with('user_tag_dw_data', rule_tag.ods_to_dw(hall, tag_code=tag_code))
    #         self.column_times_setter(column_times=3)
    #         self.delete_keyset_data_with('user_tag_dw_data', rule_tag.call_etl_delete_dw_data(hall))
    #         self.upsert_data_with('user_tag_dw_data', rule_tag.call_etl_process_dw_date(hall))
    #
    # @try_exc_func
    # def tag_rules(self, hall: Hall, spanner_db: CDP_BBOS, spanner_config: CDP_Config):
    #     bq = BQ_BaseBBOS(hall)
    #     rule_tag = RuleTag(spanner_db=spanner_db, spanner_config=spanner_config)
    #     rule_tag.get_init_activated(hall)
    #     rule_tag.get_latest_activated(hall)
    #     rule_tag.get_init_deposit(hall)
    #     rule_tag.get_latest_deposit(hall)
    #     bq.get_init_bet_time()
    #     bq.get_latest_bet_time()
    #     bq.get_init_cash()
    #     bq.get_latest_cash()
    #     bq.get_online_month_game()
    #     rule_tag.online_month_game_df = bq.online_month_game_df
    #     bq.get_promotion_type()
    #     rule_tag.promotion_type_df = bq.promotion_type_df
    #     rule_tag.target_cash_df = bq.cash_df
    #     self.column_times_setter(column_times=2)
    #     self.replace_data_with('user_tag_ods_data', rule_tag.churned(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.boss(hall))
    #     self.upsert_data_with('user_tag_ods_data', rule_tag.deposit(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.huge_winner(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.huge_loser(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.frequent_loser(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.game_lover(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.win_lose_reaction(hall))
    #     self.upsert_data_with('user_tag_ods_data', rule_tag.app_user(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.bet_time(hall, bq.bet_time_df))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.arbitrage_suspect(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.login_ub(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.login_pc(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.continuous_deposit(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.deposit_latest_average_record(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.max_bet_per_game(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.week_valid_bet_descend(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.deposit_method(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.hesitate(hall, bq))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.certification(hall))
    #     self.replace_data_with('user_tag_ods_data', rule_tag.term())
    #     self.replace_data_with('user_tag_ods_data', rule_tag.top_promotion_type(hall))
    #     self.update_data_with('user_tag_ods_data', rule_tag.tag_edit(hall))
    #     self.update_data_with('user_tag_ods_data', rule_tag.tag_of_disjoint(hall))
    #
    #     self.column_times_setter(column_times=3)
    #     self.replace_data_with('user_tag_dw_data', rule_tag.ods_to_dw(hall))
    #     self.delete_keyset_data_with('user_tag_dw_data', rule_tag.call_etl_delete_dw_data(hall))
    #     self.upsert_data_with('user_tag_dw_data', rule_tag.call_etl_process_dw_date(hall))
    #
    # @try_exc_func
    # def tag_of_operate(self, hall: Hall, spanner_db: CDP_BBOS, start, end):
    #     start = datetime.strptime(start, '%Y-%m-%d').date()
    #     end = datetime.strptime(end, '%Y-%m-%d').date()
    #
    #     def fill_na(df):
    #         df.loc[df['last_login_date'].isna(), 'last_login_date'] = datetime.strptime('1990-01-01', "%Y-%m-%d")
    #         df.loc[df['activated_date'].isna(), 'activated_date'] = datetime.strptime('1990-01-01', "%Y-%m-%d")
    #         df.loc[df['deposit_count'].isna(), 'deposit_count'] = 0.0
    #         df.loc[df['wagers_total'].isna(), 'wagers_total'] = 0.0
    #         df[["deposit_count", "wagers_total"]] = df[["deposit_count", "wagers_total"]].astype(int)
    #         return df
    #
    #     adaptor = NAPL(spanner_db)
    #     day_count = (end - start).days + 1
    #     napl_path = f'{Environment.ROOT_PATH}/files/spanner/{hall.domain_name}_NAPL.csv'
    #     for target_date in (start + timedelta(n) for n in range(day_count)):
    #         if not os.path.exists(napl_path):
    #             st = time.time()
    #
    #             member_df = adaptor.get_member_df(hall.hall_id, hall.domain_id, '1990-01-01',
    #                                               target_date)
    #             activated_tag = adaptor.get_activated_tag(hall.hall_id, hall.domain_id, "1990-01-01",
    #                                                       target_date)
    #             activated_tag = activated_tag.sort_values(
    #                 by=['hall_id', 'domain_id', 'user_id', 'data_date']).groupby(
    #                 by=['hall_id', 'domain_id', 'user_id']).tail(1).reset_index()
    #             operate_df = adaptor.get_operate_df(hall.hall_id, hall.domain_id, '1990-01-01',
    #                                                 target_date)
    #
    #             member_df = pd.merge(left=member_df, right=activated_tag, how="left",
    #                                  on=["hall_id", "domain_id", "user_id", "user_name"])
    #             member_df = pd.merge(left=member_df, right=operate_df, how="left",
    #                                  on=["hall_id", "domain_id", "user_id"])
    #             member_df = fill_na(member_df)
    #
    #             et = time.time()
    #             print(f"Init data cost cost {et - st} seconds ")
    #             print("-----------------------------------------------------------")
    #         else:
    #             st = time.time()
    #             member_df = pd.read_csv(napl_path)
    #             member_df[["hall_id", "domain_id", "user_id", "deposit_count", "wagers_total"]] = member_df[
    #                 ["hall_id", "domain_id", "user_id", "deposit_count", "wagers_total"]].astype(int)
    #             member_df['user_name'] = member_df['user_name'].astype(str)
    #             member_df["register_date"] = pd.to_datetime(member_df['register_date'])
    #             member_df["last_login_date"] = pd.to_datetime(member_df['last_login_date'])
    #             member_df["activated_date"] = pd.to_datetime(member_df['activated_date'])
    #             member_df['currently_tag_code'] = member_df['currently_tag_code'].astype(float)
    #
    #             # 最後登入日與最後實動日 >= target_date， 表示已經執行過標籤計算！
    #             # 避免累計下注等資訊，而造成貼標錯誤
    #             if target_date <= max(member_df['last_login_date'].max(), member_df['activated_date'].max()):
    #                 print(f'{target_date} is already taged')
    #                 print("-----------------------------------------------------------")
    #                 continue
    #             else:
    #                 s_date = (max(member_df['last_login_date'].max(),
    #                               member_df['activated_date'].max()) + timedelta(days=1)).date()
    #                 print(f'Execute {target_date} ......................................')
    #                 print("-----------------------------------------------------------")
    #
    #             new_member_df = adaptor.get_member_df(hall.hall_id, hall.domain_id, s_date, target_date)
    #             new_member_df = new_member_df[~new_member_df['user_id'].isin(member_df['user_id'])]
    #             member_df = pd.concat([member_df, new_member_df])
    #             member_df = fill_na(member_df)
    #
    #             print(f"Add {len(new_member_df)} new member")
    #             print("-----------------------------------------------------------")
    #
    #             operate_df = adaptor.get_operate_df(hall.hall_id, hall.domain_id, s_date, target_date)
    #             agg_df = pd.concat([member_df, operate_df]).groupby(by=['hall_id', 'domain_id', 'user_id']).agg(
    #                 {"last_login_date": "max", "activated_date": "max", "deposit_count": sum,
    #                  "wagers_total": sum}).reset_index()
    #             member_df = pd.merge(left=member_df[
    #                 ["hall_id", "domain_id", "user_id", "user_name", "register_date", 'currently_tag_code']],
    #                                  right=agg_df, how="left", on=["hall_id", "domain_id", "user_id"])
    #
    #             et = time.time()
    #             print(f"Add data cost cost {et - st} seconds ")
    #             print("-----------------------------------------------------------")
    #
    #         # 將 最後登入時間 與 註冊時間 取最大值為 最後拜訪日
    #         member_df['visited_date'] = member_df[['register_date', 'last_login_date']].max(axis=1)
    #         member_df['tag_code'] = np.nan
    #         member_df['data_date'] = target_date
    #         member_df['created_time'] = datetime.now(pytz.timezone('Asia/Taipei'))
    #         member_df['updated_time'] = datetime.now(pytz.timezone('Asia/Taipei'))
    #
    #         # setting tag code
    #         member_df.loc[(member_df['activated_date'] >= pd.to_datetime(
    #             target_date - timedelta(days=14))), "tag_code"] = 50100  # 成癮客
    #         member_df.loc[(member_df['activated_date'] >= pd.to_datetime(target_date - timedelta(days=14))) & (
    #                 member_df["deposit_count"] > 0) & (
    #                               member_df["wagers_total"] > 0), "tag_code"] = 50101  # 新客
    #         member_df.loc[(member_df['activated_date'] >= pd.to_datetime(target_date - timedelta(days=14))) & (
    #                 member_df["deposit_count"] >= 3) & (
    #                               member_df["wagers_total"] > 0), "tag_code"] = 50102  # 活躍客
    #
    #         member_df.loc[(member_df['activated_date'] < pd.to_datetime(
    #             target_date - timedelta(days=14))), "tag_code"] = 50105  # 流失客
    #         member_df.loc[(member_df['activated_date'] < pd.to_datetime(target_date - timedelta(days=14))) & (
    #                 member_df["deposit_count"] > 0) & (
    #                               member_df["wagers_total"] > 0), "tag_code"] = 50104  # 即將流失客
    #         member_df.loc[(member_df['activated_date'] < pd.to_datetime(target_date - timedelta(days=14))) & (
    #                 member_df["deposit_count"] >= 3) & (
    #                               member_df["wagers_total"] > 0), "tag_code"] = 50103  # 潛在客
    #         member_df.loc[(member_df['activated_date'] < pd.to_datetime(target_date - timedelta(days=60))) & ((
    #                 member_df["deposit_count"] > 0) | (
    #                               member_df["wagers_total"] > 0)), "tag_code"] = 50106  # 封存客
    #
    #         member_df.loc[(member_df['activated_date'] == '1990-01-01') & (
    #                 member_df['visited_date'] >= pd.to_datetime(
    #             target_date - timedelta(days=60))), "tag_code"] = 50107
    #         member_df.loc[(member_df['activated_date'] == '1990-01-01') & (
    #                 member_df['visited_date'] < pd.to_datetime(
    #             target_date - timedelta(days=60))), "tag_code"] = 50108
    #
    #         # insert operate tag day
    #         insert_df = member_df[(member_df['currently_tag_code'] != member_df['tag_code']) & (
    #                 (member_df["visited_date"] >= pd.to_datetime("2021-01-01")) | (
    #                 member_df["activated_date"] >= pd.to_datetime("2021-01-01")))][
    #             ['hall_id', 'domain_id', 'user_id', 'user_name', 'data_date', 'tag_code', 'created_time',
    #              'updated_time']].reset_index(drop=True)
    #         insert_df['tag_code'] = insert_df['tag_code'].astype(int)
    #         self.upsert_data_with('activated_tag_day', insert_df)
    #
    #         member_df['currently_tag_code'] = member_df['tag_code']
    #         member_df[["hall_id", "domain_id", "user_id", "user_name", "register_date", "currently_tag_code",
    #                    "last_login_date", "activated_date", "deposit_count", "wagers_total"]].to_csv(napl_path,
    #                                                                                                  index=False)
    #     # insert operate tag day
    #     self.tag_of_operate_day(hall, spanner_db, str(start), str(end))
    #
    # def tag_of_operate_week(self, hall: Hall, spanner_db: CDP_BBOS, start, end):
    #     adaptor = NAPL(spanner_db)
    #     napl_tag_path = f'{Environment.ROOT_PATH}/files/spanner/{hall.domain_name}_NAPL_tag.csv'
    #     fin_df = adaptor.get_fin_df(start, end)
    #     for index, row in fin_df.iterrows():
    #         target_date = row['fin_week_end'].date() - timedelta(days=1)
    #         # fill tag info
    #         if not os.path.exists(napl_tag_path):
    #             activated_tag = adaptor.get_activated_tag(hall.hall_id, hall.domain_id, "1990-01-01",
    #                                                       target_date)
    #             print(f'Execute {target_date} ......................................')
    #         else:
    #             activated_tag = pd.read_csv(napl_tag_path)
    #             activated_tag[["hall_id", "domain_id", "user_id"]] = activated_tag[
    #                 ["hall_id", "domain_id", "user_id"]].astype(int)
    #             activated_tag['user_name'] = activated_tag['user_name'].astype(str)
    #             activated_tag['currently_tag_code'] = activated_tag['currently_tag_code'].astype(float)
    #             activated_tag["data_date"] = pd.to_datetime(activated_tag['data_date'])
    #
    #             # 最後登入日與最後實動日 >= target_date， 表示已經執行過標籤計算！
    #             # 避免累計下注等資訊，而造成貼標錯誤
    #             if target_date <= activated_tag['data_date'].max():
    #                 print(f'{target_date} is already filled')
    #             else:
    #                 s_date = activated_tag['data_date'].max().date() + timedelta(days=1)
    #                 print(f'Execute {target_date} ......................................')
    #
    #                 new_activated_tag = adaptor.get_activated_tag(hall.hall_id, hall.domain_id, s_date,
    #                                                               target_date)
    #                 activated_tag = pd.concat([activated_tag, new_activated_tag]).reset_index(drop=True)
    #
    #                 target_activated_tag_df = activated_tag[
    #                     activated_tag['data_date'] <= row['fin_week_end']].reset_index(drop=True)
    #                 target_activated_tag_df = target_activated_tag_df.sort_values(
    #                     by=['hall_id', 'domain_id', 'user_id', 'user_name', 'data_date']).groupby(
    #                     by=['hall_id', 'domain_id', 'user_id', "user_name"]).tail(1)[
    #                     ['hall_id', 'domain_id', 'user_id', 'user_name', "currently_tag_code"]].reset_index(drop=True)
    #                 target_activated_tag_df = target_activated_tag_df.rename(columns={"currently_tag_code": "tag_code"})
    #                 target_activated_tag_df['tag_code'] = target_activated_tag_df['tag_code'].astype(int)
    #                 target_activated_tag_df['fin_year'] = row['fin_year']
    #                 target_activated_tag_df['fin_month'] = row['fin_month']
    #                 target_activated_tag_df['fin_week'] = row['fin_week']
    #                 target_activated_tag_df['created_time'] = datetime.now(pytz.timezone('Asia/Taipei'))
    #                 target_activated_tag_df['updated_time'] = datetime.now(pytz.timezone('Asia/Taipei'))
    #                 self.column_times_setter(3)
    #                 self.upsert_data_with("activated_tag_ods_week", target_activated_tag_df)
    #
    #         ods_week_df = adaptor.generate_activated_ods_week(hall.hall_id, hall.domain_id,
    #                                                           row['fin_year'], row['fin_month'],
    #                                                           row['fin_week'], 0).rename(
    #             columns={'tag_code': 'week_tag_code'})  # 『本』週營運標籤
    #         ods_one_week_df = adaptor.generate_activated_ods_week(hall.hall_id, hall.domain_id,
    #                                                               row['fin_year'], row['fin_month'],
    #                                                               row['fin_week'], 7).rename(
    #             columns={'tag_code': 'one_weeks_tag_code'})  # 『上』週營運標籤
    #         ods_two_week_df = adaptor.generate_activated_ods_week(hall.hall_id, hall.domain_id,
    #                                                               row['fin_year'], row['fin_month'],
    #                                                               row['fin_week'], 14).rename(
    #             columns={'tag_code': 'two_weeks_tag_code'})  # 『上上』週營運標籤
    #         dw_one_week_df = adaptor.generate_activated_dw_week(hall.hall_id, hall.domain_id,
    #                                                             row['fin_year'], row['fin_month'],
    #                                                             row['fin_week'], 7)  # 『上次』週營運標籤
    #
    #         dw_week_df = pd.merge(ods_week_df, ods_one_week_df, how='left',
    #                               on=['hall_id', 'domain_id', 'user_id', 'user_name']).reset_index(drop=True)
    #         dw_week_df = pd.merge(dw_week_df, ods_two_week_df, how='left',
    #                               on=['hall_id', 'domain_id', 'user_id', 'user_name']).reset_index(drop=True)
    #         dw_week_df = pd.merge(dw_week_df, dw_one_week_df, how='left',
    #                               on=['hall_id', 'domain_id', 'user_id', 'user_name']).reset_index(drop=True)
    #         dw_week_df['fin_year'] = row['fin_year']
    #         dw_week_df['fin_month'] = row['fin_month']
    #         dw_week_df['fin_week'] = row['fin_week']
    #
    #         dw_week_df['last_week_step'] = 0
    #         dw_week_df.loc[((dw_week_df['two_weeks_tag_code'].isna()) | (
    #             dw_week_df['two_weeks_tag_code'].isin([50102]))) & (
    #                            dw_week_df['one_weeks_tag_code'].isin([50102])), "last_week_step"] = 1
    #         dw_week_df.loc[((dw_week_df['two_weeks_tag_code'].isna()) | (
    #             dw_week_df['two_weeks_tag_code'].isin([50107, 50108]))) & (
    #                            dw_week_df['one_weeks_tag_code'].isin(
    #                                [50100, 50101, 50102])), "last_week_step"] = 2
    #         dw_week_df.loc[((dw_week_df['two_weeks_tag_code'].isna()) | (
    #             dw_week_df['two_weeks_tag_code'].isin([50100, 50101]))) & (
    #                            dw_week_df['one_weeks_tag_code'].isin([50102])), "last_week_step"] = 3
    #         dw_week_df.loc[((dw_week_df['two_weeks_tag_code'].isna()) | (
    #             dw_week_df['two_weeks_tag_code'].isin([50103, 50104, 50105]))) & (
    #                            dw_week_df['one_weeks_tag_code'].isin([50101, 50102])), "last_week_step"] = 4
    #         dw_week_df.loc[((dw_week_df['two_weeks_tag_code'].isna()) | (
    #             dw_week_df['two_weeks_tag_code'].isin([50105, 50106]))) & (
    #                            dw_week_df['one_weeks_tag_code'].isin([50101, 50102])), "last_week_step"] = 5
    #         dw_week_df.loc[((dw_week_df['two_weeks_tag_code'].isna()) | (
    #             dw_week_df['two_weeks_tag_code'].isin([50100, 50101, 50102]))) & (
    #                            dw_week_df['one_weeks_tag_code'].isin(
    #                                [50103, 50104, 50105])), "last_week_step"] = 6
    #         dw_week_df.loc[((dw_week_df['two_weeks_tag_code'].isna()) | (
    #             dw_week_df['two_weeks_tag_code'].isin([50103, 50104, 50105]))) & (
    #                            dw_week_df['one_weeks_tag_code'].isin([50106])), "last_week_step"] = 7
    #
    #         dw_week_df['this_week_step'] = 0
    #         dw_week_df.loc[((dw_week_df['one_weeks_tag_code'].isna()) | (
    #             dw_week_df['one_weeks_tag_code'].isin([50102]))) & (
    #                            dw_week_df['week_tag_code'].isin([50102])), "this_week_step"] = 1
    #         dw_week_df.loc[((dw_week_df['one_weeks_tag_code'].isna()) | (
    #             dw_week_df['one_weeks_tag_code'].isin([50107, 50108]))) & (
    #                            dw_week_df['week_tag_code'].isin([50100, 50101, 50102])), "this_week_step"] = 2
    #         dw_week_df.loc[((dw_week_df['one_weeks_tag_code'].isna()) | (
    #             dw_week_df['one_weeks_tag_code'].isin([50100, 50101]))) & (
    #                            dw_week_df['week_tag_code'].isin([50102])), "this_week_step"] = 3
    #         dw_week_df.loc[((dw_week_df['one_weeks_tag_code'].isna()) | (
    #             dw_week_df['one_weeks_tag_code'].isin([50103, 50104, 50105]))) & (
    #                            dw_week_df['week_tag_code'].isin([50101, 50102])), "this_week_step"] = 4
    #         dw_week_df.loc[((dw_week_df['one_weeks_tag_code'].isna()) | (
    #             dw_week_df['one_weeks_tag_code'].isin([50105, 50106]))) & (
    #                            dw_week_df['week_tag_code'].isin([50101, 50102])), "this_week_step"] = 5
    #         dw_week_df.loc[((dw_week_df['one_weeks_tag_code'].isna()) | (
    #             dw_week_df['one_weeks_tag_code'].isin([50100, 50101, 50102]))) & (
    #                            dw_week_df['week_tag_code'].isin([50103, 50104, 50105])), "this_week_step"] = 6
    #         dw_week_df.loc[((dw_week_df['one_weeks_tag_code'].isna()) | (
    #             dw_week_df['one_weeks_tag_code'].isin([50103, 50104, 50105]))) & (
    #                            dw_week_df['week_tag_code'].isin([50106])), "this_week_step"] = 7
    #         dw_week_df[
    #             ['dw_last_week_step', 'dw_this_week_step', 'dw_two_weeks_tag_code', 'dw_one_weeks_tag_code',
    #              'dw_week_tag_code', 'two_weeks_tag_code', 'one_weeks_tag_code', 'week_tag_code']] = dw_week_df[
    #             ['dw_last_week_step', 'dw_this_week_step', 'dw_two_weeks_tag_code', 'dw_one_weeks_tag_code',
    #              'dw_week_tag_code', 'two_weeks_tag_code', 'one_weeks_tag_code', 'week_tag_code']].fillna(0)
    #         dw_week_df = dw_week_df[
    #             (dw_week_df['dw_last_week_step'] != 0) | (dw_week_df['dw_this_week_step'] != 0) | (
    #                     dw_week_df['last_week_step'] != 0) | (
    #                     dw_week_df['this_week_step'] != 0)].reset_index(drop=True)
    #
    #         index = ((dw_week_df['last_week_step'] == 0) & (dw_week_df['this_week_step'] == 0)) | (
    #                 (dw_week_df['last_week_step'] != 0) & (dw_week_df['this_week_step'] == 0))
    #         dw_week_df.loc[
    #             index, ['last_week_step', 'this_week_step', 'two_weeks_tag_code', 'one_weeks_tag_code',
    #                     'week_tag_code']] = dw_week_df.loc[
    #             index, ['dw_last_week_step', 'dw_this_week_step', 'dw_two_weeks_tag_code',
    #                     'dw_one_weeks_tag_code', 'dw_week_tag_code']].values
    #         index = ((dw_week_df["last_week_step"] == 0) & (dw_week_df['this_week_step'] != 0))
    #         dw_week_df.loc[index, ['last_week_step', 'two_weeks_tag_code']] = dw_week_df.loc[
    #             index, ['dw_this_week_step', 'dw_one_weeks_tag_code']].values
    #         dw_week_df.fillna(0, inplace=True)
    #         dw_week_df = dw_week_df.replace(r'^\s*$', 0, regex=True)
    #         dw_week_df['created_time'] = datetime.now(pytz.timezone('Asia/Taipei'))
    #         dw_week_df['updated_time'] = datetime.now(pytz.timezone('Asia/Taipei'))
    #         dw_week_df = dw_week_df[
    #             ['hall_id', 'domain_id', 'user_id', 'user_name', 'last_week_step', 'this_week_step',
    #              'two_weeks_tag_code', 'one_weeks_tag_code', 'week_tag_code', 'fin_year', 'fin_month',
    #              'fin_week', 'created_time', 'updated_time']]
    #         dw_week_df[
    #             ['hall_id', 'domain_id', 'user_id', 'last_week_step', 'this_week_step', 'two_weeks_tag_code',
    #              'one_weeks_tag_code', 'week_tag_code', 'fin_year', 'fin_month', 'fin_week']] = dw_week_df[
    #             ['hall_id', 'domain_id', 'user_id', 'last_week_step', 'this_week_step', 'two_weeks_tag_code',
    #              'one_weeks_tag_code', 'week_tag_code', 'fin_year', 'fin_month', 'fin_week']].astype(int)
    #         self.column_times_setter(5)
    #         self.delete_keyset_data_with("activated_tag_dw_week", dw_week_df[
    #             ['hall_id', 'domain_id', 'user_id', 'fin_year', 'fin_month', 'fin_week']])
    #         self.upsert_data_with("activated_tag_dw_week", dw_week_df)
    #         activated_tag.to_csv(napl_tag_path, index=False)
    #
    # def tag_of_operate_day(self, hall: Hall, spanner_db: CDP_BBOS, start, end):
    #     adaptor = NAPL(spanner_db)
    #     if os.path.exists(f'{Environment.ROOT_PATH}/files/spanner/{hall.domain_name}_NAPL_tag_day.csv'):
    #         print('NAPL_tag_day file exist, ignore initial')
    #         file_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/spanner/{hall.domain_name}_NAPL_tag_day.csv')
    #         max_date = file_df.sort_values('data_date').groupby(['hall_id', 'domain_id']).tail(1)['data_date'].values[0]
    #         file_df = df_type_format_tag(file_df)
    #         tag_day_df = self.select_data_with(
    #             ['hall_id', 'domain_id', 'user_id', 'ag_name', 'user_name', 'tag_code', 'data_date'],
    #             "SELECT napl.hall_id, napl.domain_id, napl.user_id, member.ag_name, napl.user_name, "
    #             "tag_code, data_date "
    #             "FROM activated_tag_day napl "
    #             "JOIN member_info member "
    #             "ON napl.hall_id = member.hall_id AND "
    #             "napl.domain_id = member.domain_id AND "
    #             "napl.user_id = member.user_id "
    #             f"WHERE napl.hall_id = {hall.hall_id} AND napl.domain_id = {hall.domain_id} "
    #             f"AND data_date > \'{max_date}\' "
    #         )
    #         tag_day_df = df_type_format_tag(tag_day_df)
    #         new_tag_day_df = pd.concat([file_df, tag_day_df], ignore_index=True)
    #         new_tag_day_df.to_csv(f'{Environment.ROOT_PATH}/files/spanner/{hall.domain_name}_NAPL_tag_day.csv', index=False)
    #     else:
    #         tag_day_df = self.select_data_with(
    #             ['hall_id', 'domain_id', 'user_id', 'ag_name', 'user_name', 'tag_code', 'data_date'],
    #             "SELECT napl.hall_id, napl.domain_id, napl.user_id, member.ag_name, napl.user_name, "
    #             "tag_code, data_date "
    #             "FROM activated_tag_day napl "
    #             "JOIN member_info member "
    #             "ON napl.hall_id = member.hall_id AND "
    #             "napl.domain_id = member.domain_id AND "
    #             "napl.user_id = member.user_id "
    #             f"WHERE napl.hall_id = {hall.hall_id} AND napl.domain_id = {hall.domain_id} "
    #         )
    #         tag_day_df = df_type_format_tag(tag_day_df)
    #         tag_day_df.to_csv(f'{Environment.ROOT_PATH}/files/spanner/{hall.domain_name}_NAPL_tag_day.csv', index=False)
    #     tag_day_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/spanner/{hall.domain_name}_NAPL_tag_day.csv')
    #     while start <= end:
    #         print(f"start: {start}")
    #         target_day_df = tag_day_df.loc[tag_day_df['data_date'] <= start]
    #         # remove adjacent duplicate tag_code
    #         cols = ['hall_id', 'domain_id', 'user_id', 'tag_code']
    #         target_day_df = target_day_df.loc[(target_day_df[cols].shift(-1) != target_day_df[cols]).any(axis=1)]
    #         target_df = target_day_df.sort_values('data_date').groupby(['hall_id', 'domain_id', 'user_id']).tail(4)
    #         target_df = df_type_format_tag(target_df)
    #
    #         this_df = target_df[target_df.sort_values('data_date', ascending=[False]).groupby(
    #             ['hall_id', 'domain_id', 'user_id']).cumcount() == 0].rename(
    #             columns={'tag_code': 'this_tag_code'})
    #         last_df = target_df[target_df.sort_values('data_date', ascending=[False]).groupby(
    #             ['hall_id', 'domain_id', 'user_id']).cumcount() == 1].rename(
    #             columns={'tag_code': 'last_tag_code', 'data_date': 'last_date'}).drop(columns=['ag_name'])
    #         last_two_df = target_df[target_df.sort_values('data_date', ascending=[False]).groupby(
    #             ['hall_id', 'domain_id', 'user_id']).cumcount() == 2].rename(
    #             columns={'tag_code': 'last_two_tag_code', 'data_date': 'last_two_date'}).drop(columns=['ag_name'])
    #         last_three_df = target_df[target_df.sort_values('data_date', ascending=[False]).groupby(
    #             ['hall_id', 'domain_id', 'user_id']).cumcount() == 3].rename(
    #             columns={'tag_code': 'last_three_tag_code', 'data_date': 'last_three_date'}).drop(columns=['ag_name'])
    #
    #         dw_day_df = pd.merge(
    #             this_df, last_df, how='left',
    #             on=['hall_id', 'domain_id', 'user_id', 'user_name']).reset_index(drop=True)
    #         dw_day_df = pd.merge(
    #             dw_day_df, last_two_df, how='left',
    #             on=['hall_id', 'domain_id', 'user_id', 'user_name']).reset_index(drop=True)
    #         dw_day_df = pd.merge(
    #             dw_day_df, last_three_df, how='left',
    #             on=['hall_id', 'domain_id', 'user_id', 'user_name']).reset_index(drop=True)
    #
    #         dw_day_df['last_two_date_diff'] = (dw_day_df['last_two_date'] - dw_day_df['last_three_date']).dt.days
    #         dw_day_df['last_date_diff'] = (dw_day_df['last_date'] - dw_day_df['last_two_date']).dt.days
    #         dw_day_df['this_date_diff'] = (dw_day_df['data_date'] - dw_day_df['last_date']).dt.days
    #
    #         dw_day_df['this_last_diff_sum'] = dw_day_df['this_date_diff'] + dw_day_df['last_date_diff']
    #         dw_day_df['all_diff_sum'] = (dw_day_df['data_date'] - dw_day_df['last_three_date']).dt.days
    #
    #         last_day = str(datetime.strptime(start, '%Y-%m-%d').date() - timedelta(days=1))
    #         last_two_day = str(datetime.strptime(start, '%Y-%m-%d').date() - timedelta(days=2))
    #         last_three_day = str(datetime.strptime(start, '%Y-%m-%d').date() - timedelta(days=3))
    #
    #         dw_day_df.loc[(dw_day_df.last_tag_code == 50102) & (dw_day_df.all_diff_sum > 3), 'last_three_tag_code'] = 50102
    #         dw_day_df.loc[(dw_day_df.last_tag_code == 50102) & (dw_day_df.this_last_diff_sum > 2), 'last_two_tag_code'] = 50102
    #         dw_day_df.loc[(dw_day_df.last_two_tag_code == 50102) & (dw_day_df.all_diff_sum > 3), 'last_three_tag_code'] = 50102
    #
    #         dw_day_df.loc[(dw_day_df.last_tag_code == 50102) & (dw_day_df.last_two_tag_code.isna()), 'last_two_tag_code'] = 50102
    #         dw_day_df.loc[(dw_day_df.last_two_tag_code == 50102) & (dw_day_df.last_three_tag_code.isna()), 'last_three_tag_code'] = 50102
    #
    #         dw_day_df.loc[(dw_day_df.data_date == last_day) & (dw_day_df.this_tag_code == 50102), 'last_three_tag_code'] = dw_day_df['last_two_tag_code']
    #         dw_day_df.loc[(dw_day_df.data_date == last_day) & (dw_day_df.this_tag_code == 50102), 'last_two_tag_code'] = dw_day_df['last_tag_code']
    #         dw_day_df.loc[(dw_day_df.data_date == last_day) & (dw_day_df.this_tag_code == 50102), 'last_tag_code'] = 50102
    #
    #         dw_day_df.loc[(dw_day_df.data_date == last_two_day) & (dw_day_df.this_tag_code == 50102), 'last_three_tag_code'] = dw_day_df['last_tag_code']
    #         dw_day_df.loc[(dw_day_df.data_date == last_two_day) & (dw_day_df.this_tag_code == 50102), 'last_two_tag_code'] = dw_day_df['this_tag_code']
    #         dw_day_df.loc[(dw_day_df.data_date == last_two_day) & (dw_day_df.this_tag_code == 50102), 'last_tag_code'] = 50102
    #
    #         dw_day_df.loc[(dw_day_df.data_date <= last_three_day) & (dw_day_df.this_tag_code == 50102), 'last_three_tag_code'] = dw_day_df['this_tag_code']
    #         dw_day_df.loc[(dw_day_df.data_date <= last_three_day) & (dw_day_df.this_tag_code == 50102), 'last_two_tag_code'] = 50102
    #         dw_day_df.loc[(dw_day_df.data_date <= last_three_day) & (dw_day_df.this_tag_code == 50102), 'last_tag_code'] = 50102
    #
    #         dw_day_df = adaptor.get_step(dw_day_df, 'this_day_step', 'this_tag_code', 'last_tag_code')
    #         dw_day_df = adaptor.get_step(dw_day_df, 'last_day_step', 'last_tag_code', 'last_two_tag_code')
    #         dw_day_df = adaptor.get_step(dw_day_df, 'last_two_day_step', 'last_two_tag_code', 'last_three_tag_code')
    #         dw_day_df.fillna(0, inplace=True)
    #         dw_day_df['init_flag'] = None
    #         dw_day_df.loc[dw_day_df.data_date == start, 'init_flag'] = True
    #         dw_day_df.loc[dw_day_df.data_date != start, 'init_flag'] = False
    #         dw_day_df.loc[(dw_day_df.this_day_step == 1) & (dw_day_df.last_day_step != 1), 'init_flag'] = True
    #
    #         dw_day_df = dw_day_df[dw_day_df['this_day_step'] != 0]
    #         # dw_day_df = dw_day_df[dw_day_df['last_day_step'] != 0]
    #         dw_day_df['data_date'] = start
    #         dw_day_df = dw_day_df[['hall_id', 'domain_id', 'user_id', 'ag_name', 'user_name', 'data_date',
    #                                'this_tag_code', 'last_tag_code', 'last_two_tag_code', 'last_three_tag_code',
    #                                'this_day_step', 'last_day_step', 'last_two_day_step', 'init_flag']]
    #
    #         dw_day_df['data_date'] = dw_day_df['data_date'].astype(str)
    #         dw_day_df = dw_day_df.merge(Environment.date_record, how='left', left_on='data_date', right_on='date')
    #         dw_day_df = dw_day_df.drop(
    #             columns=['fin_week_start', 'fin_week_end', 'fin_month_weeks', 'fin_M_ym01']
    #         ).rename(
    #             columns={'financial_year': 'fin_year', 'financial_month': 'fin_month', 'financial_week': 'fin_week'})
    #
    #         self.column_times_setter(2)
    #         dw_day_df = df_type_format_tag(dw_day_df)
    #         dw_day_df['data_date'] = dw_day_df['data_date'].astype(str)
    #         dw_day_df['updated_time'] = spanner.COMMIT_TIMESTAMP
    #         self.replace_data_with("activated_tag_dw_day", dw_day_df)
    #         start = datetime.strptime(start, '%Y-%m-%d').date()
    #         start += timedelta(days=1)
    #         start = str(start)
    #
    # @try_exc_func
    # def tag_sync_match(self, hall: Hall, api_service: BBOSApi, start, end):
    #     seconds_diff = int((datetime.strptime(end, '%Y-%m-%d %H:%M:%S')-datetime.strptime(start, '%Y-%m-%d %H:%M:%S')).total_seconds())
    #
    #     self.table_name = 'user_tag_sync_batch'
    #     self.statement = (
    #         "SELECT hall_id, domain_id, "
    #         "batch_id, tag_name, tag_id, icon_id, icon_name, "
    #         "file_name, file_path, "
    #         "sync_type, sync_date_type, sync_date_begin, sync_date_end, "
    #         "ag_name, user_level_id, user_level_exclude, "
    #         "activated_date_begin, activated_date_end, "
    #         "tag_str, tag_str_exclude, "
    #         "user_step, user_activity "
    #         "FROM user_tag_sync_batch "
    #         f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #         f"AND tag_enabled IS TRUE "
    #         f"AND ("
    #         f"    sync_date_type = 2 "
    #         f"OR "
    #         f"    (sync_date_type = 1 AND DATE(CURRENT_TIMESTAMP(), 'America/New_York') BETWEEN sync_date_begin AND sync_date_end) "
    #         f")"
    #         f"AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), operator_time, SECOND) <= {seconds_diff} "
    #     )
    #     self.mode = self.QUERY_MODE
    #     self.exec()
    #     if len(self.fetch_data) > 0:
    #         columns = [
    #             'hall_id', 'domain_id',
    #             'batch_id', 'tag_name', 'tag_id', 'icon_id', 'icon_name',
    #             'file_name', 'file_path',
    #             'sync_type', 'sync_date_type', 'sync_date_begin', 'sync_date_end',
    #             'ag_name', 'user_level_id', 'user_level_exclude',
    #             'activated_date_begin', 'activated_date_end',
    #             'tag_str', 'tag_str_exclude',
    #             'user_step', 'user_activity'
    #         ]
    #         api_service.token_api()  # 取得access token
    #         for batch in self.fetch_data:
    #             batch_df = pd.DataFrame([batch], columns=columns)
    #             batch_id = batch_df['batch_id'].values[0]
    #             tag_name = batch_df['tag_name'].values[0]
    #             ag_name = batch_df['ag_name'].values[0]  # 代理名稱
    #             user_level_id = batch_df['user_level_id'].values[0]  # 會員層級ID
    #             user_level_exclude = batch_df['user_level_exclude'].values[0]  # 會員層級ID(排除)
    #             activated_date_begin = batch_df['activated_date_begin'].values[0]  # 起始實動日期
    #             activated_date_end = batch_df['activated_date_end'].values[0]  # 結束實動日期
    #             tag_str = batch_df['tag_str'].values[0]  # 標籤組合
    #             tag_str_exclude = batch_df['tag_str_exclude'].values[0]  # 標籤組合(排除)
    #             user_step = batch_df['user_step'].values[0]  # 會員生命週期
    #             user_activity = batch_df['user_activity'].values[0]  # vip活躍度
    #
    #             file_name = batch_df['file_name'].values[0]
    #             file_path = batch_df['file_path'].values[0]
    #
    #             if file_name and file_path:
    #                 storage = GCS('ghr-cdp', f'{Environment.ROOT_PATH}/cert/gcp-ghr-storage.json')
    #                 storage.blob = f'{file_path}{file_name}'
    #                 storage.file = f'{Environment.ROOT_PATH}/files/{file_name}'
    #                 storage.mode = storage.DOWNLOAD_MODE
    #                 storage.exec()
    #                 file_df = pd.read_csv(storage.file)
    #                 username_condition = "','".join(file_df['會員名稱'])
    #
    #                 user_id_df = self.select_data_with(
    #                     columns=['hall_id', 'domain_id', 'user_id'],
    #                     statement="SELECT hall_id, domain_id, user_id "
    #                               "FROM member_info WHERE "
    #                               f"hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #                               f"AND user_name IN ('{username_condition}') "
    #                 )
    #                 result_id_df = user_id_df
    #                 os.remove(storage.file)
    #             else:
    #                 ag_name_condition = (
    #                     f"AND ag_name = '{ag_name}' " if ag_name is not None else "AND 1=1 "
    #                 )
    #                 user_level_condition = (
    #                     f"AND user_level_id = {user_level_id} " if user_level_id is not None else "AND 1=1 "
    #                 )
    #                 user_level_exclude_condition = (
    #                     f"AND user_level_id NOT IN ({user_level_exclude}) " if user_level_exclude is not None else "AND 1=1 "
    #                 )
    #                 if tag_str:
    #                     tag_str_condition = "AND ("
    #                     tag_list = tag_str.split(",")
    #                     for tag in tag_list:
    #                         tag_str_condition += f"tag_str LIKE '%{tag}%' AND "
    #                     tag_str_condition += "1=1 ) "
    #                 else:
    #                     tag_str_condition = "AND 1=1 "
    #                 if tag_str_exclude:
    #                     tag_str_exclude_condition = "AND ("
    #                     tag_exclude_list = tag_str_exclude.split(",")
    #                     for tag_ex in tag_exclude_list:
    #                         tag_str_exclude_condition += f"tag_str NOT LIKE '%{tag_ex}%' AND "
    #                     tag_str_exclude_condition += "1=1 ) "
    #                 else:
    #                     tag_str_exclude_condition = "AND 1=1 "
    #                 activated_date_condition = (
    #                     f"AND last_activated_date BETWEEN '{activated_date_begin}' AND '{activated_date_end}' "
    #                 )
    #                 member_tag_df = self.select_data_with(
    #                     ['hall_id', 'domain_id', 'user_id'],
    #                     "SELECT hall_id, domain_id, user_id "
    #                     "FROM user_tag_dw_data "
    #                     f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #                     f"{ag_name_condition} "
    #                     f"{user_level_condition} "
    #                     f"{user_level_exclude_condition} "
    #                     f"{activated_date_condition} "
    #                     f"{tag_str_condition} "
    #                     f"{tag_str_exclude_condition} "
    #                 )
    #                 result_id_df = member_tag_df
    #                 if user_step:
    #                     financial_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/financial_date_2007-2049.csv', index_col="date")
    #                     data_date = str(date.today() - timedelta(days=2))
    #                     user_step_df = self.select_data_with(
    #                         ['hall_id', 'domain_id', 'user_id'],
    #                         "SELECT hall_id, domain_id, user_id "
    #                         "FROM activated_tag_dw_day "
    #                         f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #                         f"AND this_day_step = {user_step} "
    #                         f"AND data_date = '{data_date}' "
    #                     )
    #                     result_id_df = pd.merge(left=result_id_df, right=user_step_df,
    #                                             on=['hall_id', 'domain_id', 'user_id'],
    #                                             how='inner')
    #                 if user_activity:
    #                     user_activity_list = user_activity.split(",")
    #                     user_activity_df = self.select_data_with(
    #                         ['hall_id', 'domain_id', 'user_id', 'action_score'],
    #                         "SELECT hall_id, domain_id, user_id, AVG(action_score) "
    #                         "FROM user_active_data "
    #                         f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #                         f"AND data_date "
    #                         f"BETWEEN '{str(date.today() - timedelta(days=8))}' AND '{str(date.today() - timedelta(days=2))}' "
    #                         f"GROUP BY hall_id, domain_id, user_id "
    #                     )
    #                     activity_id_df = pd.DataFrame()
    #                     for level in user_activity_list:
    #                         if int(level) == 0:
    #                             activity_id_df = pd.concat(
    #                                 [activity_id_df,
    #                                  user_activity_df[user_activity_df['action_score'].between(0, 0.17)]],
    #                                 ignore_index=True)
    #                         if int(level) == 1:
    #                             activity_id_df = pd.concat(
    #                                 [activity_id_df,
    #                                  user_activity_df[user_activity_df['action_score'].between(0.17, 0.34)]],
    #                                 ignore_index=True)
    #                         if int(level) == 2:
    #                             activity_id_df = pd.concat(
    #                                 [activity_id_df,
    #                                  (user_activity_df[user_activity_df['action_score'].between(0.34, 0.51)])],
    #                                 ignore_index=True)
    #                         if int(level) == 3:
    #                             activity_id_df = pd.concat(
    #                                 [activity_id_df,
    #                                  (user_activity_df[user_activity_df['action_score'].between(0.51, 0.68)])],
    #                                 ignore_index=True)
    #                         if int(level) == 4:
    #                             activity_id_df = pd.concat(
    #                                 [activity_id_df,
    #                                  (user_activity_df[user_activity_df['action_score'].between(0.68, 0.85)])],
    #                                 ignore_index=True)
    #                         if int(level) == 5:
    #                             activity_id_df = pd.concat(
    #                                 [activity_id_df,
    #                                  (user_activity_df[user_activity_df['action_score'].between(0.85, 1)])],
    #                                 ignore_index=True)
    #                     result_id_df = pd.merge(left=result_id_df, right=activity_id_df,
    #                                             on=['hall_id', 'domain_id', 'user_id'],
    #                                             how='inner')
    #
    #             user_id_list = result_id_df['user_id'].values.tolist()
    #             print(f"batch_id: {batch_id} tag_name: {tag_name}, match_count:{len(user_id_list)} ")
    #             # 更新batch 條件符合人數 更新時間
    #             self.update_data_dml_with(
    #                 statement=
    #                 "UPDATE user_tag_sync_batch "
    #                 f"SET "
    #                 f"match_user_count = {len(user_id_list)}, "
    #                 "updated_time = PENDING_COMMIT_TIMESTAMP() "
    #                 f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #                 f"AND batch_id = {batch_id} "
    #             )
    #
    # @try_exc_func
    # def tag_sync(self, hall: Hall, api_service: BBOSApi, spanner_db: CDP_BBOS):
    #     sync_tag = SyncTag(spanner_db=spanner_db)
    #     api_service.token_api()
    #     batch_list = sync_tag.get_batch(hall)
    #     for batch in batch_list:
    #         sync_tag.get_match_user(hall, batch)
    #         # # test user
    #         # sync_tag.origin_match_user_df = self.select_data_with(
    #         #     ['hall_id', 'domain_id', 'user_id'],
    #         #     "SELECT hall_id, domain_id, user_id "
    #         #     "FROM member_info "
    #         #     f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #         #     # f"AND user_id IN (1171469, 1221004, 1248351) "
    #         #     "AND user_id IN (1221004, 1248351, 1430378, 1246431) "
    #         # )
    #         match_user_count = len(sync_tag.origin_match_user_df)
    #         if match_user_count > 0:
    #             sync_tag.get_sync_group_user(hall)
    #             sync_tag.get_tagged_user(hall, api_service)
    #             sync_tag.get_new_tag_user(api_service)
    #             sync_tag.tag_group(hall)
    #             sync_tag.tag_campaign(hall)
    #             sync_tag.batch_tag_user(api_service)
    #             api_service.token_api()
    #             sync_tag.tag_user_count(api_service)
    #
    #             # 更新batch 條件符合人數 標籤總人數 標籤id icon 更新時間
    #             sync_tag.campaign_id = 0 if sync_tag.campaign_id is None else sync_tag.campaign_id
    #             sync_tag.tag_count = 0 if sync_tag.tag_count is None else sync_tag.tag_count
    #             self.update_data_dml_with(
    #                 statement=
    #                 "UPDATE user_tag_sync_batch "
    #                 f"SET tag_id = {sync_tag.tag_id}, "
    #                 f"icon_id = {sync_tag.icon_id}, "
    #                 f"icon_name = '{sync_tag.icon_name}', "
    #                 f"match_user_count = {match_user_count}, "
    #                 f"actual_user_count = {sync_tag.tag_count}, "
    #                 f"activity_id = {sync_tag.campaign_id}, "
    #                 "updated_time = PENDING_COMMIT_TIMESTAMP() "
    #                 f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
    #                 f"AND batch_id = {sync_tag.batch_id} "
    #             )
    #         else:
    #             print(f"batch_id: {sync_tag.batch_id} tag_name: {sync_tag.tag_name}, no match user ")