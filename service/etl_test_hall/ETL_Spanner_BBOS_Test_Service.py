import pandas as pd
from datetime import datetime, timedelta
import Environment
from Adaptors.Hall import Hall
from Adaptors.SpannerAdaptor import CDP_BBOS
from util.dataframe_process import df_timezone_format
from Adaptors.TelegramAdaptor import Notify
from util.ip_database import search_ip_multiple
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


class ETL_Spanner_BBOS_Test_Service(CDP_BBOS):
    def __init__(self, spanner_db: str, bq_db):
        super(ETL_Spanner_BBOS_Test_Service, self).__init__(database=spanner_db)
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
            FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.user_info`  
            WHERE
            domain_id = {hall.domain_id} 
            AND role = 1 
            AND (DATE(created_at) BETWEEN '{start}' AND '{end}' 
            OR 
            DATE(created_time) BETWEEN '{start}' AND '{end}') 
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        # print(self.bq_db.statement)
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
                FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.login_log` log 
                JOIN `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.User_View` member 
                    ON log.user_id = member.user_id 
            LEFT JOIN (
                SELECT user_id, 'App' AS host, data_date 
                FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.login_log` 
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
        # print(self.bq_db.statement)
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
              `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.bet_analysis_jackpot` bet
            JOIN
              `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.user_info` member
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
        # print(self.bq_db.statement)
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
                FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.deposit_record`
                    WHERE status <> 0 
                    GROUP BY user_id, data_date, financial_year , financial_month , financial_week 
            ) deposit 
            JOIN `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.user_info` member 
            ON deposit.user_id = member.user_id 
            LEFT JOIN ( 
                SELECT user_id, SUM(real_amount) AS real_amount, SUM(fee) AS withdraw_fee, 
                SUM(withdraw_count) AS withdraw_count, 
                data_date 
                FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.withdraw_record` 
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
                FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.deposit_record` 
                    WHERE status <> 0 GROUP BY user_id, data_date) deposit 
            RIGHT JOIN ( 
                SELECT user_id, SUM(real_amount) AS real_amount, SUM(fee) AS withdraw_fee, 
                SUM(withdraw_count) AS withdraw_count, 
                data_date, financial_year , financial_month , financial_week 
                FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.withdraw_record` 
                    WHERE status <> 0 
                    GROUP BY user_id, data_date, financial_year , financial_month , financial_week 
            ) withdraw ON deposit.user_id = withdraw.user_id 
                AND withdraw.data_date = deposit.data_date 
            JOIN `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.user_info` member 
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
        # print(self.bq_db.statement)
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
            self.replace_data_with('deposit_withdraw_record', insert_df)

    @try_exc_func
    def offer(self, hall: Hall, start, end):
        self.bq_db.statement = (
            f"""
            SELECT offer.user_id, 
            member.parent_username AS ag_name, 
            opcode, opcode_name, premium_total, premium_amount, 
            data_date, financial_year, financial_month, financial_week, 
            member.level_id AS user_level_id, member.level_name AS user_level 
            FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.offer_info` offer 
            JOIN `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.user_info` member 
            ON member.user_id = offer.user_id 
            WHERE offer.data_date BETWEEN '{start}' AND '{end}' 
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        self.bq_db.exec()
        # print(self.bq_db.statement)
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
            FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.offer_dispatch` dispatch 
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
            FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.offer_applicant` applicant 
            JOIN `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.offer_promotion_info` promotion 
                ON applicant.promotion_id = promotion.promotion_id 
            LEFT JOIN `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.offer_dispatch` info 
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
            FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.offer_promotion_info` 
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
            FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.offer_info` offer 
            WHERE data_date BETWEEN '{start}' AND '{end}' 
            GROUP BY offer.user_id, data_date, financial_year, financial_month, financial_week 
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        # print(self.bq_db.statement)
        self.bq_db.exec()
        offer_df = self.bq_db.fetch_data

        self.bq_db.statement = (
            f"""
            SELECT bet.user_id, SUM(payoff) AS payoff, 
            data_date, financial_year, financial_month, financial_week 
            FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.bet_analysis_jackpot` bet 
            WHERE data_date BETWEEN '{start}' AND '{end}' 
            GROUP BY bet.user_id, data_date, financial_year, financial_month, financial_week 
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        # print(self.bq_db.statement)
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
            FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.User_View` member 
            WHERE member.role = 1
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        # print(self.bq_db.statement)
        self.bq_db.exec()
        member_df = self.bq_db.fetch_data

        bq_gh = BQ_GH()
        bq_gh.statement = (
            f"""
            SELECT 
            user_vip_level.user_id, 
            user_vip_level.vip_id, vip_level.name AS vip_name
            , user_vip_level.vip_level 
            FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.user_vip_level` user_vip_level 
            JOIN `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.vip_level` vip_level 
            ON user_vip_level.vip_id = vip_level.vip_id 
            WHERE 
            user_vip_level.vip_level <> 0 
            """
        )
        bq_gh.mode = self.bq_db.QUERY_MODE
        bq_gh.exec()
        # print(self.bq_db.statement)
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
            FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.Login_View` login 
            JOIN `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.User_View` member 
            ON login.user_id = member.user_id 
            LEFT JOIN `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.user_vip_level` vip 
            ON login.user_id = vip.user_id 
            WHERE vip.vip_level >= 1
            AND DATE(login.login_date_ae) BETWEEN '{start}' AND '{end}' 
            GROUP BY login.user_id, member.username, login_hour, DATE(login.login_date_ae)
            """
        )
        self.bq_db.mode = self.bq_db.QUERY_MODE
        # print(self.bq_db.statement)
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
            FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.Login_View` login 
            JOIN `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.user_info` member 
            ON login.user_id = member.user_id 
            JOIN 
            ( 
                SELECT user_id, DATE(login_date_ae) AS data_date, result, MAX(login_date_ae) AS max_at
                FROM `{self.bq_db.project_id}.ssr_dw_test_{hall.domain_name}.Login_View` 
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
        # print(self.bq_db.statement)
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
        query_start_date = datetime.strptime(start, '%Y-%m-%d').date() - delta  # 用美東時間 所以用end
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
            self.bq_db.query_by(action=data_type)
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
