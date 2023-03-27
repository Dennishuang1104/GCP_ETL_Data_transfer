import os
from google.cloud import spanner
import pandas as pd
from datetime import datetime, timedelta
import Environment
from Adaptors.Hall import Hall
from Adaptors.SpannerAdaptor import CDP_BBIN, CDP_Config
from Adaptors.TelegramAdaptor import Notify
from Adaptors.CloudStorageAdaptor import GCS
from Adaptors.SpannerAdaptor import RuleTag
from util.dataframe_process import df_demo_amount_format

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


class ETL_Spanner_BBIN_Test_Service(CDP_BBIN):
    def __init__(self, spanner_db: str, bq_db):
        super(ETL_Spanner_BBIN_Test_Service, self).__init__(database=spanner_db)
        self.source_db = CDP_BBIN(spanner_db)
        self.sv_begin_time = None
        self.sv_end_time = None
        self.telegram_message = ''

    @try_exc_func
    def member_info(self, hall: Hall, start, end):
        self.source_db.statement = (
            "SELECT hall_id, hall_name, domain_id, domain_name, "
            "user_id, "
            "CONCAT(SUBSTR(ag_name, 0, 3), '*****') AS ag_name, "
            "CONCAT(SUBSTR(user_name, 0, 3), '*****') AS user_name, "
            "CONCAT(SUBSTR(name_real, 0, 1), '*****') AS name_real, "
            "register_date, "
            "CONCAT(SUBSTR(user_phone, 0, 5), '*****') AS user_phone, "
            "CONCAT(SUBSTR(user_mail, 0, 3), '****@*****') AS user_mail, "
            "balance, "
            "last_login, "
            "CONCAT(SUBSTR(last_ip, 0, 3), '.***.***.***') AS last_ip, "
            "CONCAT(SUBSTR(qq_num, 0, 3), '*****') AS qq_num, "
            "CONCAT(SUBSTR(wechat, 0, 3), '*****') AS wechat, "
            "user_level_id, "
            "user_level "
            f"FROM member_info  "
            f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"AND DATE(register_date, 'America/New_York') BETWEEN DATE_ADD(DATE '{start}', INTERVAL -2 DAY) AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'hall_name', 'domain_id', 'domain_name',
                'user_id', 'ag_name', 'user_name',
                'name_real', 'register_date', 'user_phone', 'user_mail', 'balance',
                'last_login', 'last_ip', 'qq_num', 'wechat', 'user_level_id', 'user_level']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            insert_df = df_demo_amount_format(insert_df)
            self.upsert_data_with('member_info', insert_df)

    @try_exc_func
    def login_log(self, hall: Hall, start, end):
        self.source_db.statement = (
            "SELECT hall_id, hall_name, domain_id, domain_name, "
            "user_id,"
            "CONCAT(SUBSTR(ag_name, 0, 3), '*****') AS ag_name, "
            "login_count, "
            "host, "
            "data_date, "
            "financial_year, "
            "financial_month, "
            "financial_week "
            "FROM login_log "
            "WHERE "
            f"  hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"  AND data_date BETWEEN '{start}' AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'hall_name', 'domain_id', 'domain_name',
                'user_id', 'ag_name', 'login_count', 'host',
                'data_date', 'financial_year', 'financial_month', 'financial_week']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.replace_data_with('login_log', insert_df)

    @try_exc_func
    def game_dict(self, hall: Hall):
        self.source_db.statement = (
            "SELECT hall_id, hall_name, domain_id, domain_name, "
            "lobby, "
            "lobby_name, "
            "lobby_group, "
            "lobby_group_name, "
            "game_type, "
            "game_type_name, "
            "is_pvp, "
            "is_pve "
            "FROM "
            f"game_type_dict "
            f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'hall_name', 'domain_id', 'domain_name',
                'lobby', 'lobby_name', 'lobby_group', 'lobby_group_name',
                'game_type', 'game_type_name', 'is_pvp', 'is_pve']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.replace_data_with('game_type_dict', insert_df)

    @try_exc_func
    def bet_analysis(self, hall: Hall, start, end):
        self.source_db.statement = (
            "SELECT hall_id, hall_name, domain_id, domain_name, "
            "user_id, "
            "CONCAT(SUBSTR(ag_name, 0, 3), '*****') AS ag_name, "
            "lobby, "
            "lobby_name, "
            "lobby_group, "
            "lobby_group_name, "
            "game_type, "
            "game_name, "
            "wagers_total, "
            "bet_amount, "
            "commissionable, "
            "payoff, "
            "win_rate, "
            "platform, "
            "platform_name, "
            "data_date, "
            "financial_year, "
            "financial_month, "
            "financial_week, "
            "user_level_id, "
            "user_level "
            "FROM "
            f"bet_analysis "
            f"WHERE 1=1 "
            f"AND hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"AND data_date BETWEEN '{start}' AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'hall_name', 'domain_id', 'domain_name',
                'user_id', 'ag_name',
                'lobby', 'lobby_name', 'lobby_group', 'lobby_group_name',
                'game_type', 'game_name',
                'wagers_total', 'bet_amount', 'commissionable', 'payoff', 'win_rate',
                'platform', 'platform_name',
                'data_date', 'financial_year', 'financial_month', 'financial_week',
                'user_level_id', 'user_level']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            insert_df = df_demo_amount_format(insert_df)
            self.column_times_setter(2)
            self.replace_data_with('bet_analysis', insert_df)

    @try_exc_func
    def deposit_withdraw(self, hall: Hall, start, end):
        self.source_db.statement = (
            "SELECT hall_id, hall_name, domain_id, domain_name, "
            "user_id,  "
            "CONCAT(SUBSTR(ag_name, 0, 3), '*****') AS ag_name, "
            "deposit_amount, "
            "deposit_count, "
            "withdraw_amount, "
            "withdraw_count, "
            "data_date, financial_year, financial_month, financial_week, "
            "user_level_id, user_level "
            "FROM deposit_withdraw_record "
            "WHERE 1=1 "
            f"AND hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"AND data_date BETWEEN '{start}' AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'hall_name', 'domain_id', 'domain_name',
                'user_id', 'ag_name',
                'deposit_amount', 'deposit_count', 'withdraw_amount', 'withdraw_count',
                'data_date', 'financial_year', 'financial_month', 'financial_week',
                'user_level_id', 'user_level']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            insert_df = df_demo_amount_format(insert_df)
            self.column_times_setter(2)
            self.replace_data_with('deposit_withdraw_record', insert_df)

    @try_exc_func
    def offer(self, hall: Hall, start, end):
        self.source_db.statement = (
            "SELECT hall_id, hall_name, domain_id, domain_name, "
            "user_id, "
            "CONCAT(SUBSTR(ag_name, 0, 3), '*****') AS ag_name, "
            "opcode, opcode_name, premium_total, premium_amount, "
            "data_date, financial_year, financial_month, financial_week, "
            "user_level_id, user_level "
            "FROM offer_info "
            "WHERE 1=1 "
            f"AND hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"AND data_date BETWEEN '{start}' AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'hall_name', 'domain_id', 'domain_name',
                'user_id', 'ag_name',
                'opcode', 'opcode_name', 'premium_total', 'premium_amount',
                'data_date', 'financial_year', 'financial_month', 'financial_week',
                'user_level_id', 'user_level']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            insert_df = df_demo_amount_format(insert_df)
            self.replace_data_with('offer_info', insert_df)

    @try_exc_func
    def profit_loss(self, hall: Hall, start, end):
        self.source_db.statement = (
            "SELECT hall_id, hall_name, domain_id, domain_name, "
            "user_id, "
            "payoff, premium_amount, profit_loss.profit_loss, "
            "data_date, financial_year, financial_month, financial_week "
            "FROM profit_loss "
            "WHERE 1=1 "
            f"AND hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"AND data_date BETWEEN '{start}' AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'hall_name', 'domain_id', 'domain_name',
                'user_id', 'payoff', 'premium_amount', 'profit_loss',
                'data_date', 'financial_year', 'financial_month', 'financial_week']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.column_times_setter(2)
            insert_df = df_demo_amount_format(insert_df)
            self.replace_data_with('profit_loss', insert_df)

    @try_exc_func
    def vip_login(self, hall: Hall, start, end):
        self.source_db.statement = (
            "SELECT hall_id, hall_name, domain_id, domain_name, "
            "user_id, "
            "CONCAT(SUBSTR(user_name, 0, 3), '*****') AS user_name, "
            "login_hour, login_count, "
            "data_date, financial_year, financial_month, financial_week, weekday "
            "FROM vip_login "
            "WHERE 1=1 "
            f"AND hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"AND data_date BETWEEN '{start}' AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'hall_name', 'domain_id', 'domain_name',
                'user_id', 'user_name', 'login_hour', 'login_count',
                'data_date', 'financial_year', 'financial_month', 'financial_week', 'weekday']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.replace_data_with('vip_login', insert_df)

    @try_exc_func
    def login_location(self, hall: Hall, start, end):
        self.source_db.statement = (
            "SELECT hall_id, hall_name, domain_id, domain_name, "
            "user_id, "
            "CONCAT(SUBSTR(ag_name, 0, 3), '*****') AS ag_name, "
            "ip, "
            "client_os, "
            "client_browser, "
            "country_code, "
            "city, "
            "latitude, longitude "
            f"FROM login_location "
            f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'hall_name', 'domain_id', 'domain_name',
                'user_id', 'ag_name', 'ip', 'client_os',
                'client_browser', 'country_code', 'city',
                'latitude', 'longitude']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.replace_data_with('login_location', insert_df)

    @try_exc_func
    def user_healthy_score(self, hall: Hall, start, end):
        self.source_db.statement = (
            "Select "
            "hall_id, "
            "domain_id, "
            "user_id, "
            "recommend_code, "
            "action_score, "
            "update_time, "
            "shapley_value "
            "from user_healthy_score "
             f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'domain_id',
                'user_id', 'recommend_code', 'action_score',
                'update_time', 'shapley_value']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.replace_data_with('user_healthy_score', insert_df)


    @try_exc_func
    def tags(self, hall: Hall, start, end):
        self.source_db.statement = (
            "SELECT hall_id, domain_id,  "
            "user_id, "
            "CONCAT(SUBSTR(user_name, 0, 3), '*****') AS user_name, "
            "register_date, "
            "tag_type, "
            "tag_code, "
            "tag_enabled, "
            "updated_time "
            f"FROM user_tag_ods_data "
            f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"AND tag_type <> '1' "
            f"AND DATE(updated_time, 'America/New_York') BETWEEN '{start}' AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'domain_id',
                'user_id', 'user_name', 'register_date',
                'tag_type', 'tag_code', 'tag_enabled', 'updated_time']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.replace_data_with('user_tag_ods_data', insert_df)

        self.source_db.statement = (
            "SELECT hall_id, domain_id, "
            "user_id, "
            "CONCAT(SUBSTR(user_name, 0, 3), '*****') AS user_name, "
            "CONCAT(SUBSTR(ag_name, 0, 3), '*****') AS ag_name, "
            "tag_str, "
            "user_level_id, "
            "user_level, "
            "updated_time, "
            "register_date, "
            "local_tag_name_str, "
            "local_tag_name_str_cn, "
            "local_tag_name_str_en, "
            "demo_tag_name_str, "
            "demo_tag_name_str_cn, "
            "demo_tag_name_str_en, "
            "prod_tag_name_str, "
            "prod_tag_name_str_en, "
            "prod_tag_name_str_cn, "
            "last_activated_date, "
            "last_login_date "
            f"FROM user_tag_dw_data "
            f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"AND DATE(updated_time, 'America/New_York') BETWEEN '{start}' AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'domain_id',
                'user_id', 'user_name', 'ag_name', 'tag_str', 'user_level_id', 'user_level',
                'updated_time', 'register_date',
                'local_tag_name_str', 'local_tag_name_str_cn', 'local_tag_name_str_en',
                'demo_tag_name_str', 'demo_tag_name_str_cn', 'demo_tag_name_str_en',
                'prod_tag_name_str', 'prod_tag_name_str_cn', 'prod_tag_name_str_en',
                'last_activated_date', 'last_login_date'
            ]
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.column_times_setter(3)
            self.replace_data_with('user_tag_dw_data', insert_df)

    @try_exc_func
    def tag_of_operate(self, hall: Hall, start, end):
        self.source_db.statement = (
            "SELECT hall_id, domain_id, "
            "user_id, "
            "CONCAT(SUBSTR(user_name, 0, 3), '*****') AS user_name, "
            "data_date, "
            "tag_code, "
            "created_time, "
            "updated_time "
            f"FROM activated_tag_day "
            f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"AND DATE(updated_time, 'America/New_York') BETWEEN '{start}' AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'domain_id',
                'user_id', 'user_name', 'data_date', 'tag_code',
                'created_time', 'updated_time']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.replace_data_with('activated_tag_day', insert_df)

        self.source_db.statement = (
            "SELECT hall_id, domain_id, "
            "user_id, "
            "CONCAT(SUBSTR(ag_name, 0, 3), '*****') AS ag_name, "
            "CONCAT(SUBSTR(user_name, 0, 3), '*****') AS user_name, "
            "last_two_day_step, "
            "last_day_step, "
            "this_day_step, "
            "last_three_tag_code, "
            "last_two_tag_code, "
            "last_tag_code, "
            "this_tag_code, "
            "init_flag, "
            "data_date, "
            "fin_year, "
            "fin_month, "
            "fin_week, "
            "updated_time "
            f"FROM activated_tag_dw_day "
            f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"AND DATE(updated_time, 'America/New_York') BETWEEN '{start}' AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'domain_id',
                'user_id', 'ag_name', 'user_name',
                'last_two_day_step', 'last_day_step', 'this_day_step',
                'last_three_tag_code', 'last_two_tag_code', 'last_tag_code', 'this_tag_code',
                'init_flag',
                'data_date', 'fin_year', 'fin_month', 'fin_week',
                'updated_time']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.replace_data_with('activated_tag_dw_day', insert_df)

    @try_exc_func
    def user_active(self, hall: Hall, start, end):
        self.source_db.statement = (
            "SELECT hall_id, domain_id, "
            "user_id, "
            "CONCAT(SUBSTR(user_name, 0, 3), '*****') AS user_name, "
            "data_date, "
            "vip_count, "
            "action_score, "
            "fin_year, "
            "fin_month, "
            "fin_week, "
            "updated_time "
            f"FROM user_active_data "
            f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"AND DATE(updated_time, 'America/New_York') BETWEEN '{start}' AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'domain_id',
                'user_id', 'user_name', 'data_date', 'vip_count', 'action_score',
                'fin_year', 'fin_month', 'fin_week',
                'updated_time']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.replace_data_with('user_active_data', insert_df)

    @try_exc_func
    def recommend_meta(self, hall: Hall, start, end):
        self.source_db.statement = (
            "SELECT hall_id, domain_id, "
            "user_id, "
            "data_date, "
            "recommend_code, "
            "action_score, "
            "update_date, "
            "enabled, "
            "received, "
            "ip_count, "
            "send_flag "
            f"FROM user_recommend_meta_data "
            f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"AND DATE(update_date, 'America/New_York') BETWEEN '{start}' AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'domain_id',
                'user_id', 'data_date',
                'recommend_code', 'action_score',
                'update_date',
                'enabled', 'received', 'ip_count', 'send_flag']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.replace_data_with('user_recommend_meta_data', insert_df)

    @try_exc_func
    def smart_message(self, hall: Hall, start, end):
        self.source_db.statement = (
            "SELECT hall_id, domain_id, "
            "message_id, "
            "kind, "
            "content, "
            "created_time, "
            "updated_time "
            f"FROM smart_message_notification_data "
            f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"AND DATE(updated_time, 'America/New_York') BETWEEN '{start}' AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'domain_id',
                'message_id', 'kind', 'content',
                'created_time', 'updated_time']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            insert_df['content'] = insert_df['content'].replace(
                to_replace=r"(會員 #)(.{3})(.*)(@.*#)(.*)(¥)([0-9|,]+)(.*)",
                value=r"\1\2*****\4\5℗\7\8", regex=True)

            insert_df['num_string'] = insert_df['content'].str.extract('℗([0-9|,]+)', expand=False)
            insert_df.fillna(0, inplace=True)
            for index, row in insert_df.iterrows():
                if row['num_string'] != 0:
                    new_num_string = str((int(row['num_string'].replace(',', '')) / 100))
                    row['content'] = row['content'].replace(row['num_string'], new_num_string)
                    insert_df.loc[index, 'content'] = row['content']

            insert_df.drop(columns='num_string', errors='ignore', inplace=True)
            self.replace_data_with('smart_message_notification_data', insert_df)

    @try_exc_func
    def member_journey(self, hall: Hall, start, end):
        self.source_db.statement = (
            f"""
            SELECT 
            hall_id, domain_id, user_id, data_date, 
            sum_bet_amount, sum_deposit_amount, 
            user_tag, profit_ratio, net_profit_ratio, profit_ratio_tiptitle, net_profit_ratio_tiptitle, 
            created_time, 
            profit_ratio_tiptitle_cn, profit_ratio_tiptitle_en, 
            net_profit_ratio_tiptitle_cn, net_profit_ratio_tiptitle_en, 
            custom_flag_title, custom_flag_content, custom_flag_operator, custom_flag_updated_time
            FROM member_journey 
            WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} 
            AND data_date BETWEEN '{start}' AND '{end}' 
            """
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'domain_id', 'user_id', 'data_date',
                'sum_bet_amount', 'sum_deposit_amount',
                'user_tag', 'profit_ratio', 'net_profit_ratio', 'profit_ratio_tiptitle', 'net_profit_ratio_tiptitle',
                'created_time',
                'profit_ratio_tiptitle_cn', 'profit_ratio_tiptitle_en',
                'net_profit_ratio_tiptitle_cn', 'net_profit_ratio_tiptitle_en',
                'custom_flag_title', 'custom_flag_content', 'custom_flag_operator', 'custom_flag_updated_time'
            ]
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.replace_data_with('member_journey', insert_df)

        self.source_db.statement = (
            f"""
            SELECT
            hall_id, domain_id, user_id, data_date, 
            source, type, content, created_time, 
            content_cn, content_en
            FROM member_journey_detail
            WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} 
            AND data_date BETWEEN '{start}' AND '{end}' 
            """
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'domain_id', 'user_id', 'data_date',
                'source', 'type', 'content', 'created_time',
                'content_cn', 'content_en'
            ]
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.replace_data_with('member_journey_detail', insert_df)

    @try_exc_func
    def ga_data(self, hall: Hall, start, end):
        self.column_times_setter(column_times=2)
        self.source_db.statement = (
            "SELECT hall_id, hall_name, domain_id, domain_name, "
            "user_id, "
            "page_views, "
            "product_clicks, "
            "promotion_clicks, "
            "service_contact, "
            "service_contact_mobile, "
            "total_sessions, "
            "total_bounces, "
            "time_on_site, "
            "data_date, "
            "financial_year, "
            "financial_month, "
            "financial_week "
            f"FROM ga_data_set "
            f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"AND data_date BETWEEN '{start}' AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'hall_name', 'domain_id', 'domain_name',
                'user_id', 'page_views',
                'product_clicks', 'promotion_clicks',
                'service_contact', 'service_contact_mobile',
                'total_sessions', 'total_bounces', 'time_on_site',
                'data_date', 'financial_year', 'financial_month', 'financial_week']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.replace_data_with('ga_data_set', insert_df)

        self.source_db.statement = (
            "SELECT hall_id, hall_name, domain_id, domain_name, "
            "user_id, "
            "hostname, page_path, page_title, hits_count, "
            "data_date, "
            "financial_year, "
            "financial_month, "
            "financial_week "
            f"FROM ga_page_path "
            f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            f"AND data_date BETWEEN '{start}' AND '{end}' "
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        if len(self.source_db.fetch_data) > 0:
            insert_columns = [
                'hall_id', 'hall_name', 'domain_id', 'domain_name',
                'user_id',
                'hostname', 'page_path', 'page_title', 'hits_count',
                'data_date', 'financial_year', 'financial_month', 'financial_week']
            insert_df = pd.DataFrame(data=self.source_db.fetch_data,
                                     columns=insert_columns)
            self.replace_data_with('ga_page_path', insert_df)

    @try_exc_func
    def tag_custom(self, hall: Hall, spanner_db: CDP_BBIN, spanner_config: CDP_Config):
        self.table_name = 'user_tag_custom_batch'
        self.statement = (
            "SELECT hall_id, hall_name, domain_id, domain_name, "
            "tag_code, file_name, file_path, operator "
            "FROM user_tag_custom_batch "
            f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
            # f"AND status = 0 "
        )
        self.mode = self.QUERY_MODE
        self.exec()

        storage = GCS('ghr-cdp', f'{Environment.ROOT_PATH}/cert/gcp-ghr-storage.json')
        if len(self.fetch_data) > 0:
            rule_tag = RuleTag(spanner_db=spanner_db, spanner_config=spanner_config)

            columns = ['hall_id', 'hall_name', 'domain_id', 'domain_name',
                       'tag_code', 'file_name', 'file_path', 'operator']
            for batch in self.fetch_data:
                batch_df = pd.DataFrame([batch], columns=columns)
                tag_code = batch_df['tag_code'].values[0]
                file_name = batch_df['file_name'].values[0]
                file_path = batch_df['file_path'].values[0]
                # 更新狀態為進行中
                self.update_data_dml_with(
                    statement=
                    "UPDATE user_tag_custom_batch SET status = 3, "
                    "updated_time = PENDING_COMMIT_TIMESTAMP() "
                    f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
                    f"AND tag_code = {tag_code} "
                )
                self.update_data_dml_with(
                    statement=
                    "UPDATE user_tag_custom_batch_log SET status = 3, "
                    "updated_time = PENDING_COMMIT_TIMESTAMP() "
                    f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
                    f"AND tag_code = {tag_code} "
                    f"AND file_name = \'{file_name}\' "
                )
                try:
                    storage.blob = f'{file_path}{file_name}'
                    storage.file = f'{Environment.ROOT_PATH}/files/{file_name}'
                    storage.mode = storage.DOWNLOAD_MODE
                    storage.exec()
                    tag_file_df = pd.read_csv(storage.file)
                    # 日期格式標準化 "/" 改 "-" 並補0
                    tag_file_df['data_date'] = tag_file_df['data_date'].str.replace('/', '-')
                    tag_file_df['data_date'] = tag_file_df['data_date'].replace(
                        to_replace=r"(\d{4})(\-)(\d{1})(\-)(\d{1}$)",
                        value=r"\1-0\3-0\5", regex=True)
                    tag_file_df['data_date'] = tag_file_df['data_date'].replace(
                        to_replace=r"(\d{4})(\-)(\d{1})(\-)(\d{2}$)",
                        value=r"\1-0\3-\5", regex=True)
                    tag_file_df['data_date'] = tag_file_df['data_date'].replace(
                        to_replace=r"(\d{4})(\-)(\d{2})(\-)(\d{1}$)",
                        value=r"\1-\3-0\5", regex=True)
                    enable_count = tag_file_df['enable'].value_counts()
                    add_count = 0 if enable_count.get(1) is None else enable_count.get(1)
                    remove_count = 0 if enable_count.get(0) is None else enable_count.get(0)

                    tag_file_df['user_name'] = tag_file_df['user_name'].replace(
                        to_replace=r"(.{3})(.*)",
                        value=r"\1*****", regex=True)
                    username_condition = "','".join(tag_file_df['user_name'])

                    update_df = self.select_data_with(
                        columns=['hall_id', 'hall_name', 'domain_id', 'domain_name',
                                 'user_id', 'user_name', 'register_date'],
                        statement="SELECT hall_id, hall_name, domain_id, domain_name, "
                                  "user_id, user_name, register_date "
                                  "FROM member_info WHERE "
                                  f"hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
                                  f"AND user_name IN ('{username_condition}') "
                    )
                    update_df['tag_type'] = str(tag_code)[0]
                    update_df['tag_code'] = tag_code
                    update_df['updated_time'] = spanner.COMMIT_TIMESTAMP
                    merge_df = pd.merge(left=update_df, right=tag_file_df,
                                        on='user_name', how='left')
                    merge_df['enable'] = merge_df['enable'].astype('bool')

                    date_log_columns = [
                        'hall_id', 'hall_name', 'domain_id', 'domain_name',
                        'user_id', 'tag_code', 'data_date', 'enable', 'updated_time']
                    self.replace_data_with('user_tag_custom_date_log', merge_df[date_log_columns])

                    ods_df = merge_df.rename(columns={'enable': 'tag_enabled'})
                    ods_columns = [
                        'hall_id', 'domain_id', 'user_id', 'user_name',
                        'tag_code', 'tag_type', 'tag_enabled', 'register_date', 'updated_time']
                    self.replace_data_with('user_tag_ods_data', ods_df[ods_columns])

                    # 更新總人數
                    row_count_df = self.select_data_with(
                        ['row_count'],
                        "SELECT COUNT(1) FROM user_tag_ods_data "
                        "WHERE "
                        f"hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
                        f"AND tag_code = {tag_code} AND tag_enabled IS TRUE "
                    )
                    self.update_data_dml_with(
                        "UPDATE user_tag_custom_batch "
                        f"SET row_count = {row_count_df['row_count'].values[0]} "
                        f"WHERE "
                        f"hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} AND tag_code = {tag_code} "
                    )


                    # 更新狀態為完成
                    self.update_data_dml_with(
                        statement=
                        "UPDATE user_tag_custom_batch "
                        f"SET status = 1, "
                        "updated_time = PENDING_COMMIT_TIMESTAMP() "
                        f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
                        f"AND tag_code = {tag_code} "
                    )
                    self.update_data_dml_with(
                        statement=
                        "UPDATE user_tag_custom_batch_log "
                        f"SET status = 1, add_count = {add_count}, remove_count = {remove_count}, "
                        "updated_time = PENDING_COMMIT_TIMESTAMP() "
                        f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
                        f"AND tag_code = {tag_code} AND file_name = \'{file_name}\'"
                    )

                except Exception as e:
                    self.update_data_dml_with(
                        statement=
                        "UPDATE user_tag_custom_batch "
                        f"SET status = 2, "
                        "updated_time = PENDING_COMMIT_TIMESTAMP() "
                        f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
                        f"AND tag_code = {tag_code} "
                    )
                    self.update_data_dml_with(
                        statement=
                        "UPDATE user_tag_custom_batch_log "
                        f"SET status = 2, "
                        "updated_time = PENDING_COMMIT_TIMESTAMP() "
                        f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
                        f"AND tag_code = {tag_code} AND file_name = \'{file_name}\' "
                    )
                    raise
                finally:
                    os.remove(storage.file)
            self.column_times_setter(column_times=3)
            self.replace_data_with('user_tag_dw_data', rule_tag.ods_to_dw(hall))