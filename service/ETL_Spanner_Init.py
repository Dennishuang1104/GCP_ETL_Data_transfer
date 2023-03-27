import pandas as pd
import Environment

from Adaptors.CloudSQLAdaptor import CDP as SQL_CDP
from Adaptors.SpannerAdaptor import CDP_BBIN
from Adaptors.SpannerAdaptor import CDP_BBOS
from util.dataframe_process import df_type_format, df_timezone_format


class ETL_Spanner_Init(CDP_BBOS):
    def __init__(self, spanner_db: str, db: SQL_CDP):
        super(ETL_Spanner_Init, self).__init__(database=spanner_db)
        self.db = db
        self.sv_begin_time = None
        self.sv_end_time = None

    def user_insert(self, start, end):
        user_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/cdp_users.csv')

        user_df['email_verified_at'] = pd.to_datetime(user_df['email_verified_at'], format='%Y-%m-%d %H:%M:%S')
        user_df['created_at'] = pd.to_datetime(user_df['created_at'], format='%Y-%m-%d %H:%M:%S')
        user_df['updated_at'] = pd.to_datetime(user_df['updated_at'], format='%Y-%m-%d %H:%M:%S')
        user_df['last_login_date'] = pd.to_datetime(user_df['last_login_date'], format='%Y-%m-%d %H:%M:%S')
        user_df['pwd_updated_time'] = pd.to_datetime(user_df['pwd_updated_time'], format='%Y-%m-%d %H:%M:%S')
        user_df = df_timezone_format(user_df, 'Asia/Taipei')  # 設定時區
        user_df['email_verified_at'] = user_df['email_verified_at'].fillna(pd.Timestamp.min)
        user_df['created_at'] = user_df['created_at'].fillna(pd.Timestamp.min)
        user_df['updated_at'] = user_df['updated_at'].fillna(pd.Timestamp.min)
        user_df['last_login_date'] = user_df['last_login_date'].fillna(pd.Timestamp.min)
        user_df['pwd_updated_time'] = user_df['pwd_updated_time'].fillna(pd.Timestamp.min)

        self.columns = user_df.columns.to_list()
        self.write_data = user_df.values.tolist()
        self.table_name = 'users'
        self.mode = self.REPLACE_MODE
        self.exec()

    def ip_whitelist(self, start, end):
        ip_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/cdp_access_ip_white_list.csv')
        ip_df['domain_id'].replace({0: None}, inplace=True)
        print(ip_df.dtypes)
        self.columns = ip_df.columns.to_list()
        self.write_data = ip_df.values.tolist()
        self.table_name = 'access_ip_white_list'
        self.mode = self.REPLACE_MODE
        self.exec()

    def financial_date(self, start, end):
        financial_date_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/cdp_financial_date_map.csv')
        self.columns = financial_date_df.columns.to_list()
        self.write_data = financial_date_df.values.tolist()
        self.table_name = 'financial_date_map'
        self.mode = self.REPLACE_MODE
        self.exec()

    def custom_tags(self, start, end):
        custom_tags_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/cdp_user_custom_tags_data.csv')
        custom_tags_df['domain_id'].replace({0: None}, inplace=True)
        custom_tags_df['created_time'] = pd.to_datetime(custom_tags_df['created_time'], format='%Y-%m-%d %H:%M:%S')
        custom_tags_df = df_timezone_format(custom_tags_df, 'Asia/Taipei')  # 設定時區
        self.columns = custom_tags_df.columns.to_list()
        self.write_data = custom_tags_df.values.tolist()
        self.table_name = 'user_custom_tags_data'
        self.mode = self.REPLACE_MODE
        self.exec()

    def activity_analysis(self, start, end):
        custom_tags_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/cdp_activity_analysis_data.csv')
        custom_tags_df['domain_id'].replace({0: None}, inplace=True)
        custom_tags_df['create_time'] = pd.to_datetime(custom_tags_df['create_time'], format='%Y-%m-%d %H:%M:%S')
        custom_tags_df = df_timezone_format(custom_tags_df, 'Asia/Taipei')  # 設定時區
        self.columns = custom_tags_df.columns.to_list()
        self.write_data = custom_tags_df.values.tolist()
        self.table_name = 'activity_analysis_data'
        self.mode = self.REPLACE_MODE
        self.exec()

    def member(self, start, end):
        sql_cdp = SQL_CDP()
        sql_cdp.statement = (
            "SELECT "
            "hall_id, hall_name, domain_id, domain_name, "
            "user_id, ag_name, user_name, name_real, "
            "DATE_ADD(register_date, INTERVAL 12 HOUR) , register_ip, register_country, register_city, register_by, "
            "user_phone, user_mail, balance, "
            "DATE_ADD(last_login, INTERVAL 12 HOUR), last_ip, last_country, last_city_id, DATE_ADD(last_online, INTERVAL 12 HOUR), "
            "zalo, facebook, "
            "user_type, vip_level, deep_effort, "
            "vip_activate_date, vip_expired_date, "
            "deep_effort_activate_date, deep_effort_expired_date, "
            "arbitrage_flag, user_level_id, user_level "
            "FROM member_info "
            "WHERE hall_id = 3820325 AND domain_id = 70 AND user_id = 14323064"
        )

        sync_columns = ['hall_id', 'hall_name', 'domain_id', 'domain_name',
                        'user_id', 'ag_name', 'user_name', 'name_real',
                        'register_date', 'register_ip', 'register_country', 'register_city', 'register_by',
                        'user_phone', 'user_mail', 'balance',
                        'last_login', 'last_ip', 'last_country', 'last_city_id', 'last_online',
                        'zalo', 'facebook',
                        'user_type', 'vip_level', 'deep_effort',
                        'vip_activate_date', 'vip_expired_date',
                        'deep_effort_activate_date', 'deep_effort_expired_date',
                        'arbitrage_flag', 'user_level_id', 'user_level']
        sql_cdp.mode = sql_cdp.QUERY_MODE  # 0
        sql_cdp.exec()
        insert_df = pd.DataFrame(sql_cdp.fetch_data, columns=sync_columns)
        insert_df = df_type_format(insert_df) # balance 要轉decimal
        print(insert_df.dtypes)

        insert_df['register_date'] = pd.to_datetime(insert_df['register_date'], format='%Y-%m-%d %H:%M:%S')
        insert_df['last_login'] = pd.to_datetime(insert_df['last_login'], format='%Y-%m-%d %H:%M:%S')
        insert_df['last_online'] = pd.to_datetime(insert_df['last_online'], format='%Y-%m-%d %H:%M:%S')

        insert_df['last_online'] = insert_df['last_online'].fillna(pd.Timestamp('1900-01-01 08:00:00.000000000'))
        insert_df['last_login'] = insert_df['last_login'].fillna(pd.Timestamp('1900-01-01 08:00:00.000000000'))
        insert_df['register_date'] = insert_df['register_date'].fillna(pd.Timestamp('1900-01-01 08:00:00.000000000'))
        print(1)
        insert_df = df_timezone_format(insert_df, 'Asia/Taipei')  # 設定時區
        print(insert_df.values)
        self.columns = insert_df.columns.to_list()
        self.write_data = insert_df.values.tolist()
        self.table_name = 'member_info'
        self.mode = self.UPSERT_MODE
        self.exec()

    def tag_ods(self, start, end):
        sql_cdp = SQL_CDP()
        sql_cdp.statement = (
            "SELECT hall_id, domain_id, user_id, user_name, tag_code, tag_type, tag_enabled, "
            "updated_time, DATE_ADD(register_date, INTERVAL 12 HOUR) AS register_date "
            "FROM user_tag_ods_data "
        )

        sync_columns = ['hall_id', 'domain_id', 'user_id', 'user_name',
                        'tag_code', 'tag_type', 'tag_enabled', 'updated_time', 'register_date']
        sql_cdp.mode = sql_cdp.QUERY_MODE  # 0
        sql_cdp.exec()
        tag_df = pd.DataFrame(sql_cdp.fetch_data, columns=sync_columns)
        tag_df['tag_enabled'] = tag_df['tag_enabled'].astype('bool')
        tag_df = df_timezone_format(tag_df, 'Asia/Taipei')  # 設定時區

        self.columns = tag_df.columns.to_list()
        self.write_data = tag_df.values.tolist()
        self.table_name = 'user_tag_ods_data'
        self.mode = self.REPLACE_MODE
        self.exec()

    def tag_dw(self, start, end):
        sql_cdp = SQL_CDP()
        sql_cdp.statement = (
            "SELECT hall_id, domain_id, user_id, user_name, ag_name, tag_str, "
            "user_level_id, user_level, "
            "updated_time, DATE_ADD(register_date, INTERVAL 12 HOUR) AS register_date "
            "FROM user_tag_dw_data "
        )

        sync_columns = ['hall_id', 'domain_id', 'user_id', 'user_name', 'ag_name', 'tag_str',
                        'user_level_id', 'user_level', 'updated_time', 'register_date']
        sql_cdp.mode = sql_cdp.QUERY_MODE  # 0
        sql_cdp.exec()
        tag_df = pd.DataFrame(sql_cdp.fetch_data, columns=sync_columns)
        tag_df = df_timezone_format(tag_df, 'Asia/Taipei')  # 設定時區

        self.columns = tag_df.columns.to_list()
        self.write_data = tag_df.values.tolist()
        self.table_name = 'user_tag_dw_data'
        self.mode = self.REPLACE_MODE
        self.exec()

    def ga_data_set(self, start, end):
        sql_cdp = SQL_CDP()
        sql_cdp.statement = (
            f"SELECT hall_id, hall_name, domain_id, domain_name, user_id, "
            f"page_views, product_clicks, promotion_clicks, service_contact, service_contact_mobile, "
            f"total_sessions, total_bounces, time_on_site, "
            f"data_date, financial_year, financial_month, financial_week "
            f"FROM ga_data_set WHERE data_date BETWEEN '{start}' AND '{end}'"
        )
        sync_columns = [
            'hall_id', 'hall_name', 'domain_id', 'domain_name',
            'user_id', 'page_views', 'product_clicks', 'promotion_clicks',
            'service_contact', 'service_contact_mobile', 'total_sessions', 'total_bounces', 'time_on_site',
            'data_date', 'financial_year', 'financial_month', 'financial_week']
        sql_cdp.mode = sql_cdp.QUERY_MODE  # 0
        sql_cdp.exec()
        tag_df = pd.DataFrame(sql_cdp.fetch_data, columns=sync_columns)
        self.columns = tag_df.columns.to_list()
        self.write_data = tag_df.values.tolist()
        self.table_name = 'ga_data_set'
        self.mode = self.REPLACE_MODE
        self.exec()

    def ga_page_path(self, start, end):
        sql_cdp = SQL_CDP()
        sql_cdp.statement = (
            f"SELECT hall_id, hall_name, domain_id, domain_name, user_id, hostname, page_path, page_title, "
            f"hits_count, data_date, financial_year, financial_month, financial_week "
            f"FROM ga_page_path WHERE data_date BETWEEN '{start}' AND '{end}'"
        )
        sync_columns = [
            'hall_id', 'hall_name', 'domain_id', 'domain_name', 'user_id', 'hostname', 'page_path', 'page_title',
            'hits_count', 'data_date', 'financial_year', 'financial_month', 'financial_week']
        sql_cdp.mode = sql_cdp.QUERY_MODE  # 0
        sql_cdp.exec()
        tag_df = pd.DataFrame(sql_cdp.fetch_data, columns=sync_columns)
        self.columns = tag_df.columns.to_list()
        self.write_data = tag_df.values.tolist()
        self.table_name = 'ga_page_path'
        self.mode = self.REPLACE_MODE
        self.exec()

    def ga_firebase_page(self, start, end):
        sql_cdp = SQL_CDP()
        sql_cdp.statement = (
            f"SELECT hall_id, hall_name, domain_id, domain_name, user_id, page_title, "
            f"data_date, financial_year, financial_month, financial_week "
            f"FROM ga_firebase_page WHERE data_date BETWEEN '{start}' AND '{end}'"
        )
        sync_columns = [
            'hall_id', 'hall_name', 'domain_id', 'domain_name', 'user_id', 'page_title',
            'data_date', 'financial_year', 'financial_month', 'financial_week']
        sql_cdp.mode = sql_cdp.QUERY_MODE  # 0
        sql_cdp.exec()
        tag_df = pd.DataFrame(sql_cdp.fetch_data, columns=sync_columns)
        self.columns = tag_df.columns.to_list()
        self.write_data = tag_df.values.tolist()
        self.table_name = 'ga_firebase_page'
        self.mode = self.REPLACE_MODE
        self.exec()
