import pandas as pd
import Environment
from Adaptors.Hall import Hall
from Adaptors.SpannerAdaptor import CDP_BBOS as SQL_CDP
from Adaptors.SpannerAdaptor import CDP_Config
from util.Hall_dict import hall_dict
from util.dataframe_process import df_type_format, df_timezone_format


class ETL_Spanner_Config(CDP_Config):
    def __init__(self, spanner_db: str, source_db: str):
        super(ETL_Spanner_Config, self).__init__(database=spanner_db)
        self.source_db = CDP_Config(source_db)
        self.sv_begin_time = None
        self.sv_end_time = None

    def get_config_tag(self, hall: Hall):
        self.mode = self.QUERY_MODE
        self.statement = f"""
                        select 
                            tag_code, tag_name, tag_name_cn, tag_name_en, can_access_env
                        from user_tag_info
                        where hall_id = {hall.hall_id} and domain_id = {hall.domain_id} 
                        and tag_enabled = true
                    """
        self.exec()
        tag_df = pd.DataFrame(data=self.fetch_data,
                              columns=["tag_code", "tag_name", "tag_name_cn", "tag_name_en", "can_access_env"])
        local_tag_name = " ".join(
            [f"when tag_code = {row['tag_code']} then '{row['tag_name']}'" for cnt, row in tag_df.iterrows() if
             'local' in row['can_access_env']])
        dev_tag_name = " ".join(
            [f"when tag_code = {row['tag_code']} then '{row['tag_name']}'" for cnt, row in tag_df.iterrows() if
             'dev' in row['can_access_env']])
        prod_tag_name = " ".join(
            [f"when tag_code = {row['tag_code']} then '{row['tag_name']}'" for cnt, row in tag_df.iterrows() if
             'prod' in row['can_access_env']])
        local_tag_name_cn = " ".join(
            [f"when tag_code = {row['tag_code']} then '{row['tag_name_cn']}'" for cnt, row in tag_df.iterrows() if
             'local' in row['can_access_env']])
        dev_tag_name_cn = " ".join(
            [f"when tag_code = {row['tag_code']} then '{row['tag_name_cn']}'" for cnt, row in tag_df.iterrows() if
             'dev' in row['can_access_env']])
        prod_tag_name_cn = " ".join(
            [f"when tag_code = {row['tag_code']} then '{row['tag_name_cn']}'" for cnt, row in tag_df.iterrows() if
             'prod' in row['can_access_env']])
        local_tag_name_en = " ".join(
            [f"when tag_code = {row['tag_code']} then '{row['tag_name_en']}'" for cnt, row in tag_df.iterrows() if
             'local' in row['can_access_env']])
        dev_tag_name_en = " ".join(
            [f"when tag_code = {row['tag_code']} then '{row['tag_name_en']}'" for cnt, row in tag_df.iterrows() if
             'dev' in row['can_access_env']])
        prod_tag_name_en = " ".join(
            [f"when tag_code = {row['tag_code']} then '{row['tag_name_en']}'" for cnt, row in tag_df.iterrows() if
             'prod' in row['can_access_env']])
        return local_tag_name, dev_tag_name, prod_tag_name, \
               local_tag_name_cn, dev_tag_name_cn, prod_tag_name_cn, \
               local_tag_name_en, dev_tag_name_en, prod_tag_name_en

    def user_insert(self):
        self.source_db.statement = "SELECT * FROM users"
        self.source_db.mode = self.QUERY_MODE
        self.source_db.exec()
        self.columns = [
            'id', 'name', 'email', 'email_verified_at', 'password', 'remember_token', 'created_at', 'updated_at',
            'user_type', 'user_status', 'login_num', 'last_login_date',
            'access_hall_name', 'pwd_updated_time', 'pwd_error_num', 'google_picture_url'
        ]
        self.write_data = self.source_db.fetch_data
        self.table_name = 'users'
        self.mode = self.REPLACE_MODE
        self.exec()

    def ip_whitelist(self):
        self.source_db.statement = "SELECT * FROM access_ip_white_list"
        self.source_db.mode = self.QUERY_MODE
        self.source_db.exec()
        self.columns = [
            'hall_id', 'domain_id', 'ip', 'description'
        ]
        self.write_data = self.source_db.fetch_data
        self.table_name = 'access_ip_white_list'
        self.mode = self.REPLACE_MODE
        self.exec()

    def param_config(self):
        self.source_db.statement = "SELECT * FROM system_param_config"
        self.source_db.mode = self.QUERY_MODE
        self.source_db.exec()
        self.columns = [
            'key', 'value', 'description'
        ]
        self.write_data = self.source_db.fetch_data
        self.table_name = 'system_param_config'
        self.mode = self.REPLACE_MODE
        self.exec()

    def custom_tags(self):
        self.source_db.statement = "SELECT * FROM user_custom_tags_data"
        self.source_db.mode = self.QUERY_MODE
        self.source_db.exec()
        self.columns = [
            'user_id', 'tags_index', 'hall_id', 'domain_id', 'tags_name', 'tags_str', 'created_time'
        ]
        self.write_data = self.source_db.fetch_data
        self.table_name = 'user_custom_tags_data'
        self.mode = self.REPLACE_MODE
        self.exec()

    def activity_analysis(self):
        custom_tags_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/cdp_activity_analysis_data.csv')
        custom_tags_df['domain_id'].replace({0: None}, inplace=True)
        custom_tags_df['create_time'] = pd.to_datetime(custom_tags_df['create_time'], format='%Y-%m-%d %H:%M:%S')
        custom_tags_df = df_timezone_format(custom_tags_df, 'Asia/Taipei')  # 設定時區
        self.columns = custom_tags_df.columns.to_list()
        self.write_data = custom_tags_df.values.tolist()
        self.table_name = 'activity_analysis_data'
        self.mode = self.REPLACE_MODE
        self.exec()

    def menu_config(self):
        self.source_db.statement = (
            "SELECT * "
            "FROM menu_config"
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        self.columns = [
            'parent_item_id', 'item_id', 'item_name', 'folder_path',
            'page_name', 'nav_icon', 'can_access_hall', 'item_order',
            'is_loading_auto_hide', 'item_user_type', 'status', 'status_description', 'can_access_env'
        ]
        self.write_data = self.source_db.fetch_data
        self.table_name = 'menu_config'
        self.mode = self.REPLACE_MODE
        self.exec()

    def tag_info(self):
        self.source_db.statement = (
            "SELECT * "
            "FROM user_tag_info"
        )
        self.source_db.mode = self.source_db.QUERY_MODE
        self.source_db.exec()
        # self.columns = [
        #     'hall_id', 'hall_name', 'domain_id', 'domain_name',
        #     'tag_type', 'tag_code',
        #     'tag_name', 'tag_description', 'tag_category', 'sort_index', 'tag_enabled', 'can_access_env'
        # ]
        self.columns = [
            'hall_id',
            'hall_name',
            'domain_id',
            'domain_name',
            'tag_type',
            'tag_code',
            'tag_name' ,
            'tag_name_cn' ,
            'tag_name_en' ,
            'tag_description' ,
            'tag_description_cn' ,
            'tag_description_en' ,
            'tag_category' ,
            'sort_index' ,
            'tag_enabled' ,
            'can_access_env' ,
            'mutual_tags_code'
        ]
        self.write_data = self.source_db.fetch_data
        self.table_name = 'user_tag_info'
        self.mode = self.REPLACE_MODE
        self.exec()

    def new_tag_info(self):
        tag_info_df = pd.DataFrame()
        # 30069~30073	[規則] 最後平均單筆存款		最後存款日之當日平均單筆存款金額
        # 30074~30078	[規則] 最大有效投注金額		15實動日內單日單款遊戲有效投注最大金額
        # 30079~30090	[規則] 有效投注下降幅度
        # 30200	[規則] 常用入款方式	公司入款	近15天實動日最大金額入款方式為公司入款
        # 30201	[規則] 常用入款方式	加密貨幣入款	近15天實動日最大金額入款方式為加密貨幣入款
        # 30202	[規則] 常用入款方式	人工存入	近15天實動日最大金額入款方式為人工存入
        # 30203	[規則] 常用入款方式	購寶錢包	近15天實動日最大金額入款方式為購寶錢包
        # 30204	[規則] 常用入款方式	CGPAY支付	近15天實動日最大金額入款方式為CGPAY支付
        # 30205	[規則] 常用入款方式	線上存款	近15天實動日最大金額入款方式為線上存款
        # 30206	[規則] 常用入款方式	e點付	近15天實動日最大金額入款方式為e點付
        # 30207	[規則] 常用入款方式	e點富	近15天實動日最大金額入款方式為e點富
        # 30208	[規則] 常用入款方式	OSPAY支付	近15天實動日最大金額入款方式為OSPAY支付

        tag_code_list = [
                {'tag_code': 30069, 'tag_type': 3, 'tag_name': '最後平均單筆存款_1萬', 'tag_description': '最後存款日之當日平均單筆存款金額大於1萬',
                     'tag_category': 1, 'sort_index': 355, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30070,30071,30072,30073'},
                {'tag_code': 30070, 'tag_type': 3, 'tag_name': '最後平均單筆存款_5萬', 'tag_description': '最後存款日之當日平均單筆存款金額大於5萬',
                     'tag_category': 1, 'sort_index': 356, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30069,30071,30072,30072'},
                {'tag_code': 30071, 'tag_type': 3, 'tag_name': '最後平均單筆存款_10萬', 'tag_description': '最後存款日之當日平均單筆存款金額大於10萬',
                     'tag_category': 1, 'sort_index': 357, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30069,30070,30072,30073'},
                {'tag_code': 30072, 'tag_type': 3, 'tag_name': '最後平均單筆存款_15萬', 'tag_description': '最後存款日之當日平均單筆存款金額大於15萬',
                     'tag_category': 1, 'sort_index': 358, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30069,30070,30071,30073'},
                {'tag_code': 30073, 'tag_type': 3, 'tag_name': '最後平均單筆存款_20萬', 'tag_description': '最後存款日之當日平均單筆存款金額大於20萬',
                     'tag_category': 1, 'sort_index': 359, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30069,30070,30071,30072'},

                {'tag_code': 30074, 'tag_type': 3, 'tag_name': '最大有效投注金額_1萬', 'tag_description': '15實動日內單日單款遊戲有效投注最大金額大於1萬',
                     'tag_category': 1, 'sort_index': 360, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30075,30076,30077,30078'},
                {'tag_code': 30075, 'tag_type': 3, 'tag_name': '最大有效投注金額_5萬', 'tag_description': '15實動日內單日單款遊戲有效投注最大金額大於5萬',
                     'tag_category': 1, 'sort_index': 361, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30074,30076,30077,30078'},
                {'tag_code': 30076, 'tag_type': 3, 'tag_name': '最大有效投注金額_10萬', 'tag_description': '15實動日內單日單款遊戲有效投注最大金額大於10萬',
                     'tag_category': 1, 'sort_index': 362, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30074,30075,30077,30078'},
                {'tag_code': 30077, 'tag_type': 3, 'tag_name': '最大有效投注金額_50萬', 'tag_description': '15實動日內單日單款遊戲有效投注最大金額大於50萬',
                     'tag_category': 1, 'sort_index': 363, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30074,30075,30076,30078'},
                {'tag_code': 30078, 'tag_type': 3, 'tag_name': '最大有效投注金額_100萬', 'tag_description': '15實動日內單日單款遊戲有效投注最大金額大於100萬',
                     'tag_category': 1, 'sort_index': 364, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30074,30075,30076,30077'},

                {'tag_code': 30079, 'tag_type': 3, 'tag_name': '有效投注下降幅度_100萬', 'tag_description': '上週與本週有效投注降幅達100萬',
                     'tag_category': 1, 'sort_index': 365, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30080,30081,30082,30083,30084,30085,30086,30087,30088,30089,30090'},
                {'tag_code': 30080, 'tag_type': 3, 'tag_name': '有效投注下降幅度_200萬', 'tag_description': '上週與本週有效投注降幅達200萬',
                     'tag_category': 1, 'sort_index': 366, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30079,30081,30082,30083,30084,30085,30086,30087,30088,30089,30090'},
                {'tag_code': 30081, 'tag_type': 3, 'tag_name': '有效投注下降幅度_300萬', 'tag_description': '上週與本週有效投注降幅達300萬',
                     'tag_category': 1, 'sort_index': 367, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30079,30080,30082,30083,30084,30085,30086,30087,30088,30089,30090'},
                {'tag_code': 30082, 'tag_type': 3, 'tag_name': '有效投注下降幅度_500萬', 'tag_description': '上週與本週有效投注降幅達500萬',
                     'tag_category': 1, 'sort_index': 368, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30079,30080,30081,30083,30084,30085,30086,30087,30088,30089,30090'},
                {'tag_code': 30083, 'tag_type': 3, 'tag_name': '有效投注下降幅度_700萬', 'tag_description': '上週與本週有效投注降幅達700萬',
                     'tag_category': 1, 'sort_index': 369, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30079,30080,30081,30082,30084,30085,30086,30087,30088,30089,30090'},
                {'tag_code': 30084, 'tag_type': 3, 'tag_name': '有效投注下降幅度_900萬', 'tag_description': '上週與本週有效投注降幅達900萬',
                     'tag_category': 1, 'sort_index': 370, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30079,30080,30081,30082,30083,30085,30086,30087,30088,30089,30090'},
                {'tag_code': 30085, 'tag_type': 3, 'tag_name': '有效投注下降幅度_1200萬', 'tag_description': '上週與本週有效投注降幅達1200萬',
                     'tag_category': 1, 'sort_index': 371, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30079,30080,30081,30082,30083,30084,30086,30087,30088,30089,30090'},
                {'tag_code': 30086, 'tag_type': 3, 'tag_name': '有效投注下降幅度_1500萬', 'tag_description': '上週與本週有效投注降幅達1500萬',
                     'tag_category': 1, 'sort_index': 372, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30079,30080,30081,30082,30083,30084,30085,30087,30088,30089,30090'},
                {'tag_code': 30087, 'tag_type': 3, 'tag_name': '有效投注下降幅度_2000萬', 'tag_description': '上週與本週有效投注降幅達2000萬',
                     'tag_category': 1, 'sort_index': 373, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30079,30080,30081,30082,30083,30084,30085,30086,30088,30089,30090'},
                {'tag_code': 30088, 'tag_type': 3, 'tag_name': '有效投注下降幅度_3000萬', 'tag_description': '上週與本週有效投注降幅達3000萬',
                     'tag_category': 1, 'sort_index': 374, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30079,30080,30081,30082,30083,30084,30085,30086,30087,30089,30090'},
                {'tag_code': 30089, 'tag_type': 3, 'tag_name': '有效投注下降幅度_4000萬', 'tag_description': '上週與本週有效投注降幅達4000萬',
                     'tag_category': 1, 'sort_index': 375, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30079,30080,30081,30082,30083,30084,30085,30086,30087,30088,30090'},
                {'tag_code': 30090, 'tag_type': 3, 'tag_name': '有效投注下降幅度_5000萬', 'tag_description': '上週與本週有效投注降幅達5000萬',
                     'tag_category': 1, 'sort_index': 376, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30079,30080,30081,30082,30083,30084,30085,30086,30087,30088,30089'},

                {'tag_code': 30200, 'tag_type': 3, 'tag_name': '常用入款_公司入款', 'tag_description': '會員近15個實動日，在 公司入款 總金額最高',
                     'tag_category': 1, 'sort_index': 377, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30201,30202,30203,30204,30205,30206,30207,30208'},
                {'tag_code': 30201, 'tag_type': 3, 'tag_name': '常用入款_加密貨幣入款', 'tag_description': '會員近15個實動日，在 加密貨幣入款 總金額最高',
                     'tag_category': 1, 'sort_index': 378, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30200,30202,30203,30204,30205,30206,30207,30208'},
                {'tag_code': 30202, 'tag_type': 3, 'tag_name': '常用入款_人工存入', 'tag_description': '會員近15個實動日，在 人工存入 總金額最高',
                     'tag_category': 1, 'sort_index': 379, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30200,30201,30203,30204,30205,30206,30207,30208'},
                {'tag_code': 30203, 'tag_type': 3, 'tag_name': '常用入款_購寶錢包', 'tag_description': '會員近15個實動日，在 購寶錢包 總金額最高',
                     'tag_category': 1, 'sort_index': 380, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30200,30201,30202,30204,30205,30206,30207,30208'},
                {'tag_code': 30204, 'tag_type': 3, 'tag_name': '常用入款_CGPAY支付', 'tag_description': '會員近15個實動日，在 CGPAY支付 總金額最高',
                     'tag_category': 1, 'sort_index': 381, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30200,30201,30202,30203,30205,30206,30207,30208'},
                {'tag_code': 30205, 'tag_type': 3, 'tag_name': '常用入款_線上存款', 'tag_description': '會員近15個實動日，在 線上存款 總金額最高',
                     'tag_category': 1, 'sort_index': 382, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30200,30201,30202,30203,30204,30206,30207,30208'},
                {'tag_code': 30206, 'tag_type': 3, 'tag_name': '常用入款_e點付', 'tag_description': '會員近15個實動日，在 e點付 總金額最高',
                     'tag_category': 1, 'sort_index': 383, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30200,30201,30202,30203,30204,30205,30207,30208'},
                {'tag_code': 30207, 'tag_type': 3, 'tag_name': '常用入款_e點富', 'tag_description': '會員近15個實動日，在 e點富 總金額最高',
                     'tag_category': 1, 'sort_index': 384, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30200,30201,30202,30203,30204,30205,30206,30208'},
                {'tag_code': 30208, 'tag_type': 3, 'tag_name': '常用入款_OSPAY支付', 'tag_description': '會員近15個實動日，在 OSPAY支付 總金額最高',
                     'tag_category': 1, 'sort_index': 385, 'tag_enabled': True, 'can_access_env': 'local,dev,prod',
                     'mutual_tags_code': '30200,30201,30202,30203,30204,30205,30206,30207'},
        ]

        tag_code_list = [
            {'tag_code': 30091, 'tag_type': 3, 'tag_name': '猶豫客', 'tag_name_cn': '犹豫客', 'tag_name_en': 'Hesitate',
             'tag_description': '從未存款，近二週內登入三次以上，或曾經有存款，倒數第五次的登入後無下注及存款',
             'tag_description_cn': '从来未存款，近二周内登入三次以上 或 曾经有存款，倒数第五次的登入后无下注及存款',
             'tag_description_en': 'No deposit record and login more than 3 times in last two weeks or with deposit record but no bet and deposit after last 5 login',
             'tag_category': 1, 'sort_index': 3009100, 'tag_enabled': False, 'can_access_env': 'local,dev,prod',
             'mutual_tags_code': ''},
            {'tag_code': 30092, 'tag_type': 3, 'tag_name': '實名驗證', 'tag_name_cn': '实名验证', 'tag_name_en': 'KYC',
             'tag_description': '會員手機或Email已被驗證',
             'tag_description_cn': '会员手机或Email已被验证',
             'tag_description_en': 'Verified cell phone number or e-mail address.',
             'tag_category': 1, 'sort_index': 3009200, 'tag_enabled': False, 'can_access_env': 'local,dev,prod',
             'mutual_tags_code': ''},
            {'tag_code': 30093, 'tag_type': 3, 'tag_name': '新會員', 'tag_name_cn': '新会员', 'tag_name_en': 'NewGambler',
             'tag_description': '有效投注>0，註冊日期為30天內',
             'tag_description_cn': '有效投注 > 0，注册日期为30天内',
             'tag_description_en': 'Registered within 30 days.',
             'tag_category': 1, 'sort_index': 3009300, 'tag_enabled': False, 'can_access_env': 'local,dev,prod',
             'mutual_tags_code': '30094,30095,30096'},
            {'tag_code': 30094, 'tag_type': 3, 'tag_name': '初期會員', 'tag_name_cn': '初期会员', 'tag_name_en': 'EarlyGambler',
             'tag_description': '有效投注>0，註冊日期介於30天到90天',
             'tag_description_cn': '有效投注 > 0，注册日期介于30天到90天',
             'tag_description_en': 'Registered between 30 and 90 days.',
             'tag_category': 1, 'sort_index': 3009400, 'tag_enabled': False, 'can_access_env': 'local,dev,prod',
             'mutual_tags_code': '30093,30095,30096'},
            {'tag_code': 30095, 'tag_type': 3, 'tag_name': '中期會員', 'tag_name_cn': '中期会员',
             'tag_name_en': 'MidTermGambler',
             'tag_description': '有效投注>0，註冊日期介於90天到180天',
             'tag_description_cn': '有效投注 > 0，注册日期介于90天到180天',
             'tag_description_en': 'Registered between 90 and 180 days.',
             'tag_category': 1, 'sort_index': 3009500, 'tag_enabled': False, 'can_access_env': 'local,dev,prod',
             'mutual_tags_code': '30093,30094,30096'},
            {'tag_code': 30096, 'tag_type': 3, 'tag_name': '長期會員', 'tag_name_cn': '长期会员',
             'tag_name_en': 'LongTermGambler',
             'tag_description': '有效投注>0，註冊日期早於180天前',
             'tag_description_cn': '有效投注 > 0，注册日期早于180天前',
             'tag_description_en': 'Registered more than 180 days.',
             'tag_category': 1, 'sort_index': 3009600, 'tag_enabled': False, 'can_access_env': 'local,dev,prod',
             'mutual_tags_code': '30093,30094,30095'},
        ]
        for tag_dict in tag_code_list:
            df = pd.DataFrame([tag_dict])
            tag_info_df = pd.concat([tag_info_df, df], ignore_index=True)

        for hall_name, hall in hall_dict.items():
            bbin_hall = hall_dict[hall_name]()
            print('hall name: ', bbin_hall.hall_name)
            tag_info_df['hall_id'] = bbin_hall.hall_id
            tag_info_df['hall_name'] = bbin_hall.hall_name
            tag_info_df['domain_id'] = 0
            tag_info_df['domain_name'] = None
            self.upsert_data_with('user_tag_info', tag_info_df)