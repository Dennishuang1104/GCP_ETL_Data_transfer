import pandas as pd

from Adaptors.SpannerAdaptor import CDP_BBOS
from datetime import timedelta, date


class NAPL:
    def __init__(self, spanner_db: CDP_BBOS):
        self.spanner_db = spanner_db
        self.telegram_message = ''
        self.tag_type = 3
        self.target_bet_df: pd.DataFrame() = pd.DataFrame()
        self.data_date = date.today() - timedelta(days=1)  # yesterday
        self.ods_data_columns = ['hall_id', 'domain_id', 'user_id', 'user_name',
                                 'tag_code', 'tag_type', 'tag_enabled', 'register_date', 'updated_time']

    def get_member_df(self, hall_id, domain_id, s_date, e_date):
        """
            取得會員資訊
        :param hall_id: 平台編號
        :param domain_id: 廳主編號
        :param s_date: 搜尋起始目標日期
        :param e_date: 搜尋截止目標日期
        :return: 回傳 會員資訊的df
        """
        # condition = f"""and Date(register_date) >= '{s_date}' and Date(register_date) <= '{e_date}'"""
        condition = f"""and Date(register_date, 'America/New_York') >= '{s_date}' and Date(register_date, 'America/New_York') <= '{e_date}'"""

        self.spanner_db.mode = self.spanner_db.QUERY_MODE
        self.spanner_db.statement = f"""
            select 
                hall_id, domain_id, user_id, user_name, FORMAT_TIMESTAMP('%F %X', register_date, 'America/New_York') as register_date
            from member_info
            where hall_id = {hall_id} and domain_id = {domain_id}
                  {condition}
        """
        self.spanner_db.exec()
        member_df = pd.DataFrame(data=self.spanner_db.fetch_data, columns=["hall_id", "domain_id", "user_id", "user_name", "register_date"]).reset_index(drop=True)
        member_df[["hall_id", "domain_id", "user_id"]] = member_df[["hall_id", "domain_id", "user_id"]].astype(int)
        member_df['user_name'] = member_df['user_name'].astype(str)
        member_df["register_date"] = pd.to_datetime(member_df['register_date'])
        return member_df

    def get_activated_tag(self, hall_id, domain_id, s_date, e_date):
        """
            取得會員營運標資訊
        :param hall_id: 平台編號
        :param domain_id: 廳主編號
        :param target_date: 搜尋目標日期
        :return: 回傳 會員營運標資訊的df
        """

        self.spanner_db.mode = self.spanner_db.QUERY_MODE
        self.spanner_db.statement = f"""
            SELECT  
                hall_id, domain_id, user_id, user_name, data_date, tag_code as currently_tag_code
            FROM activated_tag_day
            where hall_id = {hall_id} and domain_id = {domain_id}
                  and data_date >= '{s_date}' and data_date < '{e_date}';
        """
        self.spanner_db.exec()
        activated_tag_df = pd.DataFrame(data=self.spanner_db.fetch_data, columns=["hall_id", "domain_id", "user_id", "user_name", 'data_date', "currently_tag_code"]).reset_index(drop=True)
        activated_tag_df[["hall_id", "domain_id", "user_id"]] = activated_tag_df[["hall_id", "domain_id", "user_id"]].astype(int)
        activated_tag_df['user_name'] = activated_tag_df['user_name'].astype(str)
        activated_tag_df['currently_tag_code'] = activated_tag_df['currently_tag_code'].astype(float)
        activated_tag_df["data_date"] = pd.to_datetime(activated_tag_df['data_date'])
        return activated_tag_df

    def generate_activated_ods_week(self, hall_id, domain_id, fin_year, fin_month, fin_week, diff_days):
        self.spanner_db.mode = self.spanner_db.QUERY_MODE
        self.spanner_db.statement = "WITH fin AS ( " \
                         "SELECT " \
                         f"     {hall_id} as hall_id, {domain_id} as domain_id, fin_year, fin_month, fin_week " \
                         "FROM financial_date_map " \
                         f"WHERE date = (SELECT DATE_ADD(MIN(date), INTERVAL -{diff_days} DAY) FROM financial_date_map WHERE fin_year = {fin_year} AND fin_month = {fin_month} AND fin_week = {fin_week})) " \
                         "SELECT " \
                         "    hall_id, domain_id, user_id, user_name, tag_code " \
                         "FROM activated_tag_ods_week@{FORCE_INDEX='idx_tag_ods_week_user_name_financial_created_time_tag_code'} as ods_week " \
                         "where (hall_id, domain_id, user_id, created_time) IN " \
                         "    (SELECT (max_data.hall_id, max_data.domain_id, max_data.user_id, max_data.max_time) FROM " \
                         "    (  SELECT " \
                         "            hall_id, domain_id, user_id, fin_year, fin_month, fin_week, MIN(created_time) AS max_time " \
                         "       FROM activated_tag_ods_week@{FORCE_INDEX='idx_tag_ods_fin_created_time'} " \
                         "       WHERE (hall_id, domain_id, fin_year, fin_month, fin_week) in (select (hall_id, domain_id, fin_year, fin_month, fin_week) from fin) " \
                         "       GROUP BY hall_id, domain_id, user_id, fin_year, fin_month, fin_week  " \
                         "    ) max_data) ;"
        self.spanner_db.exec()
        df = pd.DataFrame(data=self.spanner_db.fetch_data, columns=['hall_id', 'domain_id', 'user_id', 'user_name', 'tag_code'])
        df[["hall_id", "domain_id", "user_id", "tag_code"]] = df[["hall_id", "domain_id", "user_id", "tag_code"]].astype(int)
        df['user_name'] = df['user_name'].astype(str)
        return df

    def generate_activated_dw_week(self, hall_id, domain_id, fin_year, fin_month, fin_week, diff_days):
        self.spanner_db.mode = self.spanner_db.QUERY_MODE
        self.spanner_db.statement = "WITH fin AS ( " \
                         "SELECT " \
                         f"     {hall_id} as hall_id, {domain_id} as domain_id, fin_year, fin_month, fin_week " \
                         "FROM financial_date_map " \
                         f"WHERE date = (SELECT DATE_ADD(MIN(date), INTERVAL -{diff_days} DAY) FROM financial_date_map WHERE fin_year = {fin_year} AND fin_month = {fin_month} AND fin_week = {fin_week})) " \
                         "SELECT " \
                         "    hall_id, domain_id, user_id, user_name, " \
                         "    last_week_step AS dw_last_week_step, this_week_step AS dw_this_week_step, " \
                         "    two_weeks_tag_code AS dw_two_weeks_tag_code, one_weeks_tag_code AS dw_one_weeks_tag_code, week_tag_code AS dw_week_tag_code " \
                         "FROM activated_tag_dw_week " \
                         "WHERE (hall_id, domain_id, user_id, fin_year, fin_month, fin_week, created_time) IN ( " \
                         "    SELECT (max_data.hall_id, max_data.domain_id, max_data.user_id, max_data.fin_year, max_data.fin_month, max_data.fin_week, max_data.max_time) FROM " \
                         "    (  SELECT " \
                         "            hall_id, domain_id, user_id, fin_year, fin_month, fin_week, MIN(created_time) AS max_time " \
                         "       FROM activated_tag_dw_week " \
                         "       WHERE (hall_id, domain_id, fin_year, fin_month, fin_week) in (select (hall_id, domain_id, fin_year, fin_month, fin_week) from fin) " \
                         "       GROUP BY hall_id, domain_id, user_id, fin_year, fin_month, fin_week  " \
                         "    ) max_data) ;"
        self.spanner_db.exec()
        df = pd.DataFrame(data=self.spanner_db.fetch_data, columns=['hall_id', 'domain_id', 'user_id', 'user_name', 'dw_last_week_step', 'dw_this_week_step', 'dw_two_weeks_tag_code', 'dw_one_weeks_tag_code', 'dw_week_tag_code'])
        df[["hall_id", "domain_id", "user_id", 'dw_last_week_step', 'dw_this_week_step', 'dw_two_weeks_tag_code', 'dw_one_weeks_tag_code', 'dw_week_tag_code']] = df[ ["hall_id", "domain_id", "user_id", 'dw_last_week_step', 'dw_this_week_step', 'dw_two_weeks_tag_code', 'dw_one_weeks_tag_code', 'dw_week_tag_code']].astype(int)
        df['user_name'] = df['user_name'].astype(str)
        return df

    def get_operate_df(self, hall_id, domain_id, s_date, e_date):
        """
            串接所有所需資訊，包含最後登入時間、最後註冊時間、最後存款時間、最後下注時間、存款次數、下注次數等，
            結合產生每位使用者的營運標籤資訊
        :param hall_id:平台編號
        :param domain_id: 廳主編號
        :param s_date: 搜尋起始目標日期
        :param e_date: 搜尋截止目標日期
        :return: 回傳 營運標籤資訊 的df
        """
        try:
            condition = f"""and data_date >= '{s_date}' and data_date <= '{e_date}'"""

            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.statement = f"""
                select 
                    {hall_id} as hall_id, {domain_id} as domain_id, 
                    IFNULL(IFNULL(login_data.user_id, dep_data.user_id), bet_data.user_id) as user_id,
                    IFNULL(login_data.last_login_date, DATE('1990-01-01')) as last_login_date, -- 最後登入時間
                    GREATEST(IFNULL(bet_data.last_bet_date, DATE('1990-01-01')), IFNULL(dep_data.last_deposit_date, DATE('1990-01-01'))) as activated_date, -- 最後存款時間 與 最後下注時間 取最大值為 最後實動日
                    IFNULL(dep_data.deposit_count , 0) as deposit_count, -- 存款次數,
                    IFNULL(bet_data.wagers_total, 0) as wagers_total -- 下注次數
                from (
                    select 
                        user_id, max(data_date) as last_login_date
                    from login_log
                    where hall_id = {hall_id} and domain_id = {domain_id}
                          {condition}
                    group by user_id -- 最後登入資訊
                ) login_data
                FULL OUTER JOIN (
                    select 
                       user_id, 
                        max(data_date) as last_deposit_date, sum(deposit_count) as deposit_count
                    from deposit_withdraw_record
                    where hall_id = {hall_id} and domain_id = {domain_id}
                          and (deposit_count != 0 or deposit_amount != 0)
                          {condition}
                    group by user_id -- 存款資訊
                ) dep_data on login_data.user_id = dep_data.user_id
                FULL OUTER JOIN (
                    select 
                        user_id, max(data_date) as last_bet_date, sum(wagers_total) as wagers_total
                    from bet_analysis
                    where hall_id = {hall_id} and domain_id = {domain_id}
                          {condition}
                    group by user_id -- 下注資訊
                ) bet_data on login_data.user_id = bet_data.user_id
            """
            self.spanner_db.exec()
            operate_df = pd.DataFrame(self.spanner_db.fetch_data, columns=["hall_id", "domain_id", "user_id", "last_login_date", "activated_date", "deposit_count", "wagers_total"])
            operate_df[["hall_id", "domain_id", "user_id", "deposit_count", "wagers_total"]] = operate_df[["hall_id", "domain_id", "user_id", "deposit_count", "wagers_total"]].astype(int)
            operate_df["last_login_date"] = pd.to_datetime(operate_df['last_login_date'])
            operate_df["activated_date"] = pd.to_datetime(operate_df['activated_date'])
            return operate_df
        except Exception as e:
            raise

    def get_fin_df(self, s_date, e_date):
        try:
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.statement = f"""
                select 
                    distinct fin_year, fin_month, fin_week, fin_week_end
                from financial_date_map
                where date >= '{s_date}' and date <= '{e_date}'
                order by fin_year, fin_month, fin_week;
            """
            self.spanner_db.exec()
            fin_df = pd.DataFrame(self.spanner_db.fetch_data, columns=["fin_year", "fin_month", "fin_week", "fin_week_end"])
            fin_df[["fin_year", "fin_month", "fin_week"]] = fin_df[["fin_year", "fin_month", "fin_week"]].astype(int)
            fin_df["fin_week_end"] = pd.to_datetime(fin_df['fin_week_end'])
            return fin_df
        except Exception as e:
            raise

    def get_step(self, dw_df: pd.DataFrame(),
                 step_column: str, this_tag_column: str, last_tag_column: str) -> pd.DataFrame():
        try:
            dw_df[step_column] = 0
            dw_df.loc[
                (dw_df[last_tag_column].isin([50102])) & (dw_df[this_tag_column].isin([50102])), step_column] = 1
            dw_df.loc[((dw_df[last_tag_column].isna()) | (dw_df[last_tag_column].isin([50107, 50108]))) & (
                dw_df[this_tag_column].isin([50100, 50101, 50102])), step_column] = 2
            dw_df.loc[(dw_df[last_tag_column].isin([50100])) & (
                dw_df[this_tag_column].isin([50101])), step_column] = 2
            dw_df.loc[(dw_df[last_tag_column].isin([50100, 50101])) & (
                dw_df[this_tag_column].isin([50102])), step_column] = 3
            dw_df.loc[(dw_df[last_tag_column].isin([50103, 50104, 50105])) & (
                dw_df[this_tag_column].isin([50100, 50101, 50102])), step_column] = 4
            dw_df.loc[(dw_df[last_tag_column].isin([50105, 50106])) & (
                dw_df[this_tag_column].isin([50100, 50101, 50102])), step_column] = 5
            dw_df.loc[(dw_df[last_tag_column].isin([50100, 50101, 50102])) & (
                dw_df[this_tag_column].isin([50103, 50104, 50105])), step_column] = 6
            dw_df.loc[(dw_df[last_tag_column].isin([50103, 50104, 50105])) & (
                dw_df[this_tag_column].isin([50106])), step_column] = 7
        except Exception as e:
            raise
        return dw_df