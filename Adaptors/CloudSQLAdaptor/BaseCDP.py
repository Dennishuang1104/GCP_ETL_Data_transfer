import time
import pandas as pd

from Environment import cfg
from mysql.MySQL import DBParams, MySqlAdaptor, MySqlDBConnection


class BaseCDP(MySqlAdaptor):
    def __init__(self, db_config: str):
        db_params = DBParams(
            host=cfg.get(db_config, 'HOST'),
            user=cfg.get(db_config, 'USER'),
            password=cfg.get(db_config, 'PASSWORD'),
            db=cfg.get(db_config, 'DB_NAME'),
            port=int(cfg.get(db_config, 'PORT'))
        )

        super(BaseCDP, self).__init__(MySqlDBConnection(db_params))

    def insert_data_with(self, insert_mode: int, table_name: str, audit_data: pd.DataFrame) -> int:
        """
            sync table data with audit
        :param insert_mode: 0: INSERT INTO, 1: REPLACE INTO, 2: INSERT IGNORE INTO
        :param table_name: target table name
        :param audit_data: source audit data with group by user or keys
        :return:
        """

        st = time.time()

        sync_size = len(audit_data.index)

        insert_type_dict = {0: 'INSERT INTO', 1: 'REPLACE INTO', 2: 'INSERT IGNORE INTO'}

        insert_type = insert_type_dict[insert_mode]

        if sync_size > 0:
            try:

                for column_name in audit_data.columns:
                    if column_name == 'tag_enabled':
                        continue
                    audit_data[column_name] = audit_data[column_name].apply(str)

                self.mode = self.INSERT_MODE
                self.write_data = audit_data.values.tolist()
                self.statement = (
                        f'{insert_type} {table_name} (' + ','.join(audit_data.keys()) + ') '
                        'VALUES (' + ','.join(['%s'] * len(audit_data.keys())) + ')  '
                )
                self.exec()

                sync_size = self.write_rowcount

            except Exception as e:
                raise

        et = time.time()
        print(f'{insert_type} {table_name} cost {et - st} seconds ')

        return sync_size

    def delete_data_with(self, table_name: str, condition: str=None):
        self.mode = self.DELETE_MODE
        self.statement = f"""DELETE FROM {table_name} {condition}"""
        self.exec()

    def get_member_df(self, cdp_schema, hall_id, domain_id, s_date, e_date):
        """
            取得會員資訊
        :param hall_id: 平台編號
        :param domain_id: 廳主編號
        :param s_date: 搜尋起始目標日期
        :param e_date: 搜尋截止目標日期
        :return: 回傳 會員資訊的df
        """
        condition = f"""and Date(register_date) >= '{s_date}' and Date(register_date) <= '{e_date}'"""

        self.mode = self.QUERY_MODE
        self.statement = f"""
            select 
                hall_id, domain_id, user_id, user_name, Date(register_date) as register_date
            from {cdp_schema}.member_info
            where hall_id = {hall_id} and domain_id = {domain_id}
                  {condition}
        """
        self.exec()
        member_df = pd.DataFrame(data=self.fetch_data, columns=["hall_id", "domain_id", "user_id", "user_name", "register_date"]).reset_index(drop=True)
        member_df[["hall_id", "domain_id", "user_id", "user_name"]] = member_df[["hall_id", "domain_id", "user_id", "user_name"]].astype(str)
        member_df["register_date"] = pd.to_datetime(member_df['register_date'])
        return member_df

    def get_activated_tag(self, cdp_schema, hall_id, domain_id, target_date):
        """
            取得會員營運標資訊
        :param hall_id: 平台編號
        :param domain_id: 廳主編號
        :param target_date: 搜尋目標日期
        :return: 回傳 會員營運標資訊的df
        """

        self.mode = self.QUERY_MODE
        self.statement = f"""
            SELECT  
                hall_id, domain_id, user_id, user_name,tag_code as currently_tag_code, data_date
            FROM {cdp_schema}.activated_tag_day
            where hall_id = {hall_id} and domain_id = {domain_id}
              and data_date < '{target_date}';
        """
        self.exec()
        activated_tag_df = pd.DataFrame(data=self.fetch_data, columns=["hall_id", "domain_id", "user_id", "user_name", "currently_tag_code", 'data_date']).reset_index(drop=True)
        activated_tag_df[["hall_id", "domain_id", "user_id", "user_name"]] = activated_tag_df[["hall_id", "domain_id", "user_id", "user_name"]].astype(str)
        activated_tag_df[["currently_tag_code"]] = activated_tag_df[["currently_tag_code"]].astype(float)
        activated_tag_df["data_date"] = pd.to_datetime(activated_tag_df['data_date'])
        return activated_tag_df

    def generate_activated_dw_week(self, cdp_schema, hall_id, domain_id, fin_year, fin_month, fin_week):
        self.mode = self.QUERY_MODE
        self.statement = f"""
            Select 
                hall_id, domain_id, user_id, user_name, 
                dw_last_week_step, dw_this_week_step, 
                dw_two_weeks_tag_code, dw_one_weeks_tag_code, dw_week_tag_code,
                last_week_step, this_week_step,
                two_weeks_tag_code, one_weeks_tag_code, week_tag_code, 
                {fin_year} as fin_year, {fin_month} as fin_month, {fin_week} as fin_week
            from
            (
                Select 
                    week.hall_id, week.domain_id, week.user_id, week.user_name, 
                    ifnull(dw.dw_last_week_step, "") as dw_last_week_step,
                    ifnull(dw.dw_this_week_step, "") as dw_this_week_step,
                    ifnull(dw.dw_two_weeks_tag_code, "") as dw_two_weeks_tag_code,
                    ifnull(dw.dw_one_weeks_tag_code, "") as dw_one_weeks_tag_code,
                    ifnull(dw.dw_week_tag_code, "") as dw_week_tag_code,
                    case
                    when (two_week.two_weeks_tag_code is null or two_week.two_weeks_tag_code in ('50102')) and one_week.one_weeks_tag_code in ('50102') then '1'
                    when ((two_week.two_weeks_tag_code is null or two_week.two_weeks_tag_code in ('50107', '50108')) and one_week.one_weeks_tag_code in ('50100', '50101', '50102')) then '2'
                    when ((two_week.two_weeks_tag_code is null or two_week.two_weeks_tag_code in ('50100', '50101')) and one_week.one_weeks_tag_code in ('50102')) then '3'
                    when ((two_week.two_weeks_tag_code is null or two_week.two_weeks_tag_code in ('50103', '50104', '50105')) and one_week.one_weeks_tag_code in ('50101', '50102')) then '4'
                    when ((two_week.two_weeks_tag_code is null or two_week.two_weeks_tag_code in ('50105', '50106')) and one_week.one_weeks_tag_code in ('50101', '50102')) then '5'
                    when ((two_week.two_weeks_tag_code is null or two_week.two_weeks_tag_code in ('50100', '50101', '50102')) and one_week.one_weeks_tag_code in ('50103', '50104', '50105')) then '6'
                    when ((two_week.two_weeks_tag_code is null or two_week.two_weeks_tag_code in ('50103', '50104', '50105')) and one_week.one_weeks_tag_code in ('50106')) then '7'
                    else ''
                    end last_week_step,
                    case
                    when ((one_week.one_weeks_tag_code is null or one_week.one_weeks_tag_code in ('50102')) and week.week_tag_code in ('50102')) then '1'
                    when ((one_week.one_weeks_tag_code is null or one_week.one_weeks_tag_code in ('50107', '50108')) and week.week_tag_code in ('50100', '50101', '50102')) then '2'
                    when ((one_week.one_weeks_tag_code is null or one_week.one_weeks_tag_code in ('50100', '50101')) and week.week_tag_code in ('50102')) then '3'
                    when ((one_week.one_weeks_tag_code is null or one_week.one_weeks_tag_code in ('50103', '50104', '50105')) and week.week_tag_code in ('50101', '50102')) then '4'
                    when ((one_week.one_weeks_tag_code is null or one_week.one_weeks_tag_code in ('50105', '50106')) and week.week_tag_code in ('50101', '50102')) then '5'
                    when ((one_week.one_weeks_tag_code is null or one_week.one_weeks_tag_code in ('50100', '50101', '50102')) and week.week_tag_code in ('50103', '50104', '50105')) then '6'
                    when ((one_week.one_weeks_tag_code is null or one_week.one_weeks_tag_code in ('50103', '50104', '50105')) and week.week_tag_code in ('50106')) then '7'
                    else ''
                    end this_week_step,
                    ifnull(two_week.two_weeks_tag_code, "") as two_weeks_tag_code, 
                    ifnull(one_week.one_weeks_tag_code, "") as one_weeks_tag_code, 
                    ifnull(week.week_tag_code, "") as week_tag_code
                from (
                    SELECT
                        hall_id, domain_id, user_id, user_name, tag_code as week_tag_code
                    FROM {cdp_schema}.activated_tag_ods_week
                    where (hall_id, domain_id, user_id, fin_year, fin_month, fin_week, created_time) in ((
                            SELECT
                                 hall_id, domain_id, user_id, fin_year, fin_month, fin_week, min(created_time) as max_time
                            FROM {cdp_schema}.activated_tag_ods_week
                            where hall_id = {hall_id} and domain_id = {domain_id}
                                  and (fin_year, fin_month, fin_week) in (({fin_year}, {fin_month}, {fin_week}))
                            group by hall_id, domain_id, user_id
                          ))
                ) week -- 本週營運標籤
                left join(
                    SELECT
                        hall_id, domain_id, user_id, user_name, tag_code as one_weeks_tag_code
                    FROM {cdp_schema}.activated_tag_ods_week
                    where (hall_id, domain_id, user_id, fin_year, fin_month, fin_week, created_time) in ((
                            SELECT
                                 hall_id, domain_id, user_id, fin_year, fin_month, fin_week, min(created_time) as max_time
                            FROM {cdp_schema}.activated_tag_ods_week
                            where hall_id = {hall_id} and domain_id = {domain_id}
                                  and (fin_year, fin_month, fin_week) in ((
                                  select  
                                        fin_year, fin_month, fin_week
                                    from {cdp_schema}.financial_date_map
                                    where date = 
                                    (select 
                                        DATE(min(date)) - INTERVAL 7 DAY
                                    from {cdp_schema}.financial_date_map
                                    where fin_year = {fin_year} and fin_month = {fin_month} and fin_week = {fin_week})))
                            group by hall_id, domain_id, user_id, fin_year, fin_month, fin_week
                          ))
                ) one_week on one_week.hall_id = week.hall_id and one_week.domain_id = week.domain_id and one_week.user_id = week.user_id and one_week.user_name = week.user_name -- 『上』週營運標籤
                left join(
                    SELECT
                        hall_id, domain_id, user_id, user_name, tag_code as two_weeks_tag_code
                    FROM {cdp_schema}.activated_tag_ods_week
                    where (hall_id, domain_id, user_id, fin_year, fin_month, fin_week, created_time) in ((
                            SELECT
                                 hall_id, domain_id, user_id, fin_year, fin_month, fin_week, min(created_time) as max_time
                            FROM {cdp_schema}.activated_tag_ods_week
                            where hall_id = {hall_id} and domain_id = {domain_id}
                                  and (fin_year, fin_month, fin_week) in ((
                                  select  
                                        fin_year, fin_month, fin_week
                                    from {cdp_schema}.financial_date_map
                                    where date = 
                                    (select 
                                        DATE(min(date)) - INTERVAL 14 DAY
                                    from {cdp_schema}.financial_date_map
                                    where fin_year = {fin_year} and fin_month = {fin_month} and fin_week = {fin_week})))
                            group by hall_id, domain_id, user_id, fin_year, fin_month, fin_week
                          ))
                ) two_week on two_week.hall_id = week.hall_id and two_week.domain_id = week.domain_id and two_week.user_id = week.user_id and two_week.user_name = week.user_name -- 『上上』週營運標籤
                left join(
                    SELECT
                        hall_id, domain_id, user_id, user_name,
                        last_week_step as dw_last_week_step, this_week_step as dw_this_week_step, 
                        two_weeks_tag_code as dw_two_weeks_tag_code, one_weeks_tag_code as dw_one_weeks_tag_code, week_tag_code as dw_week_tag_code
                    FROM {cdp_schema}.activated_tag_dw_week
                    where (hall_id, domain_id, user_id, fin_year, fin_month, fin_week, created_time) in ((
                            SELECT
                                 hall_id, domain_id, user_id, fin_year, fin_month, fin_week, min(created_time) as max_time
                            FROM {cdp_schema}.activated_tag_dw_week
                            where hall_id = {hall_id} and domain_id = {domain_id}
                                  and (fin_year, fin_month, fin_week) in ((
                                  select  
                                        fin_year, fin_month, fin_week
                                    from {cdp_schema}.financial_date_map
                                    where date = 
                                    (select 
                                        DATE(min(date)) - INTERVAL 7 DAY
                                    from {cdp_schema}.financial_date_map
                                    where fin_year = {fin_year} and fin_month = {fin_month} and fin_week = {fin_week})))
                            group by hall_id, domain_id, user_id, fin_year, fin_month, fin_week
                          ))
                ) dw on dw.hall_id = week.hall_id and dw.domain_id = week.domain_id and dw.user_id = week.user_id and dw.user_name = week.user_name -- 『上次』週營運標籤
            ) result
            where dw_last_week_step != '' or dw_this_week_step != ''
                          or last_week_step != '' or this_week_step != '';
        """
        self.exec()

        activated_tag_dw_df = pd.DataFrame(data=self.fetch_data, columns=["hall_id", "domain_id", "user_id", "user_name", "dw_last_week_step", "dw_this_week_step", "dw_two_weeks_tag_code", "dw_one_weeks_tag_code", "dw_week_tag_code", "last_week_step", "this_week_step", "two_weeks_tag_code", "one_weeks_tag_code", "week_tag_code", "fin_year", "fin_month", "fin_week"]).reset_index(drop=True)
        activated_tag_dw_df[["hall_id", "domain_id", "user_id", "user_name", "dw_last_week_step", "dw_this_week_step", "dw_two_weeks_tag_code", "dw_one_weeks_tag_code", "dw_week_tag_code", "last_week_step", "this_week_step", "two_weeks_tag_code", "one_weeks_tag_code", "week_tag_code", "fin_year", "fin_month", "fin_week"]] = activated_tag_dw_df[["hall_id", "domain_id", "user_id", "user_name", "dw_last_week_step", "dw_this_week_step", "dw_two_weeks_tag_code", "dw_one_weeks_tag_code", "dw_week_tag_code", "last_week_step", "this_week_step", "two_weeks_tag_code", "one_weeks_tag_code", "week_tag_code", "fin_year", "fin_month", "fin_week"]].astype(str)
        return activated_tag_dw_df

    def get_operate_df(self, cdp_schema, hall_id, domain_id, s_date, e_date):
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

            self.mode = self.QUERY_MODE
            self.statement = f"""
                select 
                    operate.hall_id, operate.domain_id, operate.user_id,
                    Date(ifnull(max(operate.last_login_date), '1990-01-01')) as last_login_date, -- 最後登入時間
                    Date(GREATEST(ifnull(max(operate.last_deposit_date), '1990-01-01'), ifnull(max(operate.last_bet_date) , '1990-01-01'))) as activated_date, -- 最後存款時間 與 最後下注時間 取最大值為 最後實動日
                    ifnull(sum(operate.deposit_count) , 0) as deposit_count, -- 存款次數,
                    ifnull(sum(operate.wagers_total), 0) as wagers_total -- 下注次數
                from (
                    select 
                        hall_id, domain_id, user_id, max(data_date) as last_login_date, '1990-01-01' as last_deposit_date, '1990-01-01' as last_bet_date, 0 as deposit_count, 0 as wagers_total
                    from {cdp_schema}.login_log
                    where hall_id = {hall_id} and domain_id = {domain_id}
                          {condition}
                    group by hall_id, domain_id, user_id -- 最後登入資訊
            
                    Union all
            
                    select 
                        hall_id, domain_id, user_id, '1990-01-01' as last_login_date, max(data_date) as last_deposit_date, '1990-01-01' as last_bet_date, sum(deposit_count) as deposit_count, 0 as wagers_total
                    from {cdp_schema}.deposit_withdraw_record
                    where hall_id = {hall_id} and domain_id = {domain_id}
                          and (deposit_count !=0 or deposit_amount !=0)
                          {condition}
                    group by hall_id, domain_id, user_id -- 存款資訊
                    
                    Union all
            
                    select 
                        hall_id, domain_id, user_id, '1990-01-01' as last_login_date, '1990-01-01' as last_deposit_date, max(data_date) as last_bet_date, 0 as deposit_count, sum(wagers_total) as wagers_total
                    from {cdp_schema}.bet_analysis
                    where hall_id = {hall_id} and domain_id = {domain_id}
                          {condition}
                    group by hall_id, domain_id, user_id -- 下注資訊
                ) operate
                group by operate.hall_id, operate.domain_id, operate.user_id;
            """
            self.exec()
            operate_df = pd.DataFrame(self.fetch_data,columns=["hall_id", "domain_id", "user_id", "last_login_date", "activated_date", "deposit_count", "wagers_total"])
            operate_df[["hall_id", "domain_id", "user_id"]] = operate_df[["hall_id", "domain_id", "user_id"]].astype(str)
            operate_df[["deposit_count", "wagers_total"]] = operate_df[["deposit_count", "wagers_total"]].astype(int)
            operate_df["last_login_date"] = pd.to_datetime(operate_df['last_login_date'])
            operate_df["activated_date"] = pd.to_datetime(operate_df['activated_date'])
            return operate_df
        except Exception as e:
            raise

    def get_fin_df(self, cdp_schema, s_date, e_date):
        try:
            self.mode = self.QUERY_MODE
            self.statement = f"""
                select 
                    distinct fin_year, fin_month, fin_week, fin_week_end
                from {cdp_schema}.financial_date_map
                where Date(date) >= '{s_date}' and Date(date) <= '{e_date}';
            """
            self.exec()
            fin_df = pd.DataFrame(self.fetch_data, columns=["fin_year", "fin_month", "fin_week", "fin_week_end"])
            fin_df[["fin_year", "fin_month", "fin_week"]] = fin_df[["fin_year", "fin_month", "fin_week"]].astype(str)
            fin_df["fin_week_end"] = pd.to_datetime(fin_df['fin_week_end'])
            return fin_df
        except Exception as e:
            raise

