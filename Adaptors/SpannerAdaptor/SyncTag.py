import pandas as pd
from google.cloud import spanner
from sklearn.model_selection import train_test_split

import Environment
from Adaptors.CloudStorageAdaptor import GCS
from Adaptors.SpannerAdaptor import CDP_BBOS
from Adaptors.Hall import Hall
from datetime import timedelta, date
from util.dataframe_process import df_type_format


class SyncTag:
    def __init__(self, spanner_db: CDP_BBOS):
        self.spanner_db = spanner_db
        self.batch_df: pd.DataFrame() = pd.DataFrame()
        self.batch_id: int = 0
        self.tag_name: str = ''
        self.tag_id: int = 0
        self.tag_count: int = 0
        self.icon_id: int = 0
        self.icon_name: str = ''
        self.sync_type: int = 0
        self.ab_test: bool = False
        self.campaign: bool = False
        self.campaign_id: int = 0
        self.operator: int = 0
        self.origin_match_user_df: pd.DataFrame() = pd.DataFrame()
        self.tagged_user_df: pd.DataFrame() = pd.DataFrame()
        self.sync_group_user_df: pd.DataFrame() = pd.DataFrame()
        self.match_user_df: pd.DataFrame() = pd.DataFrame()

    def get_batch(self, hall: Hall):
        try:
            self.spanner_db.table_name = 'user_tag_sync_batch'
            self.spanner_db.statement = (
                "SELECT hall_id, domain_id, "
                "batch_id, tag_name, tag_id, icon_id, icon_name, "
                "file_name, file_path, "
                "sync_type, sync_date_type, sync_date_begin, sync_date_end, "
                "ag_name, user_level_id, user_level_exclude, "
                "activated_date_begin, activated_date_end, "
                "tag_str, tag_str_exclude, "
                "user_step, user_activity, deposit_predict_rate, bet_amount_rank, ab_test, "
                "activity, activity_id, activity_name, activity_start_date, activity_end_date, activity_purpose,activity_description, "
                "operator "
                "FROM user_tag_sync_batch "
                f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
                f"AND tag_enabled IS TRUE "
                f"AND ("
                f"    sync_date_type = 2 "
                f"OR "
                f"    (sync_date_type = 1 AND DATE(CURRENT_TIMESTAMP(), 'America/New_York') BETWEEN sync_date_begin AND sync_date_end) "
                f")"
                # f"AND batch_id = 20 "  # test
            )
            self.spanner_db.mode = self.spanner_db.QUERY_MODE
            self.spanner_db.exec()
        except Exception as e:
            raise
        return self.spanner_db.fetch_data

    def get_match_user(self, hall: Hall, batch):
        try:
            columns = [
                'hall_id', 'domain_id',
                'batch_id', 'tag_name', 'tag_id', 'icon_id', 'icon_name',
                'file_name', 'file_path',
                'sync_type', 'sync_date_type', 'sync_date_begin', 'sync_date_end',
                'ag_name', 'user_level_id', 'user_level_exclude',
                'activated_date_begin', 'activated_date_end',
                'tag_str', 'tag_str_exclude',
                'user_step', 'user_activity', 'deposit_predict_rate', 'bet_amount_rank', 'ab_test',
                'activity', 'activity_id', 'activity_name', 'activity_start_date', 'activity_end_date',
                'activity_purpose', 'activity_description',
                'operator'
            ]
            self.batch_df = pd.DataFrame([batch], columns=columns)
            self.batch_id = self.batch_df['batch_id'].values[0]
            self.tag_name = self.batch_df['tag_name'].values[0]
            self.tag_id = self.batch_df['tag_id'].values[0]
            self.icon_id = self.batch_df['icon_id'].values[0]
            self.icon_name = self.batch_df['icon_name'].values[0]
            self.sync_type = self.batch_df['sync_type'].values[0]  # 同步方式 1:新增(append)|2:刪除後新增
            # sync_date_type = batch_df['sync_date_type'].values[0]  # 同步時間 1:區間|2:永久
            # sync_date_begin = batch_df['sync_date_begin'].values[0]  # 起始有效時間
            # sync_date_end = batch_df['sync_date_end'].values[0]  # 結尾有效時間
            ag_name = self.batch_df['ag_name'].values[0]  # 代理名稱
            user_level_str = self.batch_df['user_level_id'].values[0]  # 會員層級ID
            user_level_exclude_str = self.batch_df['user_level_exclude'].values[0]  # 會員層級ID(排除)
            activated_date_begin = self.batch_df['activated_date_begin'].values[0]  # 起始實動日期
            activated_date_end = self.batch_df['activated_date_end'].values[0]  # 結束實動日期
            tag_str = self.batch_df['tag_str'].values[0]  # 標籤組合
            tag_str_exclude = self.batch_df['tag_str_exclude'].values[0]  # 標籤組合(排除)
            user_step = self.batch_df['user_step'].values[0]  # 會員生命週期
            user_activity = self.batch_df['user_activity'].values[0]  # vip活躍度
            deposit_predict_rate = self.batch_df['deposit_predict_rate'].values[0]  # 預測存款機率
            bet_amount_rank = self.batch_df['bet_amount_rank'].values[0]  # 貨量排名
            self.ab_test = self.batch_df['ab_test'].values[0]  # A/B test
            self.campaign = self.batch_df['activity'].values[0]  # 活動成效分析
            self.campaign_id = self.batch_df['activity_id'].values[0]
            self.operator = self.batch_df['operator'].values[0]  # operator_id

            file_name = self.batch_df['file_name'].values[0]
            file_path = self.batch_df['file_path'].values[0]

            if file_name and file_path:
                storage = GCS('ghr-cdp', f'{Environment.ROOT_PATH}/cert/gcp-ghr-storage.json')
                storage.blob = f'{file_path}{file_name}'
                storage.file = f'{Environment.ROOT_PATH}/files/{file_name}'
                storage.mode = storage.DOWNLOAD_MODE
                storage.exec()
                file_df = pd.read_csv(storage.file)
                username_condition = "','".join(file_df['會員名稱'])

                user_id_df = self.spanner_db.select_data_with(
                    columns=['hall_id', 'domain_id', 'user_id'],
                    statement="SELECT hall_id, domain_id, user_id "
                              "FROM member_info WHERE "
                              f"hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
                              f"AND user_name IN ('{username_condition}') "
                )
                match_user_df = user_id_df
            else:
                ag_name_condition = (
                    f"AND ag_name = '{ag_name}' " if ag_name is not None else "AND 1=1 "
                )
                if user_level_str:
                    level_id_list = user_level_str.split(';')
                    user_level_condition = "AND ("
                    user_level_condition += " OR ".join(f"user_level_id = {level_id}" for level_id in level_id_list)
                    user_level_condition += ") "
                else:
                    user_level_condition = "AND 1=1 "

                if user_level_exclude_str:
                    exclude_level_id_list = user_level_exclude_str.split(',')
                    user_level_exclude_condition = "AND ("
                    user_level_exclude_condition += " AND ".join(f"user_level_id <> {level_id}" for level_id in exclude_level_id_list)
                    user_level_exclude_condition += ") "
                else:
                    user_level_exclude_condition = "AND 1=1 "

                if tag_str:
                    tag_group_list = tag_str.split(';')
                    tag_str_condition = "AND ("
                    tag_condition_list = []
                    for tag_group in tag_group_list:
                        tag_group_condition = "("
                        tag_group_condition += " AND ".join(f"tag_str LIKE '%{x}%'" for x in tag_group.split(','))
                        tag_group_condition += ")"
                        tag_condition_list.append(tag_group_condition)
                    tag_str_condition += " OR ".join(tag_condition_list)
                    tag_str_condition += ") "
                else:
                    tag_str_condition = "AND 1=1 "

                if tag_str_exclude:
                    tag_exclude_group_list = tag_str_exclude.split(';')
                    tag_str_exclude_condition = "AND ("
                    tag_exclude_condition_list = []
                    for tag_exclude_group in tag_exclude_group_list:
                        tag_group_condition = "("
                        tag_group_condition += " AND ".join(f"tag_str NOT LIKE '%{x}%'" for x in tag_exclude_group.split(','))
                        tag_group_condition += ")"
                        tag_exclude_condition_list.append(tag_group_condition)
                    tag_str_exclude_condition += " AND ".join(tag_exclude_condition_list)
                    tag_str_exclude_condition += ") "
                else:
                    tag_str_exclude_condition = "AND 1=1 "
                activated_date_condition = (
                    f"AND last_activated_date BETWEEN '{activated_date_begin}' AND '{activated_date_end}' "
                )
                member_tag_df = self.spanner_db.select_data_with(
                    ['hall_id', 'domain_id', 'user_id'],
                    "SELECT hall_id, domain_id, user_id "
                    "FROM user_tag_dw_data "
                    f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
                    f"{ag_name_condition} "
                    f"{user_level_condition} "
                    f"{user_level_exclude_condition} "
                    f"{activated_date_condition} "
                    f"{tag_str_condition} "
                    f"{tag_str_exclude_condition} "
                )
                match_user_df = member_tag_df
                if user_step:
                    user_step_df = self.spanner_db.select_data_with(
                        ['hall_id', 'domain_id', 'user_id'],
                        "SELECT hall_id, domain_id, user_id "
                        "FROM activated_tag_dw_day "
                        f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
                        f"AND this_day_step = {user_step} "
                        f"AND data_date = '{str(date.today() - timedelta(days=2))}' "
                    )
                    match_user_df = pd.merge(left=match_user_df, right=user_step_df,
                                             on=['hall_id', 'domain_id', 'user_id'],
                                             how='inner')
                if user_activity:
                    user_activity_list = user_activity.split(",")
                    user_activity_df = self.spanner_db.select_data_with(
                        ['hall_id', 'domain_id', 'user_id', 'action_score'],
                        "SELECT hall_id, domain_id, user_id, AVG(action_score) "
                        "FROM user_active_data "
                        f"WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} "
                        f"AND data_date "
                        f"BETWEEN '{str(date.today() - timedelta(days=8))}' AND '{str(date.today() - timedelta(days=2))}' "
                        f"GROUP BY hall_id, domain_id, user_id "
                    )
                    activity_id_df = pd.DataFrame()
                    for level in user_activity_list:
                        if int(level) == 0:
                            activity_id_df = pd.concat(
                                [activity_id_df,
                                 user_activity_df[user_activity_df['action_score'].between(0, 0.17)]],
                                ignore_index=True)
                        if int(level) == 1:
                            activity_id_df = pd.concat(
                                [activity_id_df,
                                 user_activity_df[user_activity_df['action_score'].between(0.17, 0.34)]],
                                ignore_index=True)
                        if int(level) == 2:
                            activity_id_df = pd.concat(
                                [activity_id_df,
                                 (user_activity_df[user_activity_df['action_score'].between(0.34, 0.51)])],
                                ignore_index=True)
                        if int(level) == 3:
                            activity_id_df = pd.concat(
                                [activity_id_df,
                                 (user_activity_df[user_activity_df['action_score'].between(0.51, 0.68)])],
                                ignore_index=True)
                        if int(level) == 4:
                            activity_id_df = pd.concat(
                                [activity_id_df,
                                 (user_activity_df[user_activity_df['action_score'].between(0.68, 0.85)])],
                                ignore_index=True)
                        if int(level) == 5:
                            activity_id_df = pd.concat(
                                [activity_id_df,
                                 (user_activity_df[user_activity_df['action_score'].between(0.85, 1)])],
                                ignore_index=True)
                    match_user_df = pd.merge(left=match_user_df, right=activity_id_df,
                                             on=['hall_id', 'domain_id', 'user_id'],
                                             how='inner')
                    match_user_df = match_user_df[['hall_id', 'domain_id', 'user_id']]
                if deposit_predict_rate:
                    upper_limit = int(deposit_predict_rate.split(';')[1])
                    lower_limit = int(deposit_predict_rate.split(';')[0])
                    deposit_predict_df = self.spanner_db.select_data_with(
                        ['hall_id', 'domain_id', 'user_id'],
                        f"""
                        SELECT hall_id, domain_id, user_id
                        FROM user_recommend_meta_data 
                        WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} 
                        AND recommend_code = 50001 
                        AND data_date BETWEEN '{activated_date_begin}' AND '{activated_date_end}' 
                        GROUP BY hall_id, domain_id, user_id
                        HAVING (AVG(action_score) * 100) BETWEEN {lower_limit} AND {upper_limit}
                        """
                    )
                    match_user_df = pd.merge(left=match_user_df, right=deposit_predict_df,
                                             on=['hall_id', 'domain_id', 'user_id'],
                                             how='inner')
                if bet_amount_rank:
                    bet_amount_rank_df = self.spanner_db.select_data_with(
                        ['hall_id', 'domain_id', 'user_id'],
                        f"""
                        SELECT hall_id, domain_id, user_id 
                        FROM bet_analysis 
                        WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} 
                        AND data_date BETWEEN '{activated_date_begin}' AND '{activated_date_end}' 
                        GROUP BY hall_id, domain_id, user_id 
                        ORDER BY SUM(bet_amount) DESC LIMIT {bet_amount_rank} 
                        """
                    )
                    match_user_df = pd.merge(left=match_user_df, right=bet_amount_rank_df,
                                             on=['hall_id', 'domain_id', 'user_id'],
                                             how='inner')
            self.origin_match_user_df = match_user_df
        except Exception as e:
            raise

    def get_sync_group_user(self, hall: Hall):
        try:
            self.sync_group_user_df = self.spanner_db.select_data_with(
                columns=['hall_id', 'domain_id', 'group_id', 'user_id'],
                statement=
                f"""
                SELECT hall_id, domain_id, group_id, user_id
                FROM user_tag_sync_group  
                WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} 
                AND batch_id = {self.batch_id} 
                """
            )
        except Exception as e:
            raise

    def get_tagged_user(self, hall, api_service):
        try:
            self.tagged_user_df = pd.DataFrame()
            if self.tag_id is None:  # New Tag
                # get first "unused" icon id
                icon_df = api_service.icon_api()
                unused_icon_df = icon_df.loc[icon_df['used'] == 0]
                self.icon_id = unused_icon_df['id'].values[0]
                self.icon_name = unused_icon_df['name'].values[0]

                # add new tag & get tag id
                tag_df = api_service.tag_add_api(icon_id=self.icon_id, tag_name=self.tag_name)
                self.tag_id = tag_df['id'].values[0]
            tagged_user_df = api_service.player_list_api(tag_id=self.tag_id).drop(columns=['user_id'], errors='ignore')
            tagged_user_df.insert(0, 'hall_id', hall.hall_id)
            tagged_user_df.insert(1, 'domain_id', hall.domain_id)
            tagged_user_df = df_type_format(tagged_user_df)
            tagged_user_df = tagged_user_df.rename(columns={'id': 'user_id'})
            if len(tagged_user_df) > 0:
                self.tagged_user_df = tagged_user_df[['hall_id', 'domain_id', 'user_id']]
        except Exception as e:
            raise

    def get_new_tag_user(self, api_service):
        try:
            repeat_user_df = pd.DataFrame()
            delete_user_df = pd.DataFrame()
            if self.ab_test:
                grouped_user_df = df_type_format(self.sync_group_user_df).drop(columns=['group_id'], errors='ignore')
                repeat_user_df = pd.merge(self.origin_match_user_df, grouped_user_df, how='inner',
                                          on=['hall_id', 'domain_id', 'user_id']).drop(columns=['group_id'], errors='ignore')
                delete_user_df = pd.concat([grouped_user_df, repeat_user_df]).drop_duplicates(
                    keep=False)
            else:
                if len(self.tagged_user_df) > 0:
                    repeat_user_df = pd.merge(self.origin_match_user_df, self.tagged_user_df, how='inner', on=['hall_id', 'domain_id', 'user_id'])
                    delete_user_df = pd.concat([self.tagged_user_df, repeat_user_df]).drop_duplicates(
                        keep=False)

            tag_new_user_df = pd.concat([self.origin_match_user_df, repeat_user_df]).drop_duplicates(keep=False)
            if self.sync_type == 2:  # delete tagged user not in this condition
                if len(delete_user_df) > 0:
                    result = api_service.tag_batch_delete_api(
                        user_id=delete_user_df['user_id'].values.tolist(), tag_id=self.tag_id)
                    print(f"tag {self.tag_id}: delete users {result} ")
            self.match_user_df = tag_new_user_df
        except Exception as e:
            raise

    def tag_group(self, hall):
        group_result_df = self.match_user_df
        group_result_df['hall_id'] = hall.hall_id
        group_result_df['hall_name'] = hall.hall_name
        group_result_df['domain_id'] = hall.domain_id
        group_result_df['domain_name'] = hall.domain_name
        group_result_df['batch_id'] = self.batch_id
        group_result_df['tag_id'] = self.tag_id
        group_result_df['tag_name'] = self.tag_name
        group_result_df['updated_time'] = spanner.COMMIT_TIMESTAMP
        group_result_df['group_id'] = 0
        group_result_df['latest_tag'] = True

        if self.ab_test:
            sync_group_count_df = self.spanner_db.select_data_with(
                columns=['hall_id', 'domain_id', 'group_id', 'group_count'],
                statement=
                f"""
                SELECT hall_id, domain_id, group_id, COUNT(1)
                FROM user_tag_sync_group  
                WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} 
                AND batch_id = {self.batch_id} 
                GROUP BY hall_id, domain_id, group_id
                """
            )
            if len(group_result_df) >= 5 or len(sync_group_count_df) == 0:
                a_df, b_df = train_test_split(group_result_df, test_size=0.2)
                a_df['group_id'] = 1  # 80%
                b_df['group_id'] = 2  # 20%
                group_result_df = pd.concat([a_df, b_df])
                self.match_user_df = a_df
            else:
                sync_group_count_df['percent'] = (sync_group_count_df['group_count'] / sync_group_count_df[
                    'group_count'].sum()) * 100
                a_percent = sync_group_count_df['percent'].loc[sync_group_count_df['group_id'] == 1].values[0]
                b_percent = sync_group_count_df['percent'].loc[sync_group_count_df['group_id'] == 2].values[0]
                if a_percent - b_percent >= 60:
                    group_result_df['group_id'] = 2
                    self.match_user_df = pd.DataFrame()
                    self.match_user_df['user_id'] = None
                else:
                    group_result_df['group_id'] = 1
                    self.match_user_df = group_result_df

        if self.sync_type == 2:
            sync_group_user_df = self.sync_group_user_df.drop(columns=['group_id'])
            group_repeat_user_df = pd.merge(self.origin_match_user_df, sync_group_user_df, how='inner',
                                            on=['hall_id', 'domain_id', 'user_id'])
            del_group_user_df = pd.concat([sync_group_user_df, group_repeat_user_df]).drop_duplicates(keep=False)
            del_group_user_df['batch_id'] = self.batch_id
            del_group_user_df['tag_id'] = self.tag_id
            # pk順序需與table一致
            del_group_user_df = del_group_user_df[['hall_id', 'domain_id', 'batch_id', 'tag_id', 'user_id']]
            self.spanner_db.delete_keyset_data_with(
                table_name='user_tag_sync_group', audit_data=del_group_user_df
            )
        self.spanner_db.update_data_dml_with(
            f"UPDATE user_tag_sync_group "
            f"SET latest_tag = False "
            f"WHERE batch_id = {self.batch_id} "
            f"AND tag_id = {self.tag_id} AND latest_tag = True "
        )
        self.spanner_db.upsert_data_with(table_name='user_tag_sync_group', audit_data=group_result_df)

    def tag_campaign(self, hall):
        if self.campaign:
            hall_id = hall.hall_id
            domain_id = hall.domain_id
            campaign_name = self.batch_df['activity_name'].values[0]
            campaign_start_date = self.batch_df['activity_start_date'].values[0]
            campaign_end_date = self.batch_df['activity_end_date'].values[0]
            campaign_purpose = self.batch_df['activity_purpose'].values[0]
            campaign_description = self.batch_df['activity_description'].values[0]
            create_time = spanner.COMMIT_TIMESTAMP
            uploader_id = self.operator
            if self.campaign_id is None:
                campaign_id_df = self.spanner_db.select_data_with(
                    columns=['campaign_id'],
                    statement="SELECT MAX(activity_id) + 1,  FROM activity_analysis_data "
                )
                self.campaign_id = campaign_id_df['campaign_id'][0]
                campaign_df = pd.DataFrame(
                    data=[(hall_id, domain_id,
                           self.campaign_id, campaign_name, campaign_start_date, campaign_end_date,
                           campaign_purpose, campaign_description, 2, '成功', uploader_id, create_time)],
                    columns=['hall_id', 'domain_id',
                             'activity_id', 'activity_name', 'activity_start_date', 'activity_end_date',
                             'activity_purpose', 'activity_description', 'status', 'status_description',
                             'uploader_id', 'create_time'])
                self.spanner_db.insert_data_with(table_name='activity_analysis_data', audit_data=campaign_df)
            else:
                campaign_df = pd.DataFrame(
                    data=[(hall_id, domain_id,
                           self.campaign_id, campaign_name, campaign_start_date, campaign_end_date,
                           campaign_purpose, campaign_description, uploader_id, create_time)],
                    columns=['hall_id', 'domain_id',
                             'activity_id', 'activity_name', 'activity_start_date', 'activity_end_date',
                             'activity_purpose', 'activity_description',
                             'uploader_id', 'create_time'])
                self.spanner_db.upsert_data_with(table_name='activity_analysis_data', audit_data=campaign_df)
            if self.sync_type == 2:
                group_repeat_user_df = pd.merge(self.origin_match_user_df, self.sync_group_user_df, how='inner',
                                                on=['hall_id', 'domain_id', 'user_id'])
                del_group_user_df = pd.concat([self.sync_group_user_df, group_repeat_user_df]).drop_duplicates(
                    keep=False)
                del_group_user_df['activity_id'] = self.campaign_id
                if len(del_group_user_df) > 0:
                    del_user_id_condition = ",".join(str(x) for x in del_group_user_df['user_id'].values.tolist())

                    del_user_df = self.spanner_db.select_data_with(
                        columns=['activity_id', 'hall_id', 'domain_id', 'user_name'],
                        statement=
                        (f"""
                        SELECT {self.campaign_id}, hall_id, domain_id, user_name
                        FROM member_info  
                        WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} 
                        AND user_id IN ({del_user_id_condition})
                        """)
                    )
                    # pk順序需與table一致
                    del_user_df = del_user_df[['activity_id', 'hall_id', 'domain_id', 'user_name']]
                    self.spanner_db.delete_keyset_data_with(
                        table_name='activity_member_data', audit_data=del_user_df
                    )
            if len(self.match_user_df) > 0:
                # update campaign user table
                match_user_id_condition = ",".join(str(x) for x in self.match_user_df['user_id'].values.tolist())

                campaign_user_df = self.spanner_db.select_data_with(
                    columns=['activity_id', 'hall_id', 'domain_id', 'user_name'],
                    statement=
                    (f"""
                    SELECT {self.campaign_id}, hall_id, domain_id, user_name
                    FROM member_info  
                    WHERE hall_id = {hall.hall_id} AND domain_id = {hall.domain_id} 
                    AND user_id IN ({match_user_id_condition})
                    """)
                )
                self.spanner_db.upsert_data_with(table_name='activity_member_data', audit_data=campaign_user_df)

    def batch_tag_user(self, api_service):
        # batch tag users
        match_user_list = self.match_user_df['user_id'].values.tolist()
        tag_batch_df = api_service.tag_batch_api(user_id=match_user_list, tag_id=self.tag_id)
        print(f"Tagged user count: {len(tag_batch_df)}")

    def tag_user_count(self, api_service):
        # get tag total users
        tag_count_df = api_service.tag_user_count_api()
        self.tag_count = int(tag_count_df.loc[tag_count_df['id'] == self.tag_id]['user_count'].values[0])
