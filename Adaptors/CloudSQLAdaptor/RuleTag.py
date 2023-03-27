import os
from os import path

import pandas as pd
import numpy as np

import Environment
from Adaptors.CloudSQLAdaptor import BaseCDP
from Adaptors.CloudSQLAdaptor import BaseBBOS
from Adaptors.CloudStorageAdaptor import GCS
from datetime import timedelta, date
from util.dataframe_process import df_type_format


class RuleTag(BaseCDP):
    def __init__(self, db_config: str):
        super(RuleTag, self).__init__(db_config)
        self.tag_type = 3
        self.target_bet_df: pd.DataFrame() = pd.DataFrame()
        self.data_date = date.today() - timedelta(days=1)  # yesterday
        self.ods_data_columns = ['hall_id', 'domain_id', 'user_id', 'user_name',
                                 'tag_code', 'tag_type', 'tag_enabled', 'register_date']

    def get_init_activated(self, ssr: BaseBBOS):  # 首次取得15日內實動資料
        if path.exists(f'{Environment.ROOT_PATH}/files/{ssr.domain_name}_activated_bet_data_15days.csv'):
            print('activated file exist, ignore initial')
            return
        try:
            self.statement = f'SELECT bet.hall_id, bet.domain_id, bet.user_id, member.user_name, member.register_date, ' \
                             f'lobby, game_type, game_kind, ' \
                             f'SUM(wagers_total), SUM(bet_amount), SUM(commissionable), SUM(payoff), data_date ' \
                             f'FROM {ssr.cdp_schema}.bet_analysis bet ' \
                             f'JOIN {ssr.cdp_schema}.member_info member ' \
                             f'    ON member.hall_id = bet.hall_id ' \
                             f'    AND member.domain_id = bet.domain_id ' \
                             f'    AND member.user_id = bet.user_id ' \
                             f'WHERE bet.hall_id = {ssr.hall_id} AND bet.domain_id = {ssr.domain_id} ' \
                             f'AND member.user_name IS NOT NULL ' \
                             f'AND commissionable > 0 ' \
                             f'GROUP BY bet.hall_id, bet.domain_id, bet.user_id, member.user_name, member.register_date, ' \
                             f'lobby, game_type, game_kind, data_date '
            self.mode = self.QUERY_MODE
            self.exec()
            sync_columns = ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'lobby', 'game_type',
                            'game_kind', 'wagers_total', 'bet_amount', 'commissionable', 'payoff', 'data_date']

            bet_df = pd.DataFrame(self.fetch_data, columns=sync_columns)

            bet_df = df_type_format(bet_df)

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
            result_df.to_csv(f'{Environment.ROOT_PATH}/files/{ssr.domain_name}_activated_bet_data_15days.csv', index=False)

        except Exception as e:
            raise

    def get_latest_activated(self, ssr: BaseBBOS):  # 將實動檔案concat最新一日下注資料，重新取得15天實動檔案
        try:
            activated_df = pd.read_csv(f'{Environment.ROOT_PATH}/files/{ssr.domain_name}_activated_bet_data_15days.csv')
            activated_df = df_type_format(activated_df)
            self.statement = f'SELECT bet.hall_id, bet.domain_id, bet.user_id, member.user_name, member.register_date, lobby, game_type, game_kind, ' \
                             f'SUM(wagers_total), SUM(bet_amount), SUM(commissionable), SUM(payoff), data_date ' \
                             f'FROM {ssr.cdp_schema}.bet_analysis bet ' \
                             f'JOIN {ssr.cdp_schema}.member_info member ' \
                             f'    ON member.hall_id = bet.hall_id ' \
                             f'    AND member.domain_id = bet.domain_id ' \
                             f'    AND member.user_id = bet.user_id ' \
                             f'WHERE bet.hall_id = {ssr.hall_id} AND bet.domain_id = {ssr.domain_id} ' \
                             f'AND member.user_name IS NOT NULL ' \
                             f'AND commissionable > 0 ' \
                             f'AND data_date = \'{self.data_date}\' ' \
                             f'GROUP BY bet.hall_id, bet.domain_id, bet.user_id, member.user_name, member.register_date, lobby, game_type, game_kind, data_date'
            self.mode = self.QUERY_MODE
            self.exec()
            sync_columns = ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'lobby', 'game_type',
                            'game_kind', 'wagers_total', 'bet_amount', 'commissionable', 'payoff', 'data_date']

            bet_df = pd.DataFrame(self.fetch_data, columns=sync_columns)
            bet_df = df_type_format(bet_df)

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
            activate_bet_df.to_csv(f'{Environment.ROOT_PATH}/files/{ssr.domain_name}_activated_bet_data_15days.csv', index=False)

            # 計算每位使用者 實動天數 至少超過3天(包含3天)
            data_df = activate_bet_df[['hall_id', 'domain_id', 'user_id', 'data_date']].drop_duplicates()
            filter_bet_df = \
                data_df[data_df.groupby(by=['hall_id', 'domain_id', 'user_id'])['hall_id'].transform('size') >= 3][
                    ['hall_id', 'domain_id', 'user_id']].drop_duplicates()
            self.target_bet_df = activate_bet_df[activate_bet_df['user_id'].isin(filter_bet_df['user_id'])]

        except Exception as e:
            raise

    def upload_tags_to_gcp(self, ssr: BaseBBOS, start, end):
        # ods_tag規則標轉入gcp bucket中
        file_date = self.data_date + timedelta(days=1)
        file_fp = f'{file_date}_bbos_user_rule_tag_ods.csv'
        tag_rules_get_history_statement = \
            f'SELECT hall_id, domain_id, user_id, user_name, tag_code, tag_type, ' \
            f'CAST(tag_enabled AS unsigned) AS tag_enabled, register_date ' \
            f'FROM {ssr.cdp_schema}.user_tag_ods_data ' \
            f'WHERE ' \
            f'hall_id = {ssr.hall_id} AND domain_id = {ssr.domain_id} ' \
            f'AND (tag_type = {self.tag_type} OR tag_code IN (40006, 40007)) ' \
            f'AND updated_time >= \'{start}\' ' \
            f'AND updated_time <= \'{end}\' '
        self.statement = tag_rules_get_history_statement
        self.mode = self.QUERY_MODE  # 0
        self.exec()

        gcs = GCS('ghr-cdp', f'{Environment.ROOT_PATH}/cert/gcp-20190903-01-e0d59c0e3be0.json')

        df = pd.DataFrame(self.fetch_data, columns=self.ods_data_columns).reset_index(drop=True)
        df = df.loc[df['hall_id'] == ssr.hall_id]
        df = df.loc[df['domain_id'] == ssr.domain_id]
        df.to_csv(file_fp, index=False)

        gcs.blob = f'upload/user_tag_ods_rules_by_hall/{ssr.domain_name}/{file_fp}'
        gcs.file = file_fp

        gcs.mode = gcs.UPLOAD_MODE
        gcs.exec()

        os.remove(file_fp)

    def final_sp_exec(self, ssr: BaseBBOS):
        self.mode = self.QUERY_MODE
        self.statement = (
            f'SELECT DATE_SUB(MAX(updated_time),INTERVAL 1 DAY) '
            f'FROM {ssr.cdp_schema}.user_tag_ods_data '
            f'WHERE hall_id = {ssr.hall_id} AND domain_id = {ssr.domain_id} '
        )
        self.exec()
        max_updated_date = self.fetch_data[0][0].strftime('%Y-%m-%d')

        self.statement = (
            f'REPLACE INTO {ssr.cdp_schema}.user_tag_dw_data '
            f'(hall_id, domain_id, user_id, user_name, ag_name, tag_str, user_level_id, user_level, register_date) '
            f'SELECT '
            '    ods.hall_id, ' 
            '    ods.domain_id, '
            '    ods.user_id, '
            '    member.user_name, '
            '    member.ag_name, '
            '    ods.tags ,'
            '    member.user_level_id,'
            '    member.user_level,'
            '    member.register_date '
            'FROM ( '
            '	 SELECT '
            '        ods.hall_id, '
            '        ods.domain_id,	'
            '        ods.user_id, '
            '        IFNULL(GROUP_CONCAT(DISTINCT (CASE WHEN (tag_enabled = 1) THEN tag_code END) '
            '        ORDER BY tag_code ASC SEPARATOR \',\'), \'\') AS tags '
            f'   FROM {ssr.cdp_schema}.user_tag_ods_data ods '
            f'   WHERE ods.user_id IN ( '
            f'       SELECT user_id '
            f'       FROM {ssr.cdp_schema}.user_tag_ods_data '
            f'       WHERE hall_id = {ssr.hall_id} AND domain_id = {ssr.domain_id} '
            f'       AND DATE(updated_time) >= \'{max_updated_date}\' '
            f'   ) '
            f'   AND hall_id = {ssr.hall_id} AND domain_id = {ssr.domain_id} '
            f'   GROUP BY ods.hall_id , ods.domain_id, ods.user_id '
            f') ods '
            f'JOIN {ssr.cdp_schema}.member_info member ON ods.hall_id = member.hall_id '
            f'AND ods.domain_id = member.domain_id '
            f'AND ods.user_id = member.user_id '
        )
        self.exec()

        # 時間區間內各標籤每日實動人數
        self.statement = (
            f'REPLACE INTO {ssr.cdp_schema}.activated_tag_day_count (hall_id, domain_id, data_date, tag_type, tag_code, total_count) '
            f'SELECT bet_data.hall_id, bet_data.domain_id, bet_data.data_date, t_ods.tag_type, t_ods.tag_code, count(*) '
            f'FROM ( '
            f'    SELECT hall_id, domain_id, user_id, data_date, count(*) '
            f'    FROM {ssr.cdp_schema}.bet_analysis '
            f'    WHERE hall_id = {ssr.hall_id} AND domain_id = {ssr.domain_id} '
            f'    GROUP BY hall_id, domain_id, user_id, data_date '
            f') AS bet_data '
            f'JOIN ( '
            f'    SELECT hall_id, domain_id, user_id, tag_type, tag_code '
            f'    FROM {ssr.cdp_schema}.user_tag_ods_data '
            f'    WHERE tag_enabled = 1 '
            f'    AND hall_id = {ssr.hall_id} AND domain_id = {ssr.domain_id} '
            f') AS t_ods '
            f'ON bet_data.hall_id = t_ods.hall_id '
            f'AND bet_data.domain_id = t_ods.domain_id '
            f'AND bet_data.user_id = t_ods.user_id '
            f'GROUP BY bet_data.hall_id, bet_data.domain_id, bet_data.data_date, t_ods.tag_type, t_ods.tag_code '
        )
        self.exec()

        # 時間區間內各標籤每月實動人數
        self.statement = (
            f'REPLACE INTO {ssr.cdp_schema}.activated_tag_month_count '
            f'(hall_id, domain_id, data_month, tag_type, tag_code, total_count) '
            f'SELECT bet_data.hall_id, bet_data.domain_id, bet_data.data_month, t_ods.tag_type, t_ods.tag_code, count(*) '
            f'FROM ( '
            f'    SELECT hall_id, domain_id, user_id, CONCAT(LEFT(CAST(data_date AS CHAR), 8), \'01\') AS data_month '
            f'    FROM {ssr.cdp_schema}.bet_analysis '
            f'    WHERE hall_id = {ssr.hall_id} AND domain_id = {ssr.domain_id} '
            f'    GROUP BY hall_id, domain_id, user_id, CONCAT(LEFT(CAST(data_date AS CHAR), 8), \'01\') '
            f') AS bet_data '
            f'JOIN ( '
            f'    SELECT hall_id, domain_id, user_id, tag_type, tag_code '
            f'    FROM {ssr.cdp_schema}.user_tag_ods_data '
            f'    WHERE tag_enabled = 1 '
            f'    AND hall_id = {ssr.hall_id} AND domain_id = {ssr.domain_id} '
            f') AS t_ods '
            f'ON bet_data.hall_id = t_ods.hall_id '
            f'AND bet_data.domain_id = t_ods.domain_id '
            f'AND bet_data.user_id = t_ods.user_id '
            f'GROUP BY bet_data.hall_id, bet_data.domain_id, bet_data.data_month, t_ods.tag_type, t_ods.tag_code '
        )
        self.exec()

    def tag_reset(self, ssr: BaseBBOS, tag_code: int):  # 重置tag_enabled為0
        try:
            self.statement = f'UPDATE {ssr.cdp_schema}.user_tag_ods_data ' \
                             f'SET tag_enabled = 0 ' \
                             f'WHERE hall_id = {ssr.hall_id} ' \
                             f'AND domain_id = {ssr.domain_id} ' \
                             f'AND tag_code = {tag_code} '
            self.mode = self.QUERY_MODE
            self.exec()

        except Exception as e:
            raise

    def tag_churned(self, ssr: BaseBBOS, tag_code: int) -> pd.DataFrame:  # 流失客
        try:
            self.statement = (f'SELECT member.hall_id, member.domain_id, login_log.user_id AS uid, member.user_name, '
                              f'{tag_code} AS tag_code, '
                              f'{self.tag_type} AS tag_type, '
                              f'CASE WHEN TIMESTAMPDIFF(DAY, MAX(login_log.data_date), \'{self.data_date}\' ) >= 30 '
                              f'    THEN 1 '
                              f'    ELSE 0 END AS tag_enabled, '
                              f'member.register_date '
                              f'FROM {ssr.cdp_schema}.login_log login_log '
                              f'INNER JOIN {ssr.cdp_schema}.member_info member '
                              f'    ON login_log.user_id = member.user_id '
                              f'    AND login_log.domain_id = member.domain_id '
                              f'    AND login_log.hall_id = member.hall_id '
                              f'WHERE member.hall_id = {ssr.hall_id} '
                              f'    AND member.domain_id = {ssr.domain_id} '
                              f'    AND login_log.data_date >= date_add(\'{self.data_date}\', INTERVAL -31 DAY) '
                              f'    AND login_log.data_date <= \'{self.data_date}\' AND login_log.user_id > 0 '
                              f'GROUP BY member.hall_id, member.domain_id, login_log.user_id, '
                              f'    member.user_name, member.register_date '
                              )
            self.mode = self.QUERY_MODE
            self.exec()
            churned_df = pd.DataFrame(self.fetch_data, columns=self.ods_data_columns)

        except Exception as e:
            raise

        return churned_df

    def tag_boss(self, ssr: BaseBBOS, tag_code: int) -> pd.DataFrame:  # 好客
        try:
            self.statement = f'SELECT hall_id, domain_id, lobby, game_type, is_pvp, is_pve ' \
                             f'FROM {ssr.cdp_schema}.game_type_dict ' \
                             f'WHERE is_pvp = \'Y\' OR is_pve = \'Y\' '
            self.mode = self.QUERY_MODE
            self.exec()
            sync_columns = ['hall_id', 'domain_id', 'lobby', 'game_type', 'is_pvp', 'is_pve']
            game_df = pd.DataFrame(self.fetch_data, columns=sync_columns)

            merged_df = pd.merge(left=self.target_bet_df, right=game_df,
                                 on=['hall_id', 'domain_id', 'lobby', 'game_type'],
                                 how='left')
            pvp_df = merged_df[merged_df['is_pvp'] == 'Y'].groupby(['hall_id', 'domain_id', 'user_id'])[
                'commissionable'].sum().reset_index().rename(columns={'commissionable': 'pvp_commissionable'})
            pve_df = merged_df[merged_df['is_pve'] == 'Y'].groupby(['hall_id', 'domain_id', 'user_id'])[
                'commissionable'].sum().reset_index().rename(columns={'commissionable': 'pve_commissionable'})
            sum_df = merged_df.groupby(['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date'])[
                'commissionable'].sum().reset_index()

            self.statement = f'SELECT ' \
                             f'hall_id, domain_id, user_id, ' \
                             f'SUM(deposit_count) AS deposit_count, ' \
                             f'SUM(deposit_amount) AS deposit_amount, ' \
                             f'SUM(deposit_amount) - SUM(withdraw_amount) AS deposit_withdraw ' \
                             f'FROM {ssr.cdp_schema}.deposit_withdraw_record ' \
                             f'WHERE hall_id = {ssr.hall_id} AND domain_id = {ssr.domain_id} ' \
                             f'AND data_date <= \'{self.data_date}\' ' \
                             f'GROUP BY hall_id , domain_id , user_id'
            sync_columns = ['hall_id', 'domain_id', 'user_id', 'deposit_count', 'deposit_amount', 'deposit_withdraw']
            self.exec()
            deposit_df = pd.DataFrame(self.fetch_data, columns=sync_columns)

            good_boss_df = pd.merge(left=sum_df, right=pvp_df, on=['hall_id', 'domain_id', 'user_id'], how='left')
            good_boss_df = pd.merge(left=good_boss_df, right=pve_df, on=['hall_id', 'domain_id', 'user_id'], how='left')
            good_boss_df = pd.merge(left=good_boss_df, right=deposit_df, on=['hall_id', 'domain_id', 'user_id'],
                                    how='left')
            good_boss_df.fillna(0, inplace=True)
            good_boss_df['tag_code'] = tag_code
            good_boss_df['tag_type'] = self.tag_type
            good_boss_df['tag_enabled'] = 0

            good_boss_df['tag_enabled'] = np.where(
                ((good_boss_df['pvp_commissionable'] / good_boss_df['commissionable']) < 0.5)
                & ((good_boss_df['pve_commissionable'] / good_boss_df['commissionable']) < 0.5)
                & (good_boss_df['deposit_count'] >= 2)
                & (good_boss_df['deposit_amount'] >= ssr.boss_threshold)
                & (good_boss_df['deposit_withdraw'] > 0)
                , 1, 0)
            good_boss_df = good_boss_df[self.ods_data_columns].loc[good_boss_df['tag_enabled'] == 1]

        except Exception as e:
            raise

        self.tag_reset(ssr, tag_code)
        return good_boss_df

    def tag_deposit(self, ssr: BaseBBOS, tag_code: int, deposit_min: int) -> pd.DataFrame:  # 再存客
        try:
            self.statement = f'SELECT depo.hall_id, depo.domain_id, depo.user_id, member.user_name, ' \
                             f'{tag_code} AS tag_code, ' \
                             f'{self.tag_type} AS tag_type, ' \
                             f'1 AS tag_enabled, member.register_date ' \
                             f'FROM {ssr.cdp_schema}.deposit_withdraw_record depo ' \
                             f'JOIN {ssr.cdp_schema}.member_info member ON depo.hall_id = member.hall_id ' \
                             f'AND depo.domain_id = member.domain_id ' \
                             f'AND depo.user_id = member.user_id ' \
                             f'AND member.user_id > 0 ' \
                             f'WHERE depo.hall_id = {ssr.hall_id} AND depo.domain_id = {ssr.domain_id} AND ' \
                             f'depo.data_date <= \'{self.data_date}\' ' \
                             f'GROUP BY depo.hall_id, depo.domain_id, depo.user_id ' \
                             f'HAVING SUM(depo.deposit_count) >= {deposit_min}'

            self.mode = self.QUERY_MODE
            self.exec()
            deposit_df = pd.DataFrame(self.fetch_data, columns=self.ods_data_columns)

        except Exception as e:
            raise

        return deposit_df

    def tag_huge_winner(self, ssr: BaseBBOS, tag_code: int) -> pd.DataFrame:  # 最近大贏
        try:
            huge_winner_df = self.target_bet_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']).sum().reset_index()
            huge_winner_df['tag_code'] = tag_code
            huge_winner_df['tag_type'] = self.tag_type
            huge_winner_df['tag_enabled'] = 0
            huge_winner_df['tag_enabled'] = np.where(
                (huge_winner_df['payoff'] > huge_winner_df['commissionable'] * 0.5)
                , 1, 0)
            huge_winner_df = huge_winner_df[self.ods_data_columns].loc[huge_winner_df['tag_enabled'] == 1]

        except Exception as e:
            raise

        self.tag_reset(ssr, tag_code)
        return huge_winner_df

    def tag_huge_loser(self, ssr: BaseBBOS, tag_code: int) -> pd.DataFrame:  # 最近大輸
        try:
            huge_loser_df = self.target_bet_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']).sum().reset_index()
            huge_loser_df['tag_code'] = tag_code
            huge_loser_df['tag_type'] = self.tag_type
            huge_loser_df['tag_enabled'] = 0
            huge_loser_df['tag_enabled'] = np.where(
                (huge_loser_df['payoff'] < 0) & (huge_loser_df['payoff'] < (huge_loser_df['commissionable'] * -0.5))
                , 1, 0)
            huge_loser_df = huge_loser_df[self.ods_data_columns].loc[huge_loser_df['tag_enabled'] == 1]

        except Exception as e:
            raise

        self.tag_reset(ssr, tag_code)
        return huge_loser_df

    def tag_frequent_loser(self, ssr: BaseBBOS, tag_code: int) -> pd.DataFrame:  # 最近常輸
        try:
            win_lose_count_df = self.target_bet_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'data_date']).sum().reset_index()
            win_lose_count_df['win_count'] = 0
            win_lose_count_df['win_count'].loc[win_lose_count_df['payoff'] >= 0] = 1
            win_lose_count_df['lose_count'] = 0
            win_lose_count_df['lose_count'].loc[win_lose_count_df['payoff'] < 0] = 1
            win_lose_count_df = win_lose_count_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']).sum().reset_index()

            win_lose_count_df['tag_code'] = tag_code
            win_lose_count_df['tag_type'] = self.tag_type
            win_lose_count_df['tag_enabled'] = 0
            win_lose_count_df['tag_enabled'] = np.where(
                (win_lose_count_df['win_count'] < win_lose_count_df['lose_count'])
                & (win_lose_count_df['payoff'] < (win_lose_count_df['commissionable'] * -0.05))
                , 1, 0)
            frequent_loser_df = win_lose_count_df[self.ods_data_columns].loc[win_lose_count_df['tag_enabled'] == 1]

        except Exception as e:
            raise

        self.tag_reset(ssr, tag_code)
        return frequent_loser_df

    def tag_game_lover(self, ssr: BaseBBOS, tag_code_dict: dict) -> pd.DataFrame:  # 各種遊戲客
        try:
            game_kind_df = self.target_bet_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'game_kind']).sum().reset_index()
            top_game_kind_df = game_kind_df.sort_values('commissionable').groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date']).tail(1)
            top_game_kind_df['tag_code'] = 0
            top_game_kind_df['tag_type'] = self.tag_type
            top_game_kind_df['tag_enabled'] = 1
            for game_kind, tag_code in tag_code_dict.items():
                top_game_kind_df['tag_code'].loc[top_game_kind_df['game_kind'] == game_kind] = tag_code
            top_game_kind_df['tag_type'] = self.tag_type
            game_lover_df = top_game_kind_df[self.ods_data_columns]

        except Exception as e:
            raise

        for game_kind, tag_code in tag_code_dict.items():
            self.tag_reset(ssr, tag_code)
        return game_lover_df

    def tag_win_lose_reaction(self, ssr: BaseBBOS, tag_code_list: list) -> pd.DataFrame:  # 輸贏後衝或跑
        try:
            user_daily_df = self.target_bet_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'data_date']).sum().reset_index()
            user_daily_df['commissionable_diff'] = user_daily_df.groupby(['hall_id', 'domain_id', 'user_id'])[
                'commissionable'].diff()
            user_daily_df['payoff_diff'] = user_daily_df.groupby(['hall_id', 'domain_id', 'user_id'])[
                'payoff'].diff()

            tag_conditions = [
                # 贏了就衝
                ((user_daily_df['payoff'] - user_daily_df['payoff_diff']) > 0) & (
                        user_daily_df['commissionable_diff'] > 0),
                # 贏了就跑
                ((user_daily_df['payoff'] - user_daily_df['payoff_diff']) > 0) & (
                        user_daily_df['commissionable_diff'] < 0),
                # 輸了就衝
                ((user_daily_df['payoff'] - user_daily_df['payoff_diff']) < 0) & (
                        user_daily_df['commissionable_diff'] > 0),
                # 輸了就跑
                ((user_daily_df['payoff'] - user_daily_df['payoff_diff']) < 0) & (
                        user_daily_df['commissionable_diff'] < 0)
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
            tag_result_df['tag_type'] = self.tag_type
            tag_result_df['tag_enabled'] = 1

            win_lose_df = tag_result_df[self.ods_data_columns]
        except Exception as e:
            raise

        for tag_code in tag_code_list:
            self.tag_reset(ssr, tag_code)
        return win_lose_df

    def tag_app_user(self, ssr: BaseBBOS, tag_code: int) -> pd.DataFrame:  # app會員
        try:
            self.statement = f'SELECT login.hall_id, login.domain_id, login.user_id, member.user_name, ' \
                             f'{tag_code} AS tag_code, ' \
                             f'{self.tag_type} AS tag_type, ' \
                             f'1 AS tag_enabled, ' \
                             f'member.register_date ' \
                             f'FROM {ssr.cdp_schema}.login_log login ' \
                             f'JOIN {ssr.cdp_schema}.member_info member ON login.hall_id = member.hall_id ' \
                             f'AND login.domain_id = member.domain_id ' \
                             f'AND login.user_id = member.user_id ' \
                             f'AND member.user_id > 0 ' \
                             f'WHERE login.hall_id = {ssr.hall_id} AND login.domain_id = {ssr.domain_id} AND ' \
                             f'login.host = \'App\' AND data_date = \'{self.data_date}\' '
            self.mode = self.QUERY_MODE
            self.exec()
            app_user_df = pd.DataFrame(self.fetch_data, columns=self.ods_data_columns)

        except Exception as e:
            raise

        return app_user_df

    def tag_arbitrage_user(self, ssr: BaseBBOS, tag_code_list: list):  # 對打套利客
        tag_code_likely = tag_code_list[0]
        tag_code_certain = tag_code_list[1]
        tag_type = 4
        self.begin_transaction()
        arbitrage_user_statement = \
            f'REPLACE INTO {ssr.cdp_schema}.user_tag_ods_data (hall_id, domain_id, user_id, user_name, tag_code, tag_type, tag_enabled, register_date) ' \
            f'SELECT ' \
            f'    hall_id, domain_id, user_id, user_name, tag_code, tag_type, tag_enabled, register_date ' \
            f'FROM ' \
            f'( ' \
            f'SELECT  ' \
            f'      member.hall_id, ' \
            f'		member.domain_id, ' \
            f'		member.user_id, ' \
            f'		member.user_name, ' \
            f'		CASE ' \
            f'			WHEN hedge_round_count < 50 THEN {tag_code_likely} ' \
            f'			ELSE {tag_code_certain} ' \
            f'		END AS tag_code, ' \
            f'		{tag_type} AS tag_type, ' \
            f'		1 AS tag_enabled, ' \
            f'		member.register_date ' \
            f'    FROM ' \
            f'        {ssr.cdp_schema}.arbitrage_info arbitrage ' \
            f'    JOIN {ssr.cdp_schema}.member_info member ON arbitrage.user_id = member.user_id ' \
            f'        AND arbitrage.domain_id = member.domain_id ' \
            f'        AND arbitrage.hall_id = member.hall_id ' \
            f'    WHERE arbitrage.hall_id = {ssr.hall_id} AND arbitrage.domain_id = {ssr.domain_id} AND ' \
            f'        external_game_code = {19} ' \
            f'            AND data_date >= \'{self.data_date}\' ' \
            f'            AND data_date < date_add(\'{self.data_date}\', INTERVAL 1 DAY) ' \
            f') result ' \
            f'GROUP BY hall_id , domain_id , user_id , user_name , tag_code , tag_type , register_date'
        self.statement = arbitrage_user_statement
        self.mode = self.QUERY_MODE
        self.exec()

        arbitrage_user_statement = \
            f'UPDATE {ssr.cdp_schema}.user_tag_ods_data t1 ' \
            f'JOIN {ssr.cdp_schema}.user_tag_ods_data t2 ' \
            f'ON t1.user_id = t2.user_id AND t1.domain_id = t2.domain_id AND t1.hall_id = t2.hall_id ' \
            f'SET t1.tag_enabled = 0 ' \
            f'WHERE t1.hall_id = {ssr.hall_id} AND t1.domain_id = {ssr.domain_id} AND ' \
            f't1.tag_code = {tag_code_likely} AND t2.tag_code = {tag_code_certain} ' \
            f'AND t1.tag_enabled = 1 AND t2.tag_enabled = 1'
        self.statement = arbitrage_user_statement
        self.exec()
        self.end_transaction()

    def tag_bet_time(self, ssr: BaseBBOS, source_df: pd.DataFrame(), weekday_code_dict: dict,
                     time_code_dict: dict) -> pd.DataFrame():  # 下注週期標
        try:
            # 週幾標
            bet_weekday_df = source_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'data_weekday']).sum().reset_index()
            top_weekday_df = bet_weekday_df.sort_values(['commissionable', 'data_weekday'],
                                                        ascending=[True, True]).groupby(
                ['hall_id', 'domain_id', 'user_id']).tail(1)

            top_weekday_df['tag_code'] = 0
            for weekday, tag_code in weekday_code_dict.items():
                top_weekday_df['tag_code'].loc[top_weekday_df['data_weekday'] == weekday] = tag_code
            top_weekday_df['tag_type'] = self.tag_type
            top_weekday_df['tag_enabled'] = 1
            final_weekday_df = top_weekday_df[self.ods_data_columns]
            for weekday, tag_code in weekday_code_dict.items():
                self.tag_reset(ssr, tag_code)

            # 小時標
            bet_hour_df = source_df.groupby(
                ['hall_id', 'domain_id', 'user_id', 'user_name', 'register_date', 'bet_hour']).sum().reset_index()
            top_bet_hour_df = bet_hour_df.sort_values(['commissionable', 'bet_hour'], ascending=[True, True]).groupby(
                ['hall_id', 'domain_id', 'user_id']).tail(1)

            top_bet_hour_df['tag_code'] = 0
            for bet_hour, tag_code in time_code_dict.items():
                top_bet_hour_df['tag_code'].loc[top_bet_hour_df['bet_hour'] == bet_hour] = tag_code
            top_bet_hour_df['tag_type'] = self.tag_type
            top_bet_hour_df['tag_enabled'] = 1
            final_bet_hour_df = top_bet_hour_df[self.ods_data_columns]
            for bet_hour, tag_code in time_code_dict.items():
                self.tag_reset(ssr, tag_code)

            result_df = pd.concat([final_bet_hour_df, final_weekday_df], ignore_index=True)

        except Exception as e:
            raise

        return result_df
