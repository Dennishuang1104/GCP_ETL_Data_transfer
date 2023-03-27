import pandas as pd
import pytz
from decimal import Decimal

def df_type_format(source_df: pd.DataFrame()) -> pd.DataFrame():
    for df_column in source_df.columns:
        if df_column in ['hall_id', 'domain_id', 'user_id', 'lobby', 'game_kind', 'wagers_total', 'id', 'promotion_id']:
            source_df[df_column] = source_df[df_column].astype(int)
        if df_column in ['user_name', 'game_type']:
            source_df[df_column] = source_df[df_column].astype(str)
        if df_column in ['bet_amount', 'commissionable', 'payoff', 'amount']:
            source_df[df_column] = source_df[df_column].astype(float)
        if df_column == 'data_date':
            source_df[df_column] = pd.to_datetime(source_df[df_column], format='%Y-%m-%d')
        if df_column == 'register_date':
            source_df[df_column] = pd.to_datetime(source_df[df_column], format='%Y-%m-%d %H:%M:%S')

    return source_df


def df_type_format_tag(source_df: pd.DataFrame()) -> pd.DataFrame():
    for df_column in source_df.columns:
        if df_column in ['hall_id', 'domain_id', 'user_id', 'lobby', 'lobby_group',
                         'game_kind', 'wagers_total', 'user_level_id', 'login_count', 'deposit_count',
                         'this_tag_code', 'last_tag_code', 'last_two_tag_code', 'last_three_tag_code',
                         'this_day_step', 'last_day_step', 'last_two_day_step'
                         ]:
            source_df[df_column] = source_df[df_column].fillna(0)
            source_df[df_column] = source_df[df_column].astype(int)
        if df_column in ['user_name', 'game_type', 'ag_name', 'host']:
            source_df[df_column] = source_df[df_column].astype(str)
        if df_column == 'data_date':
            source_df[df_column] = pd.to_datetime(source_df[df_column], format='%Y-%m-%d')
        if df_column == 'register_date':
            source_df[df_column] = pd.to_datetime(source_df[df_column], format='%Y-%m-%d %H:%M:%S')
        if df_column in ['balance']:
            # numeric資料要轉decimal
            source_df[df_column] = source_df[df_column].apply(str)
            source_df[df_column] = source_df[df_column].apply(Decimal)
        if df_column in ['deposit_amount', 'bet_amount', 'commissionable', 'payoff']:
            source_df[df_column] = source_df[df_column].astype(float)
    return source_df

def df_demo_amount_format(source_df: pd.DataFrame()) -> pd.DataFrame():
    for df_column in source_df.columns:
        if df_column in [
            'balance',
            'bet_amount', 'commissionable', 'payoff',
            'deposit_amount', 'withdraw_amount',
            'premium_amount',
            'profit_loss'
        ]:
            source_df[df_column] = source_df[df_column].fillna(0)
            source_df[df_column] = source_df[df_column] / 100
            source_df[df_column] = source_df[df_column].apply(str)
            source_df[df_column] = source_df[df_column].apply(Decimal)
    return source_df


def df_timezone_format(source_df: pd.DataFrame(), timezone: str) -> pd.DataFrame():
    target_timezone = pytz.timezone(timezone)
    for df_column in source_df.columns:
        if df_column in ['register_date', 'last_login', 'last_online']:
            source_df[df_column] = source_df[df_column].dt.tz_localize(target_timezone, ambiguous='infer')

    return source_df


def split_list(origin_list, wanted_parts=1):
    length = len(origin_list)
    return [origin_list[i * length // wanted_parts: (i + 1) * length // wanted_parts]
            for i in range(wanted_parts)]
