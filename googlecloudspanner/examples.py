from google.cloud import spanner
from datetime import datetime
import pytz
from ExampleClass import ExampleClass

spanner_client = ExampleClass(database='cdp')
spanner_client.table_name = 'member_info'

# select
spanner_client.statement = "SELECT * FROM member_info WHERE hall_id = 3820325 and domain_id = 70 AND user_id = 7935888"
spanner_client.mode = spanner_client.QUERY_MODE
spanner_client.exec()

# insert
us = pytz.timezone('America/New_York')

spanner_client.columns = ['hall_id', 'hall_name', 'domain_id', 'domain_name',
                          'user_id', 'ag_name', 'user_name', 'name_real',
                          'register_date', 'register_ip', 'register_country', 'register_city', 'register_by',
                          'user_phone', 'user_mail', 'balance',
                          'last_login', 'last_ip', 'last_country', 'last_city_id', 'last_online',
                          'zalo', 'facebook',
                          'user_type', 'vip_level', 'deep_effort',
                          'vip_activate_date', 'vip_expired_date',
                          'deep_effort_activate_date', 'deep_effort_expired_date',
                          'arbitrage_flag', 'user_level_id', 'user_level']
spanner_client.write_data = [
    ('3820325', 'BBOS', '41', 'ey',
     '12345', 'agent0', 'ABC', '321',
     datetime.strptime('2019-10-31 02:15:10', '%Y-%m-%d %H:%M:%S'), '111.235.135.45', 'MY', None, '3',
     '', '', '0',
     None, None, None, None, None,
     '', '',
     '0', '0', '0',
     None, None,
     None, None,
     '0', '391', '未分層'),
    ('3820325', 'BBOS', '41', 'ey',
     '12346', 'bbostest1', 'DEF', '123',
     datetime.strptime('2019-10-31 23:17:19', '%Y-%m-%d %H:%M:%S').replace(tzinfo=us),
     '2405:1c0:61db:133:2565:394a:eeb5:456b', 'MY', None,
     '1',
     '123324234', 'victor_tseng@mail.chungyo.net', '0',
     datetime.strptime('2019-11-19 23:03:00', '%Y-%m-%d %H:%M:%S'), '2405:1c0:61db:133:9422:58a0:5bf8:be03', 'MY', None,
     datetime.strptime('2019-11-20 01:30:06', '%Y-%m-%d %H:%M:%S'),
     '', '',
     '0', '0', '0',
     None, None,
     None, None,
     '0', '397', '測試帳號'),
]
spanner_client.mode = spanner_client.INSERT_MODE
spanner_client.exec()
spanner_client.mode = spanner_client.UPSERT_MODE
spanner_client.exec()
spanner_client.mode = spanner_client.REPLACE_MODE
spanner_client.exec()

#
# update
# update balance from primary key: hall_id domain_id user_id
columns = ['hall_id', 'domain_id', 'user_id', 'balance']
spanner_client.mode = spanner_client.UPDATE_MODE
spanner_client.table_name = 'member_info'
spanner_client.columns = columns
spanner_client.update_data = [[3820325, 41, 43613, 12345], [3820325, 41, 43732, 23456]]
spanner_client.exec()

# delete key set
# delete user_id 43613
spanner_client.mode = spanner_client.DELETE_KEYSET_MODE
spanner_client.table_name = 'member_info'
spanner_client.delete_data = [[3820325, 41, 43613]]
spanner_client.exec()

# delete key range
# delete user_id from 43613 to 43732
spanner_client.mode = spanner_client.DELETE_RANGE_MODE
spanner_client.table_name = 'member_info'
spanner_client.delete_range = [[3820325, 41, 43613], [3820325, 41, 43732]]
spanner_client.exec()
