import os
import sys

sys.path.append(f'{os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))}')
import Environment
import functools
from datetime import datetime, timedelta
import sys
import getopt
import Logger
import traceback
from basis import Logger
from Adaptors.BQAdaptor import BQ_SSR
from Adaptors.GAAdaptor import BQ_GA
from service import ETL_Spanner_Service, ETL_Spanner_Config
from Tools.ThreadTool import ThreadTool
from util.Hall_dict import hall_dict
from Adaptors.APIAdaptor import BBOS as BBOSApi


def work(task, begin_date=None, end_date=None):
    # 用multithread執行各個廳
    thread_num = len(hall_dict)
    api_thread = ThreadTool(thread_num)

    for hall_name, hall in hall_dict.items():
        bbos_hall = hall_dict[hall_name]()
        print('hall name: ', bbos_hall.domain_name)
        if 'ga_' in task: # ga 只能取得2天前資料
            begin_date = str(datetime.strptime(begin_date, '%Y-%m-%d').date() - timedelta(days=1))
            end_date = str(datetime.strptime(end_date, '%Y-%m-%d').date() - timedelta(days=1))
            service = ETL_Spanner_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_GA())
            task_map = {
                'ga_data_set': functools.partial(service.ga_data, hall=bbos_hall, data_type='data_set',
                                                 start=begin_date, end=end_date),
                'ga_page_path': functools.partial(service.ga_data, hall=bbos_hall, data_type='page_path',
                                                  start=begin_date, end=end_date),
                'ga_firebase_page': functools.partial(service.ga_data, hall=bbos_hall, data_type='firebase_page',
                                                      start=begin_date, end=end_date),
            }
        else:
            service = ETL_Spanner_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_SSR())
            config_service = ETL_Spanner_Config(spanner_db='cdp_config')
            api_service = BBOSApi(bbos_hall.domain_name)
            task_map = {
                'member': functools.partial(service.member_info, hall=bbos_hall, start=begin_date, end=end_date),
                'login': functools.partial(service.login_log, hall=bbos_hall, start=begin_date, end=end_date),
                'game': functools.partial(service.game_dict, hall=bbos_hall),
                'bet': functools.partial(service.bet_analysis, hall=bbos_hall, start=begin_date, end=end_date),
                'depo_with': functools.partial(service.deposit_withdraw, hall=bbos_hall, start=begin_date, end=end_date),
                'offer': functools.partial(service.offer, hall=bbos_hall, start=begin_date, end=end_date),
                'dispatch': functools.partial(service.dispatch, hall=bbos_hall, start=begin_date, end=end_date),
                'applicant': functools.partial(service.applicant, hall=bbos_hall, start=begin_date, end=end_date),
                'promotion': functools.partial(service.promotion, hall=bbos_hall, start=begin_date, end=end_date),
                'profit_loss': functools.partial(service.profit_loss, hall=bbos_hall, start=begin_date, end=end_date),
                'vip_level': functools.partial(service.vip_level, hall=bbos_hall),
                'vip_login': functools.partial(service.vip_login, hall=bbos_hall, start=begin_date, end=end_date),
                'login_location': functools.partial(service.login_location, hall=bbos_hall, start=begin_date, end=end_date),
                'tag_custom': functools.partial(service.tag_custom, hall=bbos_hall, spanner_db=service, spanner_config=config_service),
                'tag_rules': functools.partial(service.tag_rules, hall=bbos_hall, spanner_db=service, spanner_config=config_service),
                'tag_sync': functools.partial(service.tag_sync, hall=bbos_hall, api_service=api_service, spanner_db=service),
                'tag_sync_match': functools.partial(service.tag_sync_match, hall=bbos_hall, api_service=api_service, start=begin_date, end=end_date),

                'tag_operate': functools.partial(service.tag_of_operate, hall=bbos_hall, spanner_db=service, start=begin_date, end=end_date),
                # 'tag_operate_week': functools.partial(service.tag_of_operate_week, hall=bbos_hall, spanner_db=service, start=begin_date, end=end_date),
                'tag_operate_day': functools.partial(service.tag_of_operate_day, hall=bbos_hall, spanner_db=service, start=begin_date, end=end_date),
            }
        task_function = task_map[task]
        # task_function()  # 單廳執行
        api_thread.threads.append(api_thread.executor.submit(task_function))
    api_thread.check_thread()


def main(argv):
    """
    Processing arguments
    :param argv:
    :return:
    """
    task = None
    begin_date = None
    end_date = None

    try:
        opts, args = getopt.getopt(args=argv, shortopts='t:b:e:', longopts=['--task', '--begin', '--end'])

        for opt, arg in opts:
            if opt in ('-t', '--task'):
                task = arg
            elif opt in ('-b', '--begin'):
                begin_date = arg
            elif opt in ('-e', '--end'):
                end_date = arg
        if task is None:
            raise

        log = Logger.Logger(logging_level=Environment.LOGGING_INFO, abspath=f'/etl_{task}.log')
        log.info(f'input params service = {task}, begin_date = {begin_date}, end_date = {end_date}')

    except Exception as e:
        log = Logger.Logger(logging_level=Environment.LOGGING_INFO, abspath=f'/error.log')
        log.exception(traceback.format_exc())

    try:
        work(task=task, begin_date=begin_date, end_date=end_date)
    except Exception as e:
        log.exception(traceback.format_exc())


if __name__ == '__main__':
    main(sys.argv[1:])
