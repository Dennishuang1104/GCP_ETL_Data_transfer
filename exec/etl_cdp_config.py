import os
import sys

sys.path.append(f'{os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))}')
import Environment
import functools
import sys
import getopt
import traceback
# from basis import Logger
from Adaptors.SpannerAdaptor import CDP_BBOS
from Adaptors.CloudSQLAdaptor import BaseCDP
from service import ETL_Spanner_Config


def work(task, begin_date=None, end_date=None):
    init_service = ETL_Spanner_Config(spanner_db='cdp_config', source_db='cdp_config')
    task_map = {
        'users': functools.partial(init_service.user_insert),
        'ip_whitelist': functools.partial(init_service.ip_whitelist),
        'param_config': functools.partial(init_service.param_config),
        'custom_tags': functools.partial(init_service.custom_tags),
        'menu_config': functools.partial(init_service.menu_config),
        'tag_info': functools.partial(init_service.tag_info),
        'new_tag_info': functools.partial(init_service.new_tag_info),
        # 'activity_analysis': functools.partial(init_service.activity_analysis, start=begin_date, end=end_date),
    }
    task_function = task_map[task]
    task_function()

    # init_service = ETL_Spanner_Config(spanner_db='cdp_config', db=BaseCDP('CDP'))
    # task_map = {
    #     'tag_info': functools.partial(init_service.tag_info, start=begin_date, end=end_date),
    # }
    # task_function = task_map[task]
    # task_function()


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

        # log = Logger.Logger(logging_level=Environment.LOGGING_INFO, abspath=f'/etl_{task}.log')
        # log.info(f'input params service = {task}, begin_date = {begin_date}, end_date = {end_date}')

    except Exception as e:
        pass
        # log = Logger.Logger(logging_level=Environment.LOGGING_INFO, abspath=f'/error.log')
        # log.exception(traceback.format_exc())

    try:
        work(task=task, begin_date=begin_date, end_date=end_date)
    except Exception as e:
        pass
        # log.exception(traceback.format_exc())