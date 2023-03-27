import os
import sys
sys.path.append(f'{os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))}')
import getopt
import traceback
import functools
from util.Hall_dict import hall_dict, hall_type, BBINhall_dict
from service.etl_test_hall import ETL_Spanner_BBIN_Test_Service, ETL_Spanner_BBOS_Test_Service
from Tools.ThreadTool import ThreadTool
from Adaptors.BQAdaptor import BQ_TEST_SSR, BQ_SSR


def bbin_work(hall, begin_date=None, end_date=None):
    bbin_hall = hall_dict[hall]()
    print('hall name: ', bbin_hall.hall_name)
    service = ETL_Spanner_BBIN_Test_Service(spanner_db=f'{bbin_hall.db_name}', bq_db=BQ_SSR())

    # service = ETL_Spanner_BBOS_Test_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_TEST_SSR())
    service.member_info(hall=bbin_hall, start=begin_date, end=end_date)

    # bbin b9廳 用multi-thread執行
    task_thread = ThreadTool(14)
    task_list = {
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').login_log,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').game_dict,
                          hall=bbin_hall),
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').bet_analysis,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').deposit_withdraw,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').offer,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').profit_loss,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').vip_login,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').login_location,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').user_healthy_score,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').tags,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').tag_of_operate,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').user_active,
                      hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').recommend_meta,
                      hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').smart_message,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').member_journey,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Test_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=f'{bbin_hall.demo_db_name}').ga_data,
                          hall=bbin_hall, start=begin_date, end=end_date)
    }
    for task in task_list:
        task_thread.threads.append(task_thread.executor.submit(task))
    task_thread.check_thread()


def bbos_work(hall, begin_date=None, end_date=None):
    bbos_hall = hall_dict[hall]()
    print(f'BBOS_hall name: ', bbos_hall.hall_name)
    service = ETL_Spanner_BBOS_Test_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_TEST_SSR())
    service.member_info(hall=bbos_hall, start=begin_date, end=end_date)

    # 用multi-thread執行
    task_thread = ThreadTool(12)
    task_list = {
        functools.partial(ETL_Spanner_BBOS_Test_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_TEST_SSR()).login_log,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Test_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_TEST_SSR()).game_dict,
                          hall=bbos_hall),
        functools.partial(ETL_Spanner_BBOS_Test_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_TEST_SSR()).bet_analysis,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Test_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_TEST_SSR()).deposit_withdraw,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Test_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_TEST_SSR()).offer,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Test_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_TEST_SSR()).dispatch,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Test_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_TEST_SSR()).applicant,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Test_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_TEST_SSR()).promotion,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Test_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_TEST_SSR()).profit_loss,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Test_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_TEST_SSR()).vip_level,
                          hall=bbos_hall),
        functools.partial(ETL_Spanner_BBOS_Test_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_TEST_SSR()).vip_login,
                          hall=bbos_hall, start=begin_date, end=end_date),
        # functools.partial(ETL_Spanner_BBOS_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_SSR()).login_location,
        #                   hall=bbos_hall, start=begin_date, end=end_date)
    }
    for task in task_list:
        task_thread.threads.append(task_thread.executor.submit(task))
    task_thread.check_thread()

    # ga_task 取當日 intraday
    # ga_task_thread = ThreadTool(2)
    # ga_task_list = {
    #     functools.partial(ETL_Spanner_BBOS_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_GA()).ga_data,
    #                       hall=bbos_hall, data_type='data_set', start=begin_date, end=end_date),
    #     functools.partial(ETL_Spanner_BBOS_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_GA()).ga_data,
    #                       hall=bbos_hall, data_type='page_path', start=begin_date, end=end_date),
    #     # functools.partial(ETL_Spanner_Service(spanner_db=f'{bbin_hall.db_name}', bq_db=BQ_GA()).ga_data,
    #     #                   hall=bbin_hall, data_type='firebase_page', start=ga_begin_date, end=ga_end_date)
    # }
    # for ga_task in ga_task_list:
    #     ga_task_thread.threads.append(ga_task_thread.executor.submit(ga_task))
    # ga_task_thread.check_thread()


def main(argv):
    """
    Processing arguments
    :param argv:
    :return:
    """
    hall = None
    begin_date = None
    end_date = None

    try:
        opts, args = getopt.getopt(args=argv, shortopts='h:b:e:', longopts=['--hall', '--begin', '--end'])

        for opt, arg in opts:
            if opt in ('-h', '--hall'):
                hall = arg
            elif opt in ('-b', '--begin'):
                begin_date = arg
            elif opt in ('-e', '--end'):
                end_date = arg
        if hall is None:
            raise

    except Exception as e:
        print(f'ERROR_INFO：{traceback.format_exc()}')
        return traceback.format_exc()

    try:
        if hall_type[hall] == 'TEST_HALL':
            # bbin_work(hall=hall, begin_date=begin_date, end_date=end_date)
            bbos_work(hall=hall, begin_date=begin_date, end_date=end_date)
            return 'Test Hall data transfersuccess'

    except Exception as e:
        print(traceback.format_exc())
        print('Error_Occur___')
        return traceback.format_exc()

# if __name__ == '__main__':
#     main(sys.argv[1:]
