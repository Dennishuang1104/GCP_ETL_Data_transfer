import os
import sys
sys.path.append(f'{os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))}')
import getopt
import traceback
import functools
from Adaptors.BQAdaptor import BQ_MASK
from util.Hall_dict import hall_dict, hall_type
from service.etl_demo_hall import ETL_Spanner_BBIN_Demo_Service , ETL_Spanner_BBOS_Demo_Service
from Adaptors.GAAdaptor import BQ_GA , BQ_GA_reacquire
from Tools.ThreadTool import ThreadTool


def bbin_work(hall, begin_date=None, end_date=None):
    bbin_hall = hall_dict[hall]()
    print('hall name: ', bbin_hall.hall_name)

    service = ETL_Spanner_BBIN_Demo_Service(spanner_db=f'{bbin_hall.db_name}', bq_db=BQ_MASK())
    service.member_info(hall=bbin_hall, start=begin_date, end=end_date)

    # bbin b9廳 用multi-thread執行
    task_thread = ThreadTool(14)
    task_list = {
        functools.partial(ETL_Spanner_BBIN_Demo_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=BQ_MASK()).login_log,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Demo_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=BQ_MASK()).game_dict,
                          hall=bbin_hall),
        functools.partial(ETL_Spanner_BBIN_Demo_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=BQ_MASK()).bet_analysis,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Demo_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=BQ_MASK()).deposit_withdraw,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Demo_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=BQ_MASK()).offer,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Demo_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=BQ_MASK()).profit_loss,
                          hall=bbin_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBIN_Demo_Service(
            spanner_db=f'{bbin_hall.db_name}', bq_db=BQ_MASK()).vip_login,
                          hall=bbin_hall, start=begin_date, end=end_date)
    }
    for task in task_list:
        task_thread.threads.append(task_thread.executor.submit(task))
    task_thread.check_thread()


def bbos_work(hall, begin_date=None, end_date=None):
    bbos_hall = hall_dict[hall]()
    print(f'BBOS_hall name: ', bbos_hall.hall_name)
    service = ETL_Spanner_BBOS_Demo_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_MASK())
    service.member_info(hall=bbos_hall, start=begin_date, end=end_date)

    # 用multi-thread執行
    task_thread = ThreadTool(12)
    task_list = {
        functools.partial(ETL_Spanner_BBOS_Demo_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_MASK()).login_log,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Demo_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_MASK()).game_dict,
                          hall=bbos_hall),
        functools.partial(ETL_Spanner_BBOS_Demo_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_MASK()).bet_analysis,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Demo_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_MASK()).deposit_withdraw,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Demo_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_MASK()).offer,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Demo_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_MASK()).dispatch,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Demo_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_MASK()).applicant,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Demo_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_MASK()).promotion,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Demo_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_MASK()).profit_loss,
                          hall=bbos_hall, start=begin_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Demo_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_MASK()).vip_level,
                          hall=bbos_hall),
        functools.partial(ETL_Spanner_BBOS_Demo_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_MASK()).vip_login,
                          hall=bbos_hall, start=begin_date, end=end_date),
        # # functools.partial(ETL_Spanner_BBOS_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_MASK()).login_location,
        #                   hall=bbos_hall, start=begin_date, end=end_date)

    }
    for task in task_list:
        task_thread.threads.append(task_thread.executor.submit(task))
    task_thread.check_thread()

    ga_task_thread = ThreadTool(2)
    ga_task_list = {
        functools.partial(ETL_Spanner_BBOS_Demo_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_GA()).ga_data,
                          hall=bbos_hall, data_type='data_set', start=end_date, end=end_date),
        functools.partial(ETL_Spanner_BBOS_Demo_Service(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_GA()).ga_data,
                          hall=bbos_hall, data_type='page_path', start=end_date, end=end_date),
        # functools.partial(ETL_Spanner_Service(spanner_db=f'{bbin_hall.db_name}', bq_db=BQ_GA()).ga_data,
        #                   hall=bbin_hall, data_type='firebase_page', start=ga_begin_date, end=ga_end_date)
    }
    for ga_task in ga_task_list:
        ga_task_thread.threads.append(ga_task_thread.executor.submit(ga_task))
    ga_task_thread.check_thread()



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
        if hall_type[hall] == 'DEMO_HALL_BBOS':
            # bbin_work(hall=hall, begin_date=begin_date, end_date=end_date)
            bbos_work(hall=hall, begin_date=begin_date, end_date=end_date)
            return 'BBOS Demo Hall data transfersuccess'

        elif hall_type[hall] == 'DEMO_HALL_BBIN':
            bbin_work(hall=hall, begin_date=begin_date, end_date=end_date)
            return 'BBIN Demo Hall data transfersuccess'

    except Exception as e:
        print(traceback.format_exc())
        print('Error_Occur___')
        return traceback.format_exc()

# if __name__ == '__main__':
#     main(sys.argv[1:]
