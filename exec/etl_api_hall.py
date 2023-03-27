from Adaptors.BQAdaptor import BQ_SSR
from util.Hall_dict import hall_dict
from service.call_api import Call_BBOS_API
import getopt
import traceback
from util.Hall_dict import hall_dict, hall_type

def main(argv):
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
        if hall_type[hall] == 'BBOS':
            bbos_hall = hall_dict[hall]()
            service = Call_BBOS_API(spanner_db=f'{bbos_hall.db_name}', bq_db=BQ_SSR())
            service.get_member_data(hall=bbos_hall, start=begin_date, end=end_date)
            return 'BBOS Hall data transfersuccess'

    except Exception as e:
        print(traceback.format_exc())
        print('Error_Occur___')
        return traceback.format_exc()



    # return df


# if __name__ == '__main__':
#     begin_date = '2022-04-10'
#     end_date = '2023-02-02'
#     hall = '5151'
#     main(hall, begin_date, end_date)
