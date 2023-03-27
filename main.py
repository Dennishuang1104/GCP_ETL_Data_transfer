from exec import etl_cdp_by_hall, etl_cdp_demo, etl_cdp_test , etl_api_hall
from datetime import datetime, timedelta
import google.cloud.logging
import functions_framework
from util.Hall_dict import demo_task


@functions_framework.http
def main(hall):
    # request_json = request.get_json(silent=True)
    # hall = request_json['hall']

    # 美東時間換算日期
    begin_date = datetime.today().date()  # 因為cr以格林威治時間為主，台灣時間
    end_date = (datetime.today() + timedelta(hours=8)).date()   # 因為cr以格林威治時間為主，台灣時間

    # 補資料用
    # begin_date = ((datetime.today() + timedelta(hours=8))-timedelta(days=3000)).date()
    begin_date = '2022-04-10'

    # 本機時間
    # begin_date = (datetime.today() - timedelta(hours=12)).date()
    # end_date = (datetime.today() - timedelta(hours=12)).date()
    print(f'Start from {begin_date} to {end_date} ETL Process starting')

    # 參數設定
    argv = ["-h", f"{hall}", "-b", f"{begin_date}", "-e", f"{end_date}"]
    # transfer = etl_cdp_by_hall

    # Demo 廳
    # transfer = etl_cdp_demo

    # Test 廳
    # transfer = etl_cdp_test

    # Api_Caller
    transfer = etl_api_hall

    transfer.main(argv)
    google.cloud.logging.Client()

    return 'all work done'


if __name__ == '__main__':
    hall = 'halo'
    main(hall)

# ["vx88","kr","esbp","5151","halo","ma","b9"]