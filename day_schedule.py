from exec import etl_cdp_by_hall_reacquire , etl_cdp_by_hall
from datetime import datetime, timedelta
import google.cloud.logging


def main(hall):
    # request_json = request.get_json(silent=True)
    # hall = request_json['hall']
    # 美東時間換算日期
    # begin_date = (datetime.today() - timedelta(hours=4)).date()  # 因為cr以格林威治時間為主，美東時間
    # end_date = (datetime.today() + timedelta(hours=8)).date()    # 因為cr以格林威治時間為主，台灣時間

    # 本機時間
    begin_date = (datetime.today() - timedelta(hours=12)).date()  # 因為cr 以格林威治時間為主３＋
    end_date = (datetime.today() - timedelta(hours=12)).date()
    print(f'{begin_date} ETL Process starting')

    # 參數設定
    argv = ["-h", f"{hall}", "-b", f"{begin_date}", "-e", f"{end_date}"]
    transfer = etl_cdp_by_hall_reacquire
    transfer.main(argv)
    google.cloud.logging.Client()


# if __name__ == '__main__':
#     hall = 'b9'
#     main(hall)

