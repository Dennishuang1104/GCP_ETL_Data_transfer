# MySQL

DB access component for google-cloud-mysql v5.7 

Python 資料庫操作物件

安裝:

請在本地的 Git 專案目錄下, clone 本專案, pycharm IDE 可偵測到有兩個 Repository 並分開 commit / push

example :

(venv) PycharmProject/Crawler$ git clone http://34.80.51.134/ghr/dbsettings.git

使用方式 :

from MySQL.MySqlAdaptor import MySqlAdaptor
from MySQL.MySqlDBConnection import MySqlDBConnection
from MySQL.DBParams import DBParams

class ExampleAdaptor(MySqlAdaptor):
    def __init__(self):
        super(ExampleAdaptor, self).__init__(MySqlDBConnection(DBParams(
            host=HOST,
            port=3306,
            user=USER,
            password=PASSWORD,
            db=DEFAULT_DB,
            charset='utf8'
        )))
        
詳細方式請參考 ExampleAdaptor.py