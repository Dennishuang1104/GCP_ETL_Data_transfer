import functools
import Environment
from bigquery.BigQuery import BigQueryAdaptor
from bigquery.BigQuery import BigQueryConnection
from bigquery.BigQuery import BigQueryParams


class BQ_GA_reacquire(BigQueryAdaptor):
    def __init__(self):
        bq_params = BigQueryParams(cert_path=f'{Environment.ROOT_PATH}/cert/rd-common-data-c3c99b621388.json')
        super().__init__(BigQueryConnection(bq_params))
        self.resource_id = None
        self.firebase_id = None
        self.hall_id = None
        self.hall_name = None
        self.domain_id = None
        self.domain_name = None
        self.data_date = None

    def query_by(self, action):
        query_mapping = {
            'data_set': functools.partial(self.__set_statement_data_set),
            'page_path': functools.partial(self.__set_statement_page_path),
            'firebase_page': functools.partial(self.__set_statement_firebase_page_title)
        }

        try:
            query_mapping[action]()

        except Exception as e:
            raise

    def __set_statement_data_set(self):
        if self.resource_id == 0:
            return
        self.mode = self.QUERY_MODE
        self.statement = (
            f"SELECT VISIT.clientId AS user_id, "
            f"IFNULL(session_bounce.page_views, 0) AS page_views, "
            f"IFNULL(product_clicks.click, 0) AS product_clicks, "
            f"IFNULL(promotion_clicks.click, 0) AS promotion_clicks, "
            f"IFNULL(service_contact_click.click, 0) AS service_contact, "
            f"IFNULL(service_contact_mobile_click.click, 0) AS service_contact_mobile, "
            f"IFNULL(session_bounce.sessions, 0) AS total_sessions, "
            f"IFNULL(session_bounce.bounces, 0) AS total_bounces, "
            f"IFNULL(session_bounce.time_on_site, 0) AS time_on_site, "
            f"PARSE_DATE('%Y%m%d', VISIT.date) AS data_date "
            f"FROM `rd-common-data.{str(self.resource_id)}.ga_sessions_{self.data_date}` VISIT, "
            f"UNNEST(VISIT.hits) HIT, "
            f"UNNEST (HIT.customDimensions) CD_hall, "
            f"UNNEST (HIT.customDimensions) CD_domain "
            f"LEFT JOIN ( "
            f"SELECT  VISIT.clientId, "
            f"COUNT(*) AS click, "
            f"FROM `rd-common-data.{str(self.resource_id)}.ga_sessions_{self.data_date}` VISIT, "
            f"UNNEST(VISIT.hits) HIT, "
            f"UNNEST (HIT.customDimensions) CD_hall, "
            f"UNNEST (HIT.customDimensions) CD_domain "
            f"WHERE CD_hall.index = 4 AND CD_hall.value = '{str(self.hall_id)}' "
            f"AND CD_domain.index = 15 AND CD_domain.value = '{str(self.domain_id)}' "
            f"AND HIT.type = 'EVENT' "
            f"AND HIT.eventInfo.eventCategory = 'enhanced_ecommerce' "
            f"AND HIT.eventInfo.eventAction = 'click' "
            f"AND HIT.eventInfo.eventLabel = 'productClick' "
            f"GROUP BY VISIT.clientId, VISIT.date "
            f") product_clicks ON VISIT.clientId = product_clicks.clientId "
            f"LEFT JOIN ( "
            f"SELECT VISIT.clientId, "
            f"COUNT(*) AS click, "
            f"FROM `rd-common-data.{str(self.resource_id)}.ga_sessions_{self.data_date}` VISIT,  "
            f"UNNEST(VISIT.hits) HIT, "
            f"UNNEST (HIT.customDimensions) CD_hall, "
            f"UNNEST (HIT.customDimensions) CD_domain "
            f"WHERE CD_hall.index = 4 AND CD_hall.value = '{str(self.hall_id)}' "
            f"AND CD_domain.index = 15 AND CD_domain.value = '{str(self.domain_id)}' "
            f"AND HIT.type = 'EVENT' "
            f"AND HIT.eventInfo.eventCategory = 'enhanced_ecommerce' "
            f"AND HIT.eventInfo.eventAction = 'click' "
            f"AND HIT.eventInfo.eventLabel = 'promotionClick' "
            f"GROUP BY VISIT.clientId, VISIT.date "
            f") promotion_clicks ON VISIT.clientId = promotion_clicks.clientId "
            f"LEFT JOIN ( "
            f"SELECT VISIT.clientId, "
            f"COUNT(*) AS click, "
            f"FROM `rd-common-data.{str(self.resource_id)}.ga_sessions_{self.data_date}` VISIT,  "
            f"UNNEST(VISIT.hits) HIT, "
            f"UNNEST (HIT.customDimensions) CD_hall, "
            f"UNNEST (HIT.customDimensions) CD_domain "
            f"WHERE CD_hall.index = 4 AND CD_hall.value = '{str(self.hall_id)}' "
            f"AND CD_domain.index = 15 AND CD_domain.value = '{str(self.domain_id)}' "
            f"AND HIT.eventInfo.eventCategory = 'online_service' "
            f"AND HIT.eventInfo.eventLabel = 'online_service_contact' "
            f"GROUP BY VISIT.clientId, VISIT.date "
            f") service_contact_click ON VISIT.clientId = service_contact_click.clientId "
            f"LEFT JOIN ( "
            f"SELECT VISIT.clientId, "
            f"COUNT(*) AS click, "
            f"FROM `rd-common-data.{str(self.resource_id)}.ga_sessions_{self.data_date}` VISIT,  "
            f"UNNEST(VISIT.hits) HIT, "
            f"UNNEST (HIT.customDimensions) CD_hall, "
            f"UNNEST (HIT.customDimensions) CD_domain "
            f"WHERE CD_hall.index = 4 AND CD_hall.value = '{str(self.hall_id)}' "
            f"AND CD_domain.index = 15 AND CD_domain.value = '{str(self.domain_id)}' "
            f"AND HIT.eventInfo.eventCategory = 'online_service' "
            f"AND HIT.eventInfo.eventLabel = 'online_service_contact_mobile' "
            f"GROUP BY VISIT.clientId, VISIT.date "
            f") service_contact_mobile_click ON VISIT.clientId = service_contact_mobile_click.clientId "
            f"LEFT JOIN ( "
            f"SELECT VISIT.clientId, "
            f"SUM(VISIT.totals.pageviews) AS page_views, "
            f"COUNT(VISIT.visitNumber) AS sessions, "
            f"SUM(IFNULL(totals.bounces, 0)) AS bounces, "
            f"SUM(IFNULL(totals.timeOnSite, 0)) AS time_on_site "
            f"FROM `rd-common-data.{str(self.resource_id)}.ga_sessions_{self.data_date}` VISIT  "
            f"GROUP BY VISIT.clientId"
            f") session_bounce ON VISIT.clientId = session_bounce.clientId "
            f"WHERE CD_hall.index = 4 AND CD_hall.value = '{str(self.hall_id)}' "
            f"AND CD_domain.index = 15 AND CD_domain.value = '{str(self.domain_id)}' "
            f"AND length(VISIT.clientId) < 10 "
            f"AND VISIT.clientId <> '''undefined''' "
            f"GROUP BY VISIT.clientId, VISIT.date, "
            f"page_views, "
            f"product_clicks, "
            f"promotion_clicks, "
            f"service_contact, service_contact_mobile, "
            f"session_bounce.sessions, session_bounce.bounces, session_bounce.time_on_site "
        )
        print(self.statement)

    def __set_statement_page_path(self):
        if self.resource_id == 0:
            return
        self.mode = self.QUERY_MODE
        self.statement = (
            f"SELECT VISIT.clientId AS user_id, "
            f"SUBSTR(HIT.page.hostname, 0 , 50) AS hostname, "
            f"SUBSTR(HIT.page.pagePath, 0 , 300) AS page_path, "
            f"SUBSTR(HIT.page.pageTitle, 0 , 150) AS page_title, "
            f"COUNT(HIT) AS hits_count, "
            f"CAST(PARSE_DATE('%Y%m%d', VISIT.date) AS STRING) AS data_date "
            f"FROM `rd-common-data.{str(self.resource_id)}.ga_sessions_{self.data_date}` VISIT,  "
            f"UNNEST(VISIT.hits) HIT, "
            f"UNNEST (HIT.customDimensions) CD_hall, "
            f"UNNEST (HIT.customDimensions) CD_domain "
            f"WHERE CD_hall.index = 4 AND CD_hall.value = '{str(self.hall_id)}' "
            f"AND CD_domain.index = 15 AND CD_domain.value = '{str(self.domain_id)}' "
            f"AND length(VISIT.clientId) < 10 AND VISIT.clientId <> 'undefined' "
            f"AND  HIT.page.pagePath IS NOT NULL "
            f"AND (HIT.page.pagePath = '/' "
            f" OR LOWER(HIT.page.pagePath) LIKE '%withdraw%' "
            f" OR LOWER(HIT.page.pagePath) LIKE '%deposit%' "
            f" OR LOWER(HIT.page.pagePath) LIKE '%login%' "
            f" OR LOWER(HIT.page.pagePath) LIKE '%promotion%' "
            f" OR (HIT.page.pagePath) LIKE '%gameLobby%' "
            f" OR (HIT.page.pagePath) LIKE '%gameEntrance%'  "
            f" OR (HIT.page.pagePath) = '/casino' "
            f" OR (HIT.page.pagePath) = '/card' "
            f" OR (HIT.page.pagePath) = '/lottery' "
            f" OR (HIT.page.pagePath) = '/live' "
            f" OR (HIT.page.pagePath) = '/sport' "
            f" OR (HIT.page.pagePath) LIKE '/mobile/iframe/game%' "
            f") "
            f"GROUP BY VISIT.clientId, "
            f"HIT.page.hostname, "
            f"HIT.page.pagePath, "
            f"HIT.page.pageTitle, "
            f"VISIT.date "
        )

    def __set_statement_firebase_page_title(self):
        self.mode = self.QUERY_MODE
        self.statement = (
            f"SELECT SAFE_CAST(user_id AS INT) AS user_id, "
            f"SUBSTR(EVENT.value.string_value, 0 , 150) AS page_title, "
            f"PARSE_DATE('%Y%m%d', event_date) AS data_date, "
            f"FROM `rd-common-data.analytics_{str(self.firebase_id)}.events_{self.data_date}` VISIT, "
            f"UNNEST (event_params) AS EVENT "
            f"WHERE "
            f"SAFE_CAST(user_id AS INT) IS NOT NULL  "
            f"AND EVENT.key = 'firebase_screen_class' "
            f"GROUP BY user_id, page_title, event_date "
        )
