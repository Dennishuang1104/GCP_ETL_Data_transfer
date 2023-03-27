import pandas as pd
from enum import IntEnum

from request.Request.ReqAdaptor import ReqAdaptor, AdaptorMode
from DataStruct.APIStruct.BBOSApi import *
from Environment import cfg


class ApiMode(IntEnum):
    DEFAULT = -1
    TOKEN_MODE = 0
    ICON_MODE = 1
    TAG_ADD_MODE = 2
    TAG_BATCH_MODE = 3
    TAG_BATCH_DELETE_MODE = 4
    TAG_USER_COUNT_MODE = 5
    TAG_USER_RESET_MODE = 6
    PLAYER_LIST_MODE = 7


class BBOS_API(ReqAdaptor):
    def __init__(self, api_config: str,
                 token_req_struct: TokenReqStruct = TokenReqStruct(),
                 icon_req_struct: IconReqStruct = IconReqStruct(),
                 icon_res_struct: IconResStruct = IconResStruct(response={}),
                 tag_add_req_struct: TagAddReqStruct = TagAddReqStruct(),
                 tag_add_res_struct: TagAddResStruct = TagAddResStruct(response={}),
                 tag_batch_req_struct: TagBatchReqStruct = TagBatchReqStruct(),
                 tag_batch_res_struct: TagBatchResStruct = TagBatchResStruct(response={}),
                 tag_user_count_req_struct: TagUserCountReqStruct = TagUserCountReqStruct(),
                 tag_user_count_res_struct: TagUserCountResStruct = TagUserCountResStruct(response={}),
                 tag_user_reset_req_struct: TagUserResetReqStruct = TagUserResetReqStruct(),
                 player_list_req_struct: PlayerListReqStruct = PlayerListReqStruct(),
                 player_list_res_struct: PlayerListResStruct = PlayerListResStruct(response={}),
                 ):
        super(BBOS_API, self).__init__()
        self.token = cfg.get('BBOS', 'API_TOKEN')
        self.authorization = cfg.get('BBOS', 'API_AUTH')
        self.grant_type = 'service_token'
        self.code = cfg.get(api_config.upper(), 'API_CODE')
        self.client_id = cfg.get(api_config.upper(), 'API_CLIENT_ID')
        self.root_url = 'https://api.bbin-fun.com/api/v1'
        self.result = pd.DataFrame()
        self.pagination = pd.DataFrame()
        self.access_token = None
        self.total_num = 0
        self.__map_mode = {
            ApiMode.TOKEN_MODE: self.__request_token,
            ApiMode.ICON_MODE: self.__request_icon,
            ApiMode.TAG_ADD_MODE: self.__request_tag_add,
            ApiMode.TAG_BATCH_MODE: self.__request_tag_batch,
            ApiMode.TAG_BATCH_DELETE_MODE: self.__request_tag_batch_delete,
            ApiMode.TAG_USER_COUNT_MODE: self.__request_tag_user_count,
            ApiMode.TAG_USER_RESET_MODE: self.__request_tag_user_reset,
            ApiMode.PLAYER_LIST_MODE: self.__request_player_list,
        }
        self.api_mode = ApiMode.DEFAULT
        self.__token_req_struct = token_req_struct
        self.__icon_req_struct = icon_req_struct
        self.__icon_res_struct = icon_res_struct
        self.__tag_add_req_struct = tag_add_req_struct
        self.__tag_add_res_struct = tag_add_res_struct
        self.__tag_batch_req_struct = tag_batch_req_struct
        self.__tag_batch_res_struct = tag_batch_res_struct
        self.__tag_user_count_req_struct = tag_user_count_req_struct
        self.__tag_user_count_res_struct = tag_user_count_res_struct
        self.__tag_user_reset_req_struct = tag_user_reset_req_struct
        self.__player_list_req_struct = player_list_req_struct
        self.__player_list_res_struct = player_list_res_struct

    @property
    def TOKEN_MODE(self):
        return ApiMode.TOKEN_MODE

    @property
    def ICON_MODE(self):
        return ApiMode.ICON_MODE

    @property
    def TAG_ADD_MODE(self):
        return ApiMode.TAG_ADD_MODE

    @property
    def TAG_BATCH_MODE(self):
        return ApiMode.TAG_BATCH_MODE

    @property
    def TAG_BATCH_DELETE_MODE(self):
        return ApiMode.TAG_BATCH_DELETE_MODE

    @property
    def TAG_USER_COUNT_MODE(self):
        return ApiMode.TAG_USER_COUNT_MODE

    @property
    def TAG_USER_RESET_MODE(self):
        return ApiMode.TAG_USER_RESET_MODE

    @property
    def PLAYER_LIST_MODE(self):
        return ApiMode.PLAYER_LIST_MODE

    @property
    def token_req_struct(self):
        return self.__token_req_struct

    @property
    def icon_req_struct(self):
        return self.__icon_req_struct

    @property
    def icon_res_struct(self):
        return self.__icon_res_struct

    @property
    def tag_add_req_struct(self):
        return self.__tag_add_req_struct

    @property
    def tag_add_res_struct(self):
        return self.__tag_add_res_struct

    @property
    def tag_batch_req_struct(self):
        return self.__tag_batch_req_struct

    @property
    def tag_batch_res_struct(self):
        return self.__tag_batch_res_struct

    @property
    def tag_user_count_req_struct(self):
        return self.__tag_user_count_req_struct

    @property
    def tag_user_count_res_struct(self):
        return self.__tag_user_count_res_struct

    @property
    def tag_user_reset_req_struct(self):
        return self.__tag_user_reset_req_struct

    @property
    def player_list_req_struct(self):
        return self.__player_list_req_struct

    @property
    def player_list_res_struct(self):
        return self.__player_list_res_struct

    def api_exec(self):
        try:
            self.__map_mode[self.api_mode]()
        except:
            raise

    def watch_api_response_for(self):
        try:
            result_code = self.result['result']
            # result_code: 執行結果｜ok：成功｜error：失敗）
            if result_code == 'error':
                error_code = self.result['code'] if 'code' in self.result else 'No error code in result'
                error_message = self.result['msg'] if 'message' in self.result else 'No error message in result'
                raise Exception(
                    f'api return '
                    f'error code :{error_code} with '
                    f'error message :{error_message}'
                )
        except:
            raise

    def __request_token(self):
        try:
            self.url = f'{self.root_url}/oauth2/token'
            self.add_to_headers('token', self.token)
            self.add_to_headers('Authorization', self.authorization)
            self.token_req_struct.grant_type = self.grant_type
            self.token_req_struct.code = self.code
            self.token_req_struct.client_id = self.client_id
            self.payload = self.token_req_struct.show()
            self.mode = AdaptorMode.POST
            self.exec()
            self.result = self.response_data.json()
            self.access_token = self.result['access_token']
        except Exception as e:
            raise

    def __request_icon(self):
        try:
            self.url = f'{self.root_url}/e/icons'
            self.add_to_headers('access-token', self.access_token)
            self.add_to_headers('token', self.token)
            self.payload = self.icon_req_struct.show()
            self.mode = AdaptorMode.GET
            self.exec()
            self.result = self.response_data.json()
            self.watch_api_response_for()
            result_df = pd.DataFrame()
            for icon in self.result['ret']:
                icon_res_struct = IconResStruct(icon)
                data = [icon_res_struct.data()]
                columns = icon_res_struct.columns()
                icon_df = pd.DataFrame(data, columns=columns)
                result_df = pd.concat([result_df, icon_df], ignore_index=True)
            self.result = result_df
        except Exception as e:
            raise

    def __request_tag_add(self):
        try:
            self.url = f'{self.root_url}/e/player/tag'
            self.add_to_headers('access-token', self.access_token)
            self.add_to_headers('token', self.token)
            self.payload = self.tag_add_req_struct.show()
            self.mode = AdaptorMode.POST
            self.exec()
            self.result = self.response_data.json()
            self.watch_api_response_for()
            tag_add_res_struct = TagAddResStruct(self.result['ret'])
            self.result = pd.DataFrame([tag_add_res_struct.data()], columns=tag_add_res_struct.columns())
        except Exception as e:
            raise

    def __request_tag_batch(self):
        try:
            self.url = f'{self.root_url}/e/player/tag/batch'
            self.add_to_headers('access-token', self.access_token)
            self.add_to_headers('token', self.token)
            self.payload = self.tag_batch_req_struct.show()
            self.mode = AdaptorMode.POST
            self.exec()
            self.result = self.response_data.json()
            self.watch_api_response_for()
            result_df = pd.DataFrame()
            for user in self.result['ret']:
                tag_batch_res_struct = TagBatchResStruct(user)
                data = [tag_batch_res_struct.data()]
                columns = tag_batch_res_struct.columns()
                level_df = pd.DataFrame(data, columns=columns)
                result_df = pd.concat([result_df, level_df], ignore_index=True)
            self.result = result_df
        except Exception as e:
            raise

    def __request_tag_batch_delete(self):
        try:
            self.url = f'{self.root_url}/e/player/tag/batch'
            self.add_to_headers('access-token', self.access_token)
            self.add_to_headers('token', self.token)
            self.payload = self.tag_batch_req_struct.show()
            self.mode = AdaptorMode.DELETE
            self.exec()
            self.result = self.response_data.json()
            self.watch_api_response_for()
            self.result = self.result['result']
        except Exception as e:
            raise

    def __request_tag_user_count(self):
        try:
            self.url = f'{self.root_url}/e/player/tag/user_count'
            self.add_to_headers('access-token', self.access_token)
            self.add_to_headers('token', self.token)
            self.mode = AdaptorMode.GET
            self.exec()
            self.result = self.response_data.json()
            self.watch_api_response_for()
            result_df = pd.DataFrame()
            for tag in self.result['ret']:
                tag_user_count_res_struct = TagUserCountResStruct(tag)
                data = [tag_user_count_res_struct.data()]
                columns = tag_user_count_res_struct.columns()
                tag_count_df = pd.DataFrame(data, columns=columns)
                result_df = pd.concat([result_df, tag_count_df], ignore_index=True)
            self.result = result_df
        except Exception as e:
            raise

    def __request_tag_user_reset(self):
        try:
            self.url = f'{self.root_url}/e/player/tag/{self.tag_user_reset_req_struct.id}/reset'
            self.add_to_headers('access-token', self.access_token)
            self.add_to_headers('token', self.token)
            self.payload = self.tag_user_reset_req_struct.show()
            self.mode = AdaptorMode.DELETE
            self.exec()
            self.result = self.response_data.json()
            self.watch_api_response_for()
            self.result = self.result['result']
        except Exception as e:
            raise

    def __request_player_list(self):
        try:
            self.url = f'{self.root_url}/e/player/list'
            self.add_to_headers('access-token', self.access_token)
            self.add_to_headers('token', self.token)
            self.payload = self.player_list_req_struct.show()
            self.mode = AdaptorMode.GET
            self.exec()
            self.result = self.response_data.json()
            self.watch_api_response_for()
            self.pagination = self.result['pagination']
            result_df = pd.DataFrame()
            for user in self.result['ret']:
                player_list_res_struct = PlayerListResStruct(user)
                data = [player_list_res_struct.data()]
                columns = player_list_res_struct.columns()
                user_df = pd.DataFrame(data, columns=columns)
                result_df = pd.concat([result_df, user_df], ignore_index=True)
            self.result = result_df
        except Exception as e:
            raise