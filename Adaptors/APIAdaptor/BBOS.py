import pandas as pd

from Adaptors.APIAdaptor import BBOS_API


def split_list(source_list, wanted_parts=1):
    length = len(source_list)
    return [source_list[i * length // wanted_parts: (i + 1) * length // wanted_parts]
            for i in range(wanted_parts)]


class BBOS(BBOS_API):
    def __init__(self, api_config: str):
        super(BBOS, self).__init__(api_config)

    def token_api(self):
        self.api_mode = self.TOKEN_MODE
        self.api_exec()

    def icon_api(self) -> pd.DataFrame():
        try:
            self.headers.clear()
            self.api_mode = self.ICON_MODE
            self.icon_req_struct.role = 1
            self.api_exec()
            icon_df = self.result

        except Exception as e:
            raise
        return icon_df

    def tag_add_api(self, icon_id: int, tag_name: str) -> pd.DataFrame():
        try:
            self.headers.clear()
            self.api_mode = self.TAG_ADD_MODE
            self.tag_add_req_struct.icon_id = icon_id
            self.tag_add_req_struct.name = tag_name
            self.api_exec()
            tag_add_df = self.result

        except Exception as e:
            raise
        return tag_add_df

    def tag_batch_api(self, user_id: list, tag_id: int) -> pd.DataFrame():
        try:
            self.headers.clear()
            self.api_mode = self.TAG_BATCH_MODE
            tag_batch_df = pd.DataFrame()
            split_num = int(len(user_id) / 1000) + (len(user_id) % 1000 > 0)
            split_result = split_list(user_id, wanted_parts=split_num)
            self.tag_batch_req_struct.tag_id = tag_id
            for part_num in range(0, len(split_result)):
                user_id = split_result[part_num]
                self.tag_batch_req_struct.user_id = user_id
                self.api_exec()
                tag_batch_df = pd.concat([tag_batch_df, self.result])

        except Exception as e:
            raise
        return tag_batch_df

    def tag_batch_delete_api(self, user_id: list, tag_id: int) -> pd.DataFrame():
        try:
            self.headers.clear()
            self.api_mode = self.TAG_BATCH_DELETE_MODE
            self.tag_batch_req_struct.user_id = user_id
            self.tag_batch_req_struct.tag_id = tag_id
            self.api_exec()
            result = self.result

        except Exception as e:
            raise
        return result

    def tag_user_count_api(self) -> pd.DataFrame():
        try:
            self.headers.clear()
            self.api_mode = self.TAG_USER_COUNT_MODE
            self.api_exec()
            tag_user_count_df = self.result

        except Exception as e:
            raise
        return tag_user_count_df

    def tag_user_reset_api(self, tag_id: int) -> pd.DataFrame():
        try:
            self.headers.clear()
            self.api_mode = self.TAG_USER_RESET_MODE
            self.tag_user_reset_req_struct.id = tag_id
            self.api_exec()
            result = self.result

        except Exception as e:
            raise
        return result

    def player_list_api(self, tag_id: int) -> pd.DataFrame():
        try:
            player_list_df = pd.DataFrame()
            self.headers.clear()
            self.api_mode = self.PLAYER_LIST_MODE
            self.player_list_req_struct.tag_ids = tag_id
            self.api_exec()

            page_result_count = 0
            while page_result_count < int(self.pagination['total']):
                self.player_list_req_struct.first_result = int(page_result_count)
                self.player_list_req_struct.max_results = 1000
                self.api_exec()
                if len(self.result) > 0:
                    player_list_df = pd.concat([player_list_df, self.result], ignore_index=True)
                page_result_count += 1000
            result = player_list_df
        except Exception as e:
            raise
        return result
