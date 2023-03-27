import re
from datetime import timedelta
import dateutil

from DataStruct import BaseStruct


class PlayerListResStruct(BaseStruct):
    def __init__(self, response: dict):
        super(PlayerListResStruct).__init__()
        self._id: int = 0 if 'id' not in response else response['id']
        self._parent_id: int = 0 if 'parent_id' not in response else response['parent_id']
        self._username: str = '' if 'username' not in response else response['username']
        self._upper_id: int = 0 if 'upper' not in response else response['upper']['id']
        self._enable: bool = None if 'enable' not in response else response['enable']
        self._bankrupt: bool = None if 'bankrupt' not in response else response['bankrupt']
        self._locked: bool = None if 'locked' not in response else response['locked']
        self._tied: bool = None if 'tied' not in response else response['tied']
        self._blacklist_modified_at = None
        if 'blacklist_modified_at' not in response:
            pass
        else:
            if response['blacklist_modified_at'] is None:
                pass
            else:
                self._blacklist_modified_at: str = (dateutil.parser.parse(response['blacklist_modified_at']) + timedelta(hours=-12)).strftime('%Y-%m-%d %H:%M:%S')
        self._last_login = None
        if 'last_login' not in response:
            pass
        else:
            if response['last_login'] is None:
                pass
            else:
                self._last_login: str = (dateutil.parser.parse(response['last_login']) + timedelta(hours=-12)).strftime('%Y-%m-%d %H:%M:%S')
        self._last_ip: str = '' if 'last_ip' not in response else response['last_ip']
        self._last_country: str = '' if 'last_country' not in response else response['last_country']
        self._last_city_id: int = 0 if 'last_city_id' not in response else response['last_city_id']
        self._last_online = None
        if 'last_online' not in response:
            pass
        else:
            if response['last_online'] is None:
                pass
            else:
                self._last_online: str = (dateutil.parser.parse(response['last_online']) + timedelta(hours=-12)).strftime('%Y-%m-%d %H:%M:%S')
        self._created_at: str = None if 'created_at' not in response else (dateutil.parser.parse(response['created_at']) + timedelta(hours=-12)).strftime('%Y-%m-%d %H:%M:%S')
        self._created_ip: str = '' if 'created_ip' not in response else response['created_ip']
        self._created_country: str = '' if 'created_country' not in response else response['created_country']
        self._created_city: str = '' if 'created_city' not in response else response['created_city']
        self._created_by: int = 0 if 'created_by' not in response else response['created_by']
        self._user_id: int = 0 if 'user_id' not in response else response['user_id']
        self._alias: str = '' if 'alias' not in response else re.sub("["u"\U00010000-\U0010ffff""]+", '', response['alias'])
        self._birthday: str = '' if 'birthday' not in response else response['birthday']
        self._gender: int = 0 if 'gender' not in response else response['gender']
        self._image: int = 0 if 'image' not in response else response['image']
        self._custom_image: str = '' if 'custom_image' not in response else response['custom_image']
        self._custom: bool = None if 'custom' not in response else response['custom']
        self._content_rating: int = 0 if 'content_rating' not in response else response['content_rating']
        self._promotion_code: str = '' if 'promotion_code' not in response else response['promotion_code']
        self._vip_level_id: int = 0 if 'vip_level_id' not in response else response['vip_level_id']
        self._ingroup_transfer_domain: dict = None if 'ingroup_transfer' not in response else response['ingroup_transfer']['domain']
        self._ingroup_transfer_user: dict = None if 'ingroup_transfer' not in response else response['ingroup_transfer']['user']
        self._parent_username: str = '' if 'parent' not in response else response['parent']['username']
        self._upper_username: dict = None if 'upper' not in response else response['upper']['username']
        self._blacklist: str = '' if 'blacklist' not in response else ','.join(str(x) for x in response['blacklist'])
        self._balance: float = 0.0 if 'cash' not in response else response['cash']['balance']
        self._currency: str = '' if 'cash' not in response else response['cash']['currency']
        self._level_name: str = '' if 'level' not in response else response['level']['name']


