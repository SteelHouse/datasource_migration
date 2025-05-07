import datetime
import json
from os import path, remove

import requests

from python.utils.config import load_config_with_pass

liveramp_auth_config = load_config_with_pass('../config.ini', '../pass.ini', 'liveramp_authorization')


def get_oauth_token():
    cache_file_path = liveramp_auth_config['cache_file_path']
    if path.exists(cache_file_path):
        with open(cache_file_path, "r") as infile:
            cache = json.load(infile)
        if int(datetime.datetime.now().timestamp()) > cache['expiration_date']:
            return get_token_and_cache()
        else:
            return cache['access_token']
    else:
        return get_token_and_cache()


def get_token_and_cache():
    request_body = {'grant_type': 'password', 'username': liveramp_auth_config['account_id'], 'password': liveramp_auth_config['password'],
                    'scope': 'openid', 'client_id': liveramp_auth_config['client_id'], 'response_type': 'token'}
    response = requests.post(liveramp_auth_config['oauth_url'], data=request_body)
    if response.status_code == 200:
        data = json.loads(response.text)
        expiration_time = datetime.datetime.now() + datetime.timedelta(seconds=data['expires_in'])
        data['expiration_date'] = int(expiration_time.timestamp())
        cache_file_path = liveramp_auth_config['cache_file_path']
        if path.exists(cache_file_path):
            with open(cache_file_path, "w") as outfile:
                json.dump(data, outfile)
        else:
            with open(cache_file_path, "x") as outfile:
                json.dump(data, outfile)
        return data['access_token']
    else:
        raise Exception("cannot get oauth token: response - " + str(response.status_code) + ": " + response.reason)


def delete_cache():
    cache_file_path = liveramp_auth_config['cache_file_path']
    if path.exists(cache_file_path):
        remove(cache_file_path)
