import sys
import time
from os import linesep
from urllib.parse import urlencode

import requests

from python.utils.authorization import get_oauth_token, delete_cache
from python.utils.config import config

audience_service_qa_config = config('../config.ini', 'audience_service_qa')
audience_service_prod_config = config('../config.ini', 'audience_service_prod')
audience_service_path_config = config('../config.ini', 'audience_service_path_urls')


def send(method, url, query_params=None, path_param_key=None, path_param_value=None, request_headers=None,
         json_data=None, do_lr_auth=False, retry_timer=5):
    """
    Sends an http request. Retrieves auth token if needed (TODO: not tested yet).
    Attempts to retry the request if the status
    code is 502 or 504
    (default retry times are 5, 10, 20, and 40 seconds).
    :param method: either 'GET','POST','PUT', or 'DELETE'
    :param url: the url to send the request to
    :param query_params: the query parameters to send
    :param path_param_key: the key of the path parameter to replace in the url
    :param path_param_value: the value of the path parameter to replace in the url
    :param request_headers: the headers to send with the request
    :param json_data: the body of the request in json format
    :param do_lr_auth: whether to use auth logic for LiveRamp
    :param retry_timer: the time to wait before retrying the request
    :return: the response in json format or
             None for 201 with no id - sometimes occurs for enable_segments_url and add_segments_to_dm_url
    :exception: Exception for all status codes except 200, 201, and 207
    """
    if path_param_key is not None:
        url = url.replace(path_param_key, str(path_param_value))
    if query_params is not None:
        url = url + "?" + urlencode(query_params, doseq=True)
    if do_lr_auth:
        token = get_oauth_token()
        api_config = config('../config.ini', 'liveramp_activation_api')
        request_headers['Authorization'] = 'Bearer ' + token
        request_headers['LR-Org-Id'] = api_config['org_id']
    response = {'status_code': 500, 'reason': 'System Error'}
    if method == 'GET':
        response = requests.get(url, headers=request_headers)
    elif method == 'POST':
        response = requests.post(url, headers=request_headers, json=json_data)
    elif method == 'PUT':
        response = requests.put(url, headers=request_headers, json=json_data)
    elif method == 'DELETE':
        response = requests.delete(url, headers=request_headers, json=json_data)
    status_code = response.status_code
    if str(status_code).startswith('2'):
        try:
            return response.json()
        except requests.exceptions.JSONDecodeError:
            return None
    elif str(status_code) == '502' or str(status_code) == '504':
        handle_retry(method, url, request_headers, query_params, path_param_key, path_param_value, json_data, response,
                     do_lr_auth, retry_timer)
    elif do_lr_auth and str(status_code) == '401' or str(status_code) == '403':
        delete_cache()
        handle_retry(method, url, request_headers, query_params, path_param_key, path_param_value, json_data, response,
                     do_lr_auth, retry_timer)
    else:
        # crash 500, 400, 422
        handle_error(response, url, print_error=True, raise_exception=True, request_headers=request_headers,
                     json_data=json_data)


def handle_error(response, url, print_error=True, raise_exception=True, request_headers=None, json_data=None):
    error = "error for request - " + url + linesep
    error += "response - " + str(response.status_code) + ": " + response.reason + linesep
    # If there is a json response, add the error code and message to the error message if they exist
    try:
        json = response.json()
        if json.get('errorCode') is not None:
            error += "error code - " + str(json.get('errorCode')) + linesep
        if json.get('message') is not None:
            error += "error message - " + str(json.get('message')) + linesep
    except requests.exceptions.JSONDecodeError:
        # response didn't have error info, and we have enough info from the response, so pass here
        pass
    if request_headers is not None:
        error += "request headers - " + str(request_headers) + linesep
    if json_data is not None:
        error += "json data - " + str(json_data) + linesep
    if print_error:
        print(error, file=sys.stderr)
    if raise_exception:
        raise Exception(error)


def handle_retry(method, url, request_headers, query_params, path_param_key, path_param_value, data, response,
                 do_lr_auth, retry_timer):
    if retry_timer != 0 and retry_timer > 40:
        handle_error(response, url, print_error=True, raise_exception=True, request_headers=request_headers,
                     json_data=data)
    else:
        handle_error(response, url, print_error=True, raise_exception=False, request_headers=request_headers,
                     json_data=data)
        print('retrying in ' + str(retry_timer) + ' seconds...')
        time.sleep(retry_timer)
        retry_timer *= 2
        return send(method=method, url=url, query_params=query_params, path_param_key=path_param_key,
                    path_param_value=path_param_value, request_headers=request_headers, json_data=data,
                    do_lr_auth=do_lr_auth, retry_timer=retry_timer)
