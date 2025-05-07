import sys

from python.utils.config import config
from python.utils.request_util import send

"""
This module contains functions to interact with the LiveRamp Activation API. LiveRamp's limit per API GET call is 100 
and each call can be slow.  
"""
api_config = config('../config.ini', 'liveramp_activation_api')


def get_segments_from_distribution_manager(v2_distribution_manager_id, after="", limit=100):
    """
    Gets segments from a distribution manager.
    :param v2_distribution_manager_id: required
    :param after:
    :param limit:

    :return: list of segmentConfigs objects
    {
      "id": "string",
      "segmentID": "string",
      "segmentType": "string",
      "destinationSegmentID": "string",
      "distributionManagerID": "string"
    }
    """
    if v2_distribution_manager_id is None:
        raise Exception('get_segments_from_distribution_manager: v2_distribution_manager_id is required')
    print('getting segments from distribution manager with id: ' + str(v2_distribution_manager_id))
    url = api_config['distribution_manager_segments_url']
    query_params = {'limit': limit}
    segments = []
    while after is not None:
        response = send('GET', url, query_params, path_param_key="{v2DistributionManagerId}",
                        path_param_value=v2_distribution_manager_id, do_lr_auth=True)
        if response is not None:
            segments.extend(response.get("segmentConfigs", []))
            after = response.get("_pagination").get("after")
            query_params['after'] = after
        else:
            print('bad response received in get_segments_from_distribution_manager. '
                  'Not all segments received for ' + str(v2_distribution_manager_id) + '.', file=sys.stderr)
            break
    return segments


def get_distribution_managers(after="", limit=100, integration_connection_id=None):
    """
    Gets a list of distribution managers
    :param after: default None - used for pagination by sending _pagination.after from previous response
    :param limit: default 100
    :param integration_connection_id: the id of the integration connection associated with the distribution manager
    :return: list of distribution managers in "v2/DistributionManagers" field
    {
        "id": "string",
        "name": "string",
        "integrationConnectionID": "string",
        "status": "ACTIVE",
        "expireAt": "2019-04-13",
        "createdAt": "2019-04-13T03:35:34Z",
        "updatedAt": "2019-04-13T03:35:34Z"
    }
    """
    print('getting distribution managers...')
    url = api_config['distribution_managers_url']
    query_params = {'limit': limit}
    if integration_connection_id is not None:
        query_params['integrationConnectionID'] = integration_connection_id
    managers = []
    while after is not None:
        response = send('GET', url, query_params, do_lr_auth=True)
        if response is not None:
            managers.extend(response.get("v2/DistributionManagers", []))
            after = response.get("_pagination").get("after")
            query_params['after'] = after
        else:
            raise Exception('Bad response received in get_distribution_managers. Not all dms received.')
    return managers


def remove_segments_from_distribution_manager(v2_distribution_manager_id, segments):
    """
    Removes segments from a distribution manager
    :param v2_distribution_manager_id: required
    :param segments: required - A list of segment ids
    :return: list of id objects
    {
        "id": "string"
    }
    """
    if v2_distribution_manager_id is None:
        raise Exception('remove_segments_from_distribution_manager: v2_distribution_manager_id is required')
    if segments is None or len(segments) == 0:
        raise Exception('remove_segments_from_distribution_manager: segments is required')
    print('removing segments from distribution manager with id: ' + str(v2_distribution_manager_id))
    url = api_config['distribution_manager_segments_url']
    payload = [{'id': str(segment)} for segment in segments]
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }
    return send('DELETE', url, json_data=payload, path_param_key="{v2DistributionManagerId}",
                path_param_value=v2_distribution_manager_id, request_headers=headers, do_lr_auth=True)


def delete_distribution_manager(distribution_manager_id):
    """
    Deletes a distribution manager
    :param distribution_manager_id: required
    """
    print('deleting distribution manager for id: ' + str(distribution_manager_id))
    if distribution_manager_id is None:
        raise Exception('delete_distribution_manager: distribution_manager_id is required')
    else:
        path_param_key = "{id}"
        path_param_value = distribution_manager_id
    url = api_config['distribution_manager_by_id_url']
    headers = {"accept": "application/json"}
    return send('DELETE', url, path_param_key, path_param_value, request_headers=headers, do_lr_auth=True)
