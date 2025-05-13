import sys
import traceback

from python.activation_api.activation_api import get_distribution_managers, get_segments_from_distribution_manager, \
    remove_segments_from_distribution_manager
from python.utils.db_util import execute_query

# TODO: Add functionality to delete empty distribution managers

MAX_SEGMENTS_PER_REQUEST = 100

def remove_provider_from_liveramp_providers(provider_name):
    print("Removing origin provider from automated liveramp updates")
    try:
        execute_query("""
                delete from liveramp.non_restricted_providers
                where provider_name = %s
            """, (provider_name,))
    except Exception as removal_exception:
        print('Error removing provider in integration: %s' % removal_exception, file=sys.stderr)

def get_distribution_managers_by_provider(distribution_manager_name):
    """
    Get distribution managers by provider name

    :param distribution_manager_name: the provider name associated with the distribution managers
    :return: a list of distribution managers
    """
    distribution_managers = get_distribution_managers()
    return [manager for manager in distribution_managers if distribution_manager_name in manager['name']]


def remove_segments_from_distribution(deprecated_cats, distribution_managers=None,
                                      distribution_manager_provider_name=None):
    """
    Remove segments from distribution for deprecated categories.  All distribution managers are searched for segments
    which takes a long time.  The time to remove segments can be increased, by inserting a list of distribution
    managers if they are known.  This is useful for rerunning the script after a failure.  The id and name of the
    distribution manager will be printed to the console if an exception is raised.  Alternatively, the name of the
    provider can be given to remove segments from only those distribution managers containing segments distributed
    by that provider.
    :param deprecated_cats: an array of deprecated categories
    :param distribution_managers:
        Format: [{'integrationConnectionID': 5386061, 'name': 'MNTN_Scanbuy_DM_11'}]
    :param distribution_manager_provider_name: the name of the distribution manager to remove segments from.
        Example: 'Scanbuy'
    :return:
    """
    print('removing segments from distribution for ' + str(len(deprecated_cats)) + ' deprecated categories')
    try:
        if distribution_managers is None and distribution_manager_provider_name is None:
            distribution_managers = get_distribution_managers()
        elif distribution_manager_provider_name is not None:
            distribution_managers = get_distribution_managers_by_provider(distribution_manager_provider_name)
    except Exception as exception:
        traceback.print_exc()
        print('%s' % exception, file=sys.stderr)
    segments_removed_from_distribution = []
    exception_occurred = False
    for distribution_manager in distribution_managers:
        manager_id = int(distribution_manager['integrationConnectionID'])
        manager_name = distribution_manager['name']
        try:
            segments = get_segments_from_distribution_manager(manager_id)
            segment_ids = [int(segment['id']) for segment in segments]
            cats_to_remove = list(set(segment_ids) & set(deprecated_cats))
            if cats_to_remove:
                segment_batches = [cats_to_remove[i:i + MAX_SEGMENTS_PER_REQUEST]
                                   for i in range(0, len(cats_to_remove), MAX_SEGMENTS_PER_REQUEST)]
                for segment_batch in segment_batches:
                    remove_segments_from_distribution_manager(manager_id, segment_batch)
                    segments_removed_from_distribution.extend(segment_batch)
                print('removed ' + str(len(cats_to_remove)) + ' segments from distribution manager '
                      + str(manager_name))
            else:
                print('no segments to remove from distribution manager ' + str(manager_name))
        except Exception as e:
            print('Exception raised while removing segments for distribution manager '
                  + str(manager_name) + ' with id ' + str(manager_id) + ': ' + str(e), file=sys.stderr)
            traceback.print_exc()
            exception_occurred = True
            # TODO save the failed distribution manager and cats to a file to retry
            continue
    print('total segments removed: ' + str(len(segments_removed_from_distribution)))
    if exception_occurred:
        print('The following segments may not have been removed from distribution: '
              + str(list(set(deprecated_cats).difference(set(segments_removed_from_distribution)))))