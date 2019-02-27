from datetime import datetime
import ckanapi
from dateutil import parser

import traceback
from leash import fill_bowl, empty_bowl


def get_metadata(site,resource_id,API_key=None):
    metadata = ckan.action.resource_show(id=resource_id)

    return metadata

def get_package_parameter(site,package_id,parameter=None,API_key=None):
    """Gets a CKAN package parameter. If no parameter is specified, all metadata
    for that package is returned."""
    # Some package parameters you can fetch from the WPRDC with
    # this function are:
    # 'geographic_unit', 'owner_org', 'maintainer', 'data_steward_email',
    # 'relationships_as_object', 'access_level_comment',
    # 'frequency_publishing', 'maintainer_email', 'num_tags', 'id',
    # 'metadata_created', 'group', 'metadata_modified', 'author',
    # 'author_email', 'state', 'version', 'department', 'license_id',
    # 'type', 'resources', 'num_resources', 'data_steward_name', 'tags',
    # 'title', 'frequency_data_change', 'private', 'groups',
    # 'creator_user_id', 'relationships_as_subject', 'data_notes',
    # 'name', 'isopen', 'url', 'notes', 'license_title',
    # 'temporal_coverage', 'related_documents', 'license_url',
    # 'organization', 'revision_id'
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = ckan.action.package_show(id=package_id)
        if parameter is None:
            return metadata
        else:
            return metadata[parameter]
    except:
        raise RuntimeError("Unable to obtain package parameter '{}' for package with ID {}".format(parameter,package_id))

def set_package_parameters_to_values(site,package_id,parameters,new_values,API_key):
    success = False
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        original_values = [get_package_parameter(site,package_id,p,API_key) for p in parameters]
        payload = {}
        payload['id'] = package_id
        for parameter,new_value in zip(parameters,new_values):
            payload[parameter] = new_value
        results = ckan.action.package_patch(**payload)
        #print(results)
        print("Changed the parameters {} from {} to {} on package {}".format(parameters, original_values, new_values, package_id))
        success = True
    except:
        success = False
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print("Error: {}".format(exc_type))
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        print(''.join('!!! ' + line for line in lines))

    return success

def query_resource(site,query,API_key=None):
    # Use the datastore_search_sql API endpoint to query a CKAN resource.

    # Note that this doesn't work for private datasets.
    # The relevant CKAN GitHub issue has been closed.
    # https://github.com/ckan/ckan/issues/1954
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    response = ckan.action.datastore_search_sql(sql=query)
    # A typical response is a dictionary like this
    #{u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'_full_text', u'type': u'tsvector'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'records': [{u'_full_text': u"'0001b00010000000':1 '11':2 '13585.47':3",
    #               u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_full_text': u"'0001c00058000000':3 '2':2 '7827.64':1",
    #               u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_full_text': u"'0001c01661006700':3 '1':1 '3233.59':2",
    #               u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}]
    # u'sql': u'SELECT * FROM "d1e80180-5b2e-4dab-8ec3-be621628649e" LIMIT 3'}
    data = response['records']
    return data


def find_extremes(resource_id,field):
    from credentials import site, ckan_api_key as API_key

    fill_bowl(resource_id)
    query = 'SELECT min({}) AS smallest, max({}) as biggest FROM "{}" LIMIT 1'.format(field,field,resource_id)
    record = query_resource(site=site, query=query, API_key=API_key)[0]
    empty_bowl(resource_id)
    return record['smallest'], record['biggest']


def fix_temporal_coverage(package_id):
    from credentials import site, ckan_api_key as API_key

    parameter = "temporal_coverage"
    inital_value = get_package_parameter(site,package_id,parameter=parameter,API_key=API_key)
    print("Initial temporal coverage = {}".format(initial_value))
    # Find all resources in package that have datastores.
    very_first = datetime(3000,4,13)
    very_last = datetime(1000,5,14)
    time_field = 'start'
    resources = get_package_parameter(site,package_id,'resources',API_key)
    for r in resources:
        if r['datastore_active']:
            resource_id = r['id']
            first, last = find_extremes(resource_id,time_field)
            first = parser.parse(first)
            last = parser.parse(last)
            if first < very_first:
                very_first = first
            if last > very_last:
                very_last = last

    temporal_coverage = "{}/{}".format(very_first.date(),very_last.date())
    print("New temporal coverage for {} = {}".format(package_id,temporal_coverage))
    # Alter metadata for package
    if initial_value != temporal_coverage:
        set_package_parameters_to_values(site,package_id,[parameter],[temporal_coverage],API_key)
    else:
        print("No update needed. (Existing temporal coverage matches current temporal coverage.)")

    # [ ] Maybe change very_last to an empty string if it is reasonably close to the present.
from credentials import transactions_package_id
fix_temporal_coverage(transactions_package_id)

