import boto3
import argparse
import zlib
import json
from datetime import datetime, timedelta
import pytz

UTC=pytz.UTC
OneWeekAgo = UTC.localize(datetime.now() - timedelta(weeks=1))

MetadataKey = 'nva-publication-identifier'


def delete_untagged_files(s3_client, account_id):
    storage_bucket = f'nva-resource-storage-{account_id}'

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=storage_bucket,
        PaginationConfig={'PageSize': 1000}
    )
    evaluated_files = 0
    deleted_files = 0

    # Iterate over the objects

    objects_to_delete = []

    for page in page_iterator:
        for obj in page['Contents']:
            key = obj['Key']
            last_modified = obj['LastModified']

            if last_modified < OneWeekAgo:
                metadata = fetch_metadata(s3_client, account_id, key)

                if MetadataKey not in metadata:
                    objects_to_delete.append({'Key': key})
                    deleted_files = deleted_files + 1

                    if len(objects_to_delete) >= 999:
                        # Delete the object if the metadata key is missing
                        delete_response = s3_client.delete_objects(
                            Bucket=storage_bucket,
                            Delete={'Objects': objects_to_delete})
                        report_delete_response(len(objects_to_delete), delete_response)
                        objects_to_delete.clear()
                        print(f'Deleted 999 files missing metadata key {MetadataKey})')

            evaluated_files = evaluated_files + 1

            if evaluated_files % 100 == 0:
                print(f'Evaluated {evaluated_files} files, deleted {deleted_files}')

    if len(objects_to_delete) > 0:
        s3_client.delete_objects(
            Bucket=storage_bucket,
            Delete={'Objects': objects_to_delete},
            Quiet=True)

    print(f'Evaluated {evaluated_files} files, deleted {deleted_files}')


def report_delete_response(expected_deletes, response):
    deleted = response['Deleted']
    print(f'Deleted {len(deleted)} of {expected_deletes}')

    if 'Errors' in response:
        errors = response['Errors']
        if len(errors) > 0:
            print(errors)


def should_delete_object(obj, metadata):
    last_modified = obj['LastModified']
    return MetadataKey not in metadata and last_modified < OneWeekAgo


def fetch_metadata(s3_client, account_id, key):
    storage_bucket = f'nva-resource-storage-{account_id}'
    return s3_client.head_object(Bucket=storage_bucket,
                                 Key=key)['Metadata']


def tag_referenced_files(dynamo_client, s3_resource, account_id, resources_table_name):
    storage_bucket = f'nva-resource-storage-{account_id}'

    tagged_files = 0
    evaluated_files = 0
    paginator = dynamo_client.get_paginator('scan')
    page_iterator = paginator.paginate(
        TableName=resources_table_name,
        IndexName='ResourcesByIdentifier',
        PaginationConfig={'PageSize': 700}
    )

    for page in page_iterator:

        items = page['Items']

        for item in items:
            data = extract_item_data(item)
            result = json.loads(data)
            identifier = result['identifier']
            if 'entityDescription' in result:
                entity_description = result['entityDescription']
                if 'publicationDate' in entity_description:
                    if 'associatedArtifacts' in result:
                        associated_artifacts = result['associatedArtifacts']
                        for associated_artifact in associated_artifacts:
                            if 'identifier' in associated_artifact:
                                evaluated_files = evaluated_files + 1
                                key = associated_artifact['identifier']
                                tagged_files = tagged_files + update_file_metadata(
                                    s3_resource,
                                    identifier,
                                    key,
                                    storage_bucket)
                                if evaluated_files % 100 == 0:
                                    print(f'Evaluated {evaluated_files} files, '
                                          + f'tagged {tagged_files}')

    print(f'Evaluated {evaluated_files} files, '
          + f'tagged {tagged_files}')


def reset_tags(s3_client, s3_resource, accountId):
    storage_bucket = f'nva-resource-storage-{accountId}'

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=storage_bucket,
        PaginationConfig={'PageSize': 1000}
    )
    count = 0
    for page in page_iterator:
        for obj in page['Contents']:
            key = obj['Key']
            s3_resource.Object(storage_bucket, key).copy_from(
                CopySource={'Bucket': storage_bucket, 'Key': key},
                Metadata={},
                MetadataDirective='REPLACE'
            )
            count = count + 1
        print(f'Reset tags on {count} objects!')
    print('Done!')


def extract_item_data(item):
    gz_data = item['data']
    return zlib.decompress(gz_data['B'], -zlib.MAX_WBITS)


def update_file_metadata(
        s3_resource,
        publication_identifier,
        file_key,
        bucket_name):
    target = s3_resource.Object(bucket_name, file_key)

    if MetadataKey in target.metadata:
        return 0
    else:
        target.metadata.update(
            {'nva-publication-identifier': publication_identifier})

        s3_resource.Object(bucket_name, file_key).copy_from(
            CopySource={'Bucket': bucket_name, 'Key': target.key},
            Metadata=target.metadata,
            MetadataDirective='REPLACE'
        )
        print('Updated metadata for file ' + file_key)
        return 1


if __name__ == '__main__':
    argParser = argparse.ArgumentParser()
    argParser.add_argument("command",
                           choices=[
                               "tag-files",
                               "delete-untagged-files",
                               "reset-tags"])
    argParser.add_argument("resourcesTableName")

    args = argParser.parse_args()

    _dynamodb_client = boto3.client('dynamodb', region_name='eu-west-1')
    _s3_client = boto3.client('s3', region_name='eu-west-1')
    _s3_resource = boto3.resource('s3', region_name='eu-west-1')

    _resources_table_name = args.resourcesTableName
    _session = boto3.Session()
    _sts_client = _session.client('sts')
    _accountId = _sts_client.get_caller_identity()

    if args.command == "tag-files":
        tag_referenced_files(_dynamodb_client, _s3_resource, _accountId, _resources_table_name)
    elif args.command == "delete-untagged-files":
        delete_untagged_files(_s3_client, _accountId)
    elif args.command == "reset-tags":
        reset_tags(_s3_client, _s3_resource, _accountId)
