import boto3
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
import sys
import json

def get_table_name():
    dynamodb = boto3.client('dynamodb')
    response = dynamodb.list_tables()
    
    for table_name in response['TableNames']:
        if table_name.startswith('nva-users-and-roles'):
            return table_name
    
    raise ValueError('No valid table found.')

def get_user_pool_id():
    client = boto3.client('ssm')
    parameter_name = 'CognitoUserPoolId'

    response = client.get_parameter(
        Name=parameter_name,
        WithDecryption=True
    )

    return response['Parameter']['Value']

def get_user_roles(key):
    table_name = get_table_name()

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    response = table.get_item(
        Key={
            'PrimaryKeyHashKey': key,
            'PrimaryKeyRangeKey': key
        }
    )

    if 'Item' not in response:
        print(f"No item found with the key: {key}")
        return None

    item = response['Item']
    roles = item.get('roles')

    if roles is None:
        print(f"No 'roles' found for the key: {key}")
        return None

    return roles

def get_all_users(user_pool_id):
    cognito = boto3.client('cognito-idp')
    pagination_token = None
    users = []

    while True:
        if pagination_token:
            response = cognito.list_users(UserPoolId=user_pool_id, PaginationToken=pagination_token)
        else:
            response = cognito.list_users(UserPoolId=user_pool_id)

        users.extend(response['Users'])

        pagination_token = response.get('PaginationToken')
        if not pagination_token:
            break

    return users

def lookup_user_by_custom_attr(nvaUsername, users):
    for user in users:
        for attribute in user['Attributes']:
            if attribute['Name'] == 'custom:nvaUsername' and attribute['Value'] == nvaUsername:
                return user['Username']

    return None

def lookup_users_by_attribute_value(attribute_value, users):
    matches = []
    for user in users:
        for attribute in user['Attributes']:
            if attribute['Value'] == attribute_value:
                matches.append(user)
    return matches if matches else None

def load_roles_from_file(filename):
    with open(filename, 'r') as f:
        roles = json.load(f)
    
    deserializer = TypeDeserializer()
    roles_python = [deserializer.deserialize(role) for role in roles['L']]

    return roles_python

def get_key_name_fields(items):
    user_pool_id = get_user_pool_id()
    users = get_all_users(user_pool_id)
    key_name_fields = [
        {
            'PrimaryKeyHashKey': item.get('PrimaryKeyHashKey'),
            'PrimaryKeyRangeKey': item.get('PrimaryKeyRangeKey'),
            'givenName': item.get('givenName'),
            'familyName': item.get('familyName'),
            'cognitoUsername':  lookup_user_by_custom_attr(item.get('username'), users)
        }
        for item in items
    ]

    return key_name_fields
def write_roles_to_db(roles, key):
    table_name = get_table_name()

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    response = table.update_item(
        Key={
            'PrimaryKeyHashKey': key,
            'PrimaryKeyRangeKey': key
        },
        UpdateExpression="set #rl = :r",
        ExpressionAttributeNames={
            "#rl": "roles"
        },
        ExpressionAttributeValues={
            ':r': roles
        },
        ReturnValues="UPDATED_NEW"
    )

def lookup(value):
    table_name = get_table_name()
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    # Initialize scan operation
    response = table.scan()

    # Collect all items that match the value
    matching_items = []

    while 'LastEvaluatedKey' in response:
        for item in response['Items']:
            for attribute_value in item.values():
                if isinstance(attribute_value, str) and value in attribute_value:
                    matching_items.append(item)

        # Paginate results
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])

    # Don't forget to process the last page of results
    for item in response['Items']:
        for attribute_value in item.values():
            if isinstance(attribute_value, str) and value in attribute_value:
                matching_items.append(item)

    return matching_items

def help():
    instructions = """
    Please, specify one of the following actions:

    - 'read' to get user roles. 
      Use it as: python3 roles.py read [key] > output.json
      This will return a JSON of roles for a given key. The output is redirected to output.json file.

    - 'write' to write roles from a file to a user. 
      Use it as: python3 roles.py write [key] [filename]
      This will read a JSON file of roles and write them to a user defined by the key. 
      The filename should be a JSON file with the roles.

    - 'lookup' to lookup a value in the whole table. 
      Use it as: python3 roles.py lookup [value]
      It will return a list of items where at least one attribute contains the given value. 
      The items are returned as a list of dictionaries with 'PrimaryKeyHashKey', 'givenName', 
      and 'familyName' as keys.

    - 'clookup' to perform a lookup directly in cognito user pool. 
      Use it as: python3 roles.py clookup [value]
      This will return a list of users where at least one attribute contains the given value. 
      The users are returned as a serialized JSON string.
      
      
    - 'help' to see this message again.
    """
    print(instructions)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Please provide the action(1)")
        sys.exit(1)

    action = sys.argv[1]

    if action == 'read':
        if len(sys.argv) < 3:
            print("Please provide the key(2)")
            sys.exit(1)
        key = sys.argv[2]
        roles = get_user_roles(key)

        if roles is not None:
            serializer = TypeSerializer()
            roles_dynamodb = serializer.serialize(roles)

            print(json.dumps(roles_dynamodb, indent=2, sort_keys=True, default=str, ensure_ascii=False))

    elif action == 'write':
        if len(sys.argv) < 4:
            print("Please provide the key(2) and the filename(3)")
            sys.exit(1)
        key = sys.argv[2]
        filename = sys.argv[3]
        roles = load_roles_from_file(filename)

        write_roles_to_db(roles, key)

    elif action == 'lookup':
        if len(sys.argv) < 3:
            print("Please provide the value to lookup(2)")
            sys.exit(1)

        value = sys.argv[2]
        items = lookup(value)

        names = get_key_name_fields(items)

        
        print(json.dumps(names, indent=2, sort_keys=True, default=str, ensure_ascii=False))
    
    elif action == 'clookup':
        if len(sys.argv) < 3:
            print("Please provide the value to lookup(2)")
            sys.exit(1)

        value = sys.argv[2]

        user_pool_id = get_user_pool_id()
        users = get_all_users(user_pool_id)
        result = lookup_users_by_attribute_value(value, users)

        
        print(json.dumps(result, indent=2, sort_keys=True, default=str, ensure_ascii=False))

    elif action == 'help':
        help()

    else:
        print("Invalid action. Please specify either 'read', 'write', 'lookup', or 'help'.")