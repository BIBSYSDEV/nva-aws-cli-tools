import boto3
import re


def list_missing_customers(profile):
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    dynamodb = session.resource('dynamodb')

    customers_table = dynamodb.Table(_get_table_name(profile, 'nva-customers'))
    users_table = dynamodb.Table(_get_table_name(profile, 'nva-users-and-roles'))

    customer_identifiers = _extract_customer_identifiers(customers_table)
    missing_customers = _find_missing_customers(users_table, customer_identifiers)

    return missing_customers

def list_duplicate_customers(profile):
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    dynamodb = session.resource('dynamodb')
    customers_table = dynamodb.Table(_get_table_name(profile, 'nva-customers'))

    duplicate_customers = _find_duplicate_customers(customers_table)

    return duplicate_customers

def _find_duplicate_customers(customers_table):
    cristinId_counts = {}
    matching_items = []
    customers_response = _scan_table(customers_table)

    for item in customers_response:
        if 'cristinId' in item:
            # Extract the first number from the cristinId
            match = re.search(r'\d+', item['cristinId'])
            if match:
                first_number = match.group()
                cristinId_counts[first_number] = cristinId_counts.get(first_number, 0) + 1
                if cristinId_counts[first_number] >= 2:
                    matching_items.append(item)

    return matching_items

def _scan_table(table):
    items = []
    response = table.scan()
    items.extend(response['Items'])

    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])

    return items

def _extract_customer_identifiers(customers_table):
    customer_identifiers = set()
    customers_response = _scan_table(customers_table)
    for customer in customers_response:
        customer_identifiers.add(customer['identifier'])
    return customer_identifiers

def _find_missing_customers(users_table, customer_identifiers):
    missing_customers = []
    users_response = _scan_table(users_table)

    for user in users_response:
        if 'institution' in user:
            # Extract the customer identifier from the institution
            match = re.search(r'(?<=customer/).+', user['institution'])
            if match:
                customer_id = match.group()
                if customer_id not in customer_identifiers:
                    missing_customers.append({
                        'PrimaryKeyHashKey': user['PrimaryKeyHashKey'],
                        'MissingCustomerId': customer_id
                    })

    return missing_customers

def _get_table_name(profile, name):
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    dynamodb = session.client('dynamodb')
    response = dynamodb.list_tables()
    
    for table_name in response['TableNames']:
        if table_name.startswith(name):
            return table_name
    
    raise ValueError('No valid table found.')