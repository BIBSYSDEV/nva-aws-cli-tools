import boto3
import re

class CustomersService:
    def __init__(self, profile):
        self.profile = profile
        pass

    def search_missing_customers(self):
        session = boto3.Session(profile_name=self.profile) if self.profile else boto3.Session()
        dynamodb = session.resource('dynamodb')
        customers_table = dynamodb.Table(self._get_table_name('nva-customers'))
        users_table = dynamodb.Table(self._get_table_name('nva-users-and-roles'))

        customer_identifiers = self._extract_customer_identifiers(customers_table)
        missing_customers = self._find_missing_customers(users_table, customer_identifiers)

        return missing_customers

    def search_duplicate_customers(self):
        session = boto3.Session(profile_name=self.profile) if self.profile else boto3.Session()
        dynamodb = session.resource('dynamodb')
        customers_table = dynamodb.Table(self._get_table_name('nva-customers'))

        duplicate_customers = self._find_duplicate_customers(customers_table)

        return duplicate_customers
    
    def _find_duplicate_customers(self, customers_table):
        cristinId_counts = {}
        matching_items = []
        customers_response = self._scan_table(customers_table)

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

    def _scan_table(self, table):
        items = []
        response = table.scan()
        items.extend(response['Items'])

        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])

        return items

    def _extract_customer_identifiers(self, customers_table):
        customer_identifiers = set()
        customers_response = self._scan_table(customers_table)
        for customer in customers_response:
            customer_identifiers.add(customer['identifier'])
        return customer_identifiers

    def _find_missing_customers(self, users_table, customer_identifiers):
        missing_customers = []
        users_response = self._scan_table(users_table)

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
    
    def _get_table_name(self, name):
        session = boto3.Session(profile_name=self.profile) if self.profile else boto3.Session()
        dynamodb = session.client('dynamodb')
        response = dynamodb.list_tables()
        
        for table_name in response['TableNames']:
            if table_name.startswith(name):
                return table_name
        
        raise ValueError('No valid table found.')

    def _items_search(self, items, search_words):
        matching_items = []
        for item in items:
            item_values = ' '.join(str(value) for value in item.values())
            if all(word in item_values for word in search_words):
                matching_items.append(item)
        return matching_items