import boto3

class UsersAndRolesService:
    def __init__(self, profile):
        self.profile = profile
        pass

    def search(self, search_term):
        table_name = self._get_table_name()
        session = boto3.Session(profile_name=self.profile) if self.profile else boto3.Session()
        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table(table_name)

        # Split search term into individual words
        search_words = search_term.split()

        # Initialize scan operation
        response = table.scan()

        # Collect all items that match the value
        matching_items = []

        while 'LastEvaluatedKey' in response:
            matching_items.extend(self._items_search(response['Items'], search_words))
            # Paginate results
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])

        # Don't forget to process the last page of results
        matching_items.extend(self._items_search(response['Items'], search_words))

        return matching_items
    
    def _items_search(self, items, search_words):
        matching_items = []
        for item in items:
            item_values = ' '.join(str(value) for value in item.values())
            if all(word in item_values for word in search_words):
                matching_items.append(item)
        return matching_items
    
    def _get_table_name(self):
        session = boto3.Session(profile_name=self.profile) if self.profile else boto3.Session()
        dynamodb = session.client('dynamodb')
        response = dynamodb.list_tables()
        
        for table_name in response['TableNames']:
            if table_name.startswith('nva-users-and-roles'):
                return table_name
        
        raise ValueError('No valid table found.')