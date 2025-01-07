import boto3

class CognitoService:
    def __init__(self, profile):
        self.profile = profile
        pass

    def search(self, search_term):
        user_pool_id = self._get_user_pool_id()
        users = self._get_all_users(user_pool_id)
        return self._lookup_users_by_attribute_value(search_term, users)

    def _get_user_pool_id(self):
        session = boto3.Session(profile_name=self.profile) if self.profile else boto3.Session()
        client = session.client('ssm')

        parameter_name = 'CognitoUserPoolId'

        response = client.get_parameter(
            Name=parameter_name,
            WithDecryption=True
        )

        return response['Parameter']['Value']
    
    def _get_all_users(self, user_pool_id):
        session = boto3.Session(profile_name=self.profile) if self.profile else boto3.Session()
        cognito = session.client('cognito-idp')
        
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
    
    def _lookup_users_by_attribute_value(self, search_term, users):
        search_words = search_term.split()
        matches = []

        for user in users:
            user_attributes = ' '.join(attribute['Value'] for attribute in user['Attributes'])
            if all(word in user_attributes for word in search_words):
                matches.append(user)

        return matches if matches else None