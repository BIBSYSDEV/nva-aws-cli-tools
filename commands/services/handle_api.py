import boto3
import requests
import json
from datetime import datetime, timedelta
'''
# example of usage

from services import HandleApiService

# Initialize the service
service = HandleApiService()

# Define request body for the update and create operations
request_body = {
    "uri": "https://sikt.no?test"
}

# Call the update_handle method
prefix = "11250.1"
suffix = "39053933"
update_response = service.update_handle(prefix, suffix, request_body)
print(update_response)

# Call the create_handle method
create_response = service.create_handle(request_body)
print(create_response)
'''
class HandleApiService:
    def __init__(self, profile):
        self.session = boto3.Session(profile_name=profile)
        self.ssm = self.session.client('ssm')
        self.secretsmanager = self.session.client('secretsmanager')
        self.api_domain = self._get_system_parameter('/NVA/ApiDomain')
        self.cognito_uri = self._get_system_parameter('/NVA/CognitoUri')
        self.client_credentials = self._get_secret('BackendCognitoClientCredentials')
        self.token = self._get_cognito_token()
        self.token_expiry_time = datetime.now()  # Initialize with current time

    def _get_system_parameter(self, name):
        response = self.ssm.get_parameter(Name=name)
        return response['Parameter']['Value']

    def _get_secret(self, name):
        response = self.secretsmanager.get_secret_value(SecretId=name)
        secret_string = response['SecretString']
        secret = json.loads(secret_string)
        return secret

    def _get_cognito_token(self):
        url = f"{self.cognito_uri}/oauth2/token"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_credentials['backendClientId'],
            'client_secret': self.client_credentials['backendClientSecret'],
        }
        response = requests.post(url, headers=headers, data=data)
        response_json = response.json()
        self.token_expiry_time = datetime.now() + timedelta(seconds=response_json['expires_in'])  # Set the expiry time
        return response_json['access_token']

    def _is_token_expired(self):
        # If there are less than 30 seconds until the token expires, consider it expired
        return datetime.now() > self.token_expiry_time - timedelta(seconds=30)

    def _get_token(self):
        if self._is_token_expired():
            self.token = self._get_cognito_token()
        return self.token

    def update_handle(self, prefix, suffix, request_body):
        url = f"https://{self.api_domain}/handle/{prefix}/{suffix}"
        headers = {'Authorization': f"Bearer {self._get_token()}", 'Content-Type': 'application/json'}
        response = requests.put(url, headers=headers, json=request_body)
        return response.json()

    def create_handle(self, request_body):
        url = f"https://{self.api_domain}/handle/"
        headers = {'Authorization': f"Bearer {self._get_token()}", 'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, json=request_body)
        return response.json()