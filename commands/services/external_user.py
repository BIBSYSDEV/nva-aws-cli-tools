import requests
import boto3
import json
from datetime import datetime, timedelta
import argparse

'''
# example of usage

from services import ExternalUserService


customer_id = "bb3d0c0c-5065-4623-9b98-5810983c2478" #sikt in dev
intended_purpose = "handle-migration"

external_user_service = ExternalUserService()
external_user = external_user_service.create(customer_id, intended_purpose)
external_user.save_to_file()
'''
class ExternalUserService:
    def __init__(self):
        self.session = boto3.Session()
        self.ssm = self.session.client('ssm')
        self.secretsmanager = self.session.client('secretsmanager')
        self.api_domain = self._get_system_parameter('/NVA/ApiDomain')
        self.cognito_uri = self._get_system_parameter('/NVA/CognitoUri')
        self.client_credentials = self._get_secret('BackendCognitoClientCredentials')
        self.token = None
        self.token_expiry_time = None

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
        token_expiry_time = datetime.now() + timedelta(seconds=response_json['expires_in'])
        return response_json['access_token'], token_expiry_time

    def _get_token(self):
        if not self.token or self._is_token_expired():
            self.token, self.token_expiry_time = self._get_cognito_token()
        return self.token

    def _is_token_expired(self):
        if not self.token_expiry_time:
            return True
        return datetime.now() > self.token_expiry_time - timedelta(seconds=30)
    
    def _create_external_client_token(self, scopes):
        url = f"https://{self.api_domain}/users-roles/external-clients"
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f"Bearer {self._get_token()}"
        }
        data = {
            "clientName": f"{self.org_abbreviation}-{self.intended_purpose}-integration",
            "customerUri": self.customer_id,
            "cristinOrgUri": self.org_id,
            "actingUser": f"{self.intended_purpose}-integration@{self.org_abbreviation}",
            "scopes": scopes
        }
        response = requests.post(url, headers=headers, json=data)
        response_json = response.json()
        return {
            "clientId": response_json["clientId"],
            "clientSecret": response_json["clientSecret"],
            "tokenUrl": response_json["clientUrl"],
            "clientName": data["clientName"],
            "customerUri": data["customerUri"],
            "cristinOrgUri": data["cristinOrgUri"],
            "actingUser": data["actingUser"],
            "scopes": data["scopes"]
        }
    
    def _get_customer_data(self, customer_id):
        url = f"https://{self.api_domain}/customer/{customer_id}"
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f"Bearer {self._get_token()}"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # raises HTTPError for 4xx and 5xx status codes
        return response.json()

    def create(self, customer_id, intended_purpose, scopes):
            customer_data = self._get_customer_data(customer_id)
            self.org_id = customer_data['cristinId']
            self.customer_id = customer_data['id']
            self.org_abbreviation = customer_data['shortName'].lower()
            self.intended_purpose = intended_purpose
            client_data = self._create_external_client_token(scopes)
            return ExternalUser(self.org_abbreviation, self.intended_purpose, client_data)


class ExternalUser:
    def __init__(self, org_abbreviation, intended_purpose, client_data):
        self.org_abbreviation = org_abbreviation
        self.intended_purpose = intended_purpose
        self.client_data = client_data

    def save_to_file(self):
        with open(f"{self.org_abbreviation}-{self.intended_purpose}-credentials.json", 'w') as json_file:
            json.dump(self.client_data, json_file, indent=4)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--customer_id', required=True, help="Customer ID to be processed")
    parser.add_argument('--intended_purpose', required=True, help="Intended purpose for creating external user")
    parser.add_argument('--scopes', required=True, help="Comma-separated list of scopes")
    args = parser.parse_args()

    customer_id = args.customer_id
    intended_purpose = args.intended_purpose
    scopes = args.scopes.split(',')

    external_user_service = ExternalUserService()
    external_user = external_user_service.create(customer_id, intended_purpose, scopes)
    external_user.save_to_file()