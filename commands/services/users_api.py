from urllib.parse import quote_plus
import boto3
import requests
import json
from datetime import datetime, timedelta, timezone


class UsersAndRolesService:
    system_user = "nva-backend@20754.0.0.0"

    def __init__(self, profile, client_id=None, client_secret=None):
        session = boto3.Session(profile_name=profile) if profile else boto3.Session()
        self.ssm = session.client("ssm")
        self.dynamodb = session.resource("dynamodb")
        self.api_domain = self._get_system_parameter("/NVA/ApiDomain")
        self.secretsmanager = session.client("secretsmanager")
        self.api_domain = self._get_system_parameter("/NVA/ApiDomain")
        self.cognito_uri = self._get_system_parameter("/NVA/CognitoUri")
        if client_id and client_secret:
            self.client_credentials = {
                "backendClientId": client_id,
                "backendClientSecret": client_secret,
            }
        else:
            self.client_credentials = self._get_secret(
                "BackendCognitoClientCredentials"
            )
        self.token = self._get_cognito_token()
        self.token_expiry_time = datetime.now()  # Initialize with current time

    def approve_terms(self, person_id):
        table_name = self._get_terms_table_name()
        table = self.dynamodb.Table(table_name)

        http_client = requests.Session()
        response = http_client.get(
            f"https://{self.api_domain}/users-roles/terms-and-conditions/current"
        )
        if response.status_code != 200:
            raise ValueError(
                f"Failed to retrieve current terms and conditions URI. Status code: {response.status_code} - {response.text}"
            )

        terms_conditions_uri = response.json().get("termsConditionsUri")
        if not terms_conditions_uri:
            raise ValueError("Current terms and conditions URI not found.")
        now_utc = datetime.now(timezone.utc)
        timestamp_str = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f000Z")

        item = {
            "id": f"https://{self.api_domain}/cristin/person/{person_id}",
            "type": "TermsConditions",
            "created": timestamp_str,
            "modified": timestamp_str,
            "modifiedBy": self.system_user,
            "owner": self.system_user,
            "termsConditionsUri": terms_conditions_uri,
        }

        table.put_item(Item=item)
        return item

    def search(self, search_term):
        table_name = self._get_users_table_name()
        table = self.dynamodb.Table(table_name)

        # Split search term into individual words
        search_words = search_term.split()

        # Initialize scan operation
        response = table.scan()

        # Collect all items that match the value
        matching_items = []

        while "LastEvaluatedKey" in response:
            matching_items.extend(self._items_search(response["Items"], search_words))
            # Paginate results
            response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])

        # Don't forget to process the last page of results
        matching_items.extend(self._items_search(response["Items"], search_words))

        return matching_items

    def add_user(self, person):
        """
        POST https://api.dev.nva.aws.unit.no/users-roles/users

        payload:
        {
        "cristinIdentifier": "34322",
        "customerId": "https://api.dev.nva.aws.unit.no/customer/bb3d0c0c-5065-4623-9b98-5810983c2478",
        "roles": [{ "type": "Role", "rolename": "Creator" }],
        "viewingScope": { "type": "ViewingScope", "includedUnits": [] }
        }
        """
        http_client = requests.Session()
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }
        response = http_client.post(
            f"https://{self.api_domain}/users-roles/users", json=person, headers=headers
        )
        if not response.ok:
            raise ValueError(
                f"Failed to create user. Status code: {response.status_code} - {response.text}"
            )

        return response.json()

    def update_user(self, user):
        http_client = requests.Session()
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }
        response = http_client.put(
            f"https://{self.api_domain}/users-roles/users/{quote_plus(user['username'])}",
            json=user,
            headers=headers,
        )
        if not response.ok:
            raise ValueError(
                f"Failed to update user. Status code: {response.status_code} - {response.text}"
            )

        return response.json()

    def get_user_by_username(self, username):
        http_client = requests.Session()
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Accept": "application/json",
        }
        response = http_client.get(
            f"https://{self.api_domain}/users-roles/users/{quote_plus(username)}",
            headers=headers,
        )
        if response.status_code != 200:
            raise ValueError(
                f"Failed to retrieve user by username. Status code: {response.status_code} - {response.text}"
            )

        return response.json()

    def _items_search(self, items, search_words):
        matching_items = []
        for item in items:
            item_values = " ".join(str(value) for value in item.values())
            if all(word in item_values for word in search_words):
                matching_items.append(item)
        return matching_items

    def _get_system_parameter(self, name):
        response = self.ssm.get_parameter(Name=name)
        return response["Parameter"]["Value"]

    def _get_users_table_name(self):
        response = self.dynamodb.meta.client.list_tables()

        for table_name in response["TableNames"]:
            if table_name.startswith("nva-users-and-roles"):
                return table_name

        raise ValueError("No valid table found.")

    def _get_terms_table_name(self):
        response = self.dynamodb.meta.client.list_tables()

        for table_name in response["TableNames"]:
            if table_name.startswith("terms-and-conditions"):
                return table_name

        raise ValueError("No valid table found.")

    def _get_system_parameter(self, name):
        response = self.ssm.get_parameter(Name=name)
        return response["Parameter"]["Value"]

    def _get_secret(self, name):
        response = self.secretsmanager.get_secret_value(SecretId=name)
        secret_string = response["SecretString"]
        secret = json.loads(secret_string)
        return secret

    def _get_cognito_token(self):
        url = f"{self.cognito_uri}/oauth2/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_credentials["backendClientId"],
            "client_secret": self.client_credentials["backendClientSecret"],
        }
        response = requests.post(url, headers=headers, data=data)
        response_json = response.json()
        self.token_expiry_time = datetime.now() + timedelta(
            seconds=response_json["expires_in"]
        )  # Set the expiry time
        return response_json["access_token"]

    def _is_token_expired(self):
        # If there are less than 30 seconds until the token expires, consider it expired
        return datetime.now() > self.token_expiry_time - timedelta(seconds=30)

    def _get_token(self):
        if self._is_token_expired():
            self.token = self._get_cognito_token()
        return self.token
