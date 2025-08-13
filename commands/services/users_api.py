import boto3
import datetime
import requests


class UsersAndRolesService:
    def __init__(self, profile):
        self.profile = profile
        session = (
            boto3.Session(profile_name=self.profile)
            if self.profile
            else boto3.Session()
        )
        self.ssm = session.client("ssm")
        self.dynamodb = session.resource("dynamodb")
        self.api_domain = self._get_system_parameter("/NVA/ApiDomain")

    def approve_terms(self, person_id):
        table_name = self._get_terms_table_name()
        table = self.dynamodb.Table(table_name)

        http_client = requests.Session()
        response = http_client.get(
            f"https://{self.api_domain}/users-roles/terms-and-conditions/current"
        )
        if response.status_code != 200:
            raise ValueError("Failed to retrieve current terms and conditions URI.")

        terms_conditions_uri = response.json().get("termsConditionsUri")
        if not terms_conditions_uri:
            raise ValueError("Current terms and conditions URI not found.")
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        timestamp_str = (
            now_utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now_utc.microsecond:06d}000Z"
        )

        item = {
            "id": f"https://{self.api_domain}/cristin/person/{person_id}",
            "type": "TermsConditions",
            "created": timestamp_str,
            "modified": timestamp_str,
            "modifiedBy": "nva-backend@20754.0.0.0",
            "owner": "nva-backend@20754.0.0.0",
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
