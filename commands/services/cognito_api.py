import boto3

USER_POOL_ID_PARAMETER = "CognitoUserPoolId"


def search_users(session: boto3.Session, search_term: str) -> list[dict] | None:
    user_pool_id = _get_user_pool_id(session)
    users = _list_all_users(session, user_pool_id)
    return _filter_by_attribute_value(users, search_term)


def _get_user_pool_id(session: boto3.Session) -> str:
    ssm = session.client("ssm")
    response = ssm.get_parameter(Name=USER_POOL_ID_PARAMETER, WithDecryption=True)
    return response["Parameter"]["Value"]


def _list_all_users(session: boto3.Session, user_pool_id: str) -> list[dict]:
    cognito = session.client("cognito-idp")
    users: list[dict] = []
    pagination_token: str | None = None
    while True:
        kwargs: dict = {"UserPoolId": user_pool_id}
        if pagination_token:
            kwargs["PaginationToken"] = pagination_token
        response = cognito.list_users(**kwargs)
        users.extend(response["Users"])
        pagination_token = response.get("PaginationToken")
        if not pagination_token:
            return users


def _filter_by_attribute_value(users: list[dict], search_term: str) -> list[dict] | None:
    search_words = search_term.split()
    matches = [
        user
        for user in users
        if all(
            word in " ".join(attribute["Value"] for attribute in user["Attributes"])
            for word in search_words
        )
    ]
    return matches if matches else None
