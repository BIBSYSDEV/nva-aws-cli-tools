"""Pattern: use `moto` for AWS-interacting code.

`@mock_aws` intercepts boto3 calls and serves them from an in-memory simulator,
so the test exercises the real boto3 API surface (method names, parameter
shapes, pagination). Wrong call signatures fail here, unlike with hand-stubbed
mocks. Reach for this whenever the code under test only talks to AWS."""

import boto3
from moto import mock_aws

from commands.services.cognito_api import USER_POOL_ID_PARAMETER, search_users


def _seed_user_pool(session: boto3.Session, users: list[dict]) -> str:
    cognito = session.client("cognito-idp")
    pool_id = cognito.create_user_pool(PoolName="nva-test")["UserPool"]["Id"]
    for user in users:
        cognito.admin_create_user(
            UserPoolId=pool_id,
            Username=user["username"],
            UserAttributes=[{"Name": name, "Value": value} for name, value in user["attributes"].items()],
        )
    session.client("ssm").put_parameter(Name=USER_POOL_ID_PARAMETER, Value=pool_id, Type="String")
    return pool_id


@mock_aws
def test_search_returns_users_matching_all_search_words():
    session = boto3.Session()
    _seed_user_pool(session, [
        {"username": "alice", "attributes": {"email": "alice@example.org"}},
        {"username": "bob", "attributes": {"email": "bob@other.org"}},
    ])

    result = search_users(session, "alice example")

    assert len(result) == 1
    assert result[0]["Username"] == "alice"


@mock_aws
def test_search_returns_none_when_no_match():
    session = boto3.Session()
    _seed_user_pool(session, [{"username": "alice", "attributes": {"email": "alice@example.org"}}])

    assert search_users(session, "missing") is None
