from unittest.mock import MagicMock

from commands.services.cognito_api import search_users


def _user(*values):
    return {"Attributes": [{"Value": value} for value in values]}


def _stub_session(user_pool_id: str, users: list[dict]) -> MagicMock:
    ssm = MagicMock()
    ssm.get_parameter.return_value = {"Parameter": {"Value": user_pool_id}}
    cognito = MagicMock()
    cognito.list_users.return_value = {"Users": users}
    session = MagicMock()
    session.client.side_effect = lambda name: {"ssm": ssm, "cognito-idp": cognito}[name]
    return session


def test_search_returns_users_matching_all_search_words():
    session = _stub_session("pool-123", [_user("alice", "alice@example.org"), _user("bob")])

    result = search_users(session, "alice example")

    assert result == [_user("alice", "alice@example.org")]


def test_search_returns_none_when_no_match():
    session = _stub_session("pool-123", [_user("alice")])

    assert search_users(session, "missing") is None
