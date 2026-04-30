import pytest


@pytest.fixture(autouse=True)
def aws_test_credentials(monkeypatch):
    """Block tests from accidentally calling real AWS by forcing dummy credentials.
    moto picks these up; real boto3 would too, so a missing @mock_aws would still
    fail closed instead of charging the wrong account."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
