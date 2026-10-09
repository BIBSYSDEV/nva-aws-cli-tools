import oracledb
import pytest

from commands.services.cristin_db import (
    PROD_DSN,
    PROD_VAULT_PATH,
    TEST_DSN,
    TEST_VAULT_PATH,
    CristinDatabaseError,
    CristinDatabaseService,
    dsn_for,
    extract_credentials,
    vault_path_for,
)

FROM_LOPENR = 123456
TO_LOPENR = 654321
SESSION_ID = 4711

PERSON_COLUMNS = ("PERSONLOPENR", "FORNAVN", "ETTERNAVN", "EPOST")
PERSON_ROWS = {
    FROM_LOPENR: (FROM_LOPENR, "Ola", "Nordmann", "ola@example.no"),
    TO_LOPENR: (TO_LOPENR, "Ola", "Nordmann", "ola@uio.no"),
}


class FakeVariable:
    def __init__(self, value=None):
        self.value = value

    def getvalue(self):
        return self.value


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self._rows = []

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception, traceback):
        return False

    def var(self, _type):
        return FakeVariable()

    def execute(self, statement, **parameters):
        self.connection.statements.append((statement, parameters))
        if "P_Merge_Person" in statement:
            if self.connection.merge_error:
                raise oracledb.DatabaseError(self.connection.merge_error)
            parameters["session_id"].value = SESSION_ID
            self._rows = []
            return
        if "all_tab_columns" in statement:
            self._rows = [(1 if parameters["table_name"] == "ANSETTELSE" else 0,)]
            return
        if "count(*)" in statement:
            self._rows = [(3,)]
            return
        row = PERSON_ROWS.get(parameters["lopenr"])
        self.description = [(column,) for column in PERSON_COLUMNS]
        self._rows = [row] if row else []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def callproc(self, name, arguments):
        if name == "dbms_output.get_line":
            line, status = arguments
            if self.connection.output_lines:
                line.value = self.connection.output_lines.pop(0)
                status.value = 0
            else:
                status.value = 1


class FakeConnection:
    def __init__(self, output_lines=None, merge_error=None):
        self.statements = []
        self.output_lines = list(output_lines or [])
        self.merge_error = merge_error
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def build_service(connection: FakeConnection, profile: str = "sikt-nva-test"):
    return CristinDatabaseService(profile, connection=connection)


def test_profile_decides_environment():
    assert dsn_for("sikt-nva-prod") == PROD_DSN
    assert vault_path_for("sikt-nva-prod") == PROD_VAULT_PATH
    assert dsn_for("sikt-nva-sandbox") == TEST_DSN
    assert vault_path_for(None) == TEST_VAULT_PATH


def test_extract_credentials_accepts_alternative_field_names():
    assert extract_credentials({"user": "frida", "pass": "secret"}) == (
        "frida",
        "secret",
    )


def test_extract_credentials_reports_unknown_fields():
    with pytest.raises(CristinDatabaseError, match="apikey"):
        extract_credentials({"apikey": "nope"})


def test_fetch_person_returns_attributes_and_counts():
    service = build_service(FakeConnection())

    person = service.fetch_person(FROM_LOPENR)

    assert person is not None
    assert person.full_name == "Ola Nordmann"
    assert person.attributes["PERSONLOPENR"] == FROM_LOPENR
    assert "EPOST" not in person.attributes
    assert person.related_counts == {"Ansettelser": 3}


def test_fetch_person_returns_none_when_missing():
    assert build_service(FakeConnection()).fetch_person(999) is None


def test_merge_returns_session_id_and_output():
    connection = FakeConnection(output_lines=["Sessionid: 4711"])
    service = build_service(connection)

    result = service.merge_person(FROM_LOPENR, TO_LOPENR)

    assert result.session_id == SESSION_ID
    assert result.output_lines == ["Sessionid: 4711"]


def test_merge_commits_with_update_flag():
    connection = FakeConnection()
    service = build_service(connection)

    service.merge_person(FROM_LOPENR, TO_LOPENR)

    assert connection.commits == 1
    assert connection.rollbacks == 0
    merge_call = connection.statements[-1][1]
    assert merge_call["update_db"] == 1
    assert merge_call["from_lopenr"] == FROM_LOPENR
    assert merge_call["to_lopenr"] == TO_LOPENR


def test_merge_rolls_back_on_error():
    connection = FakeConnection(merge_error="ORA-20001")
    service = build_service(connection)

    with pytest.raises(CristinDatabaseError, match="ORA-20001"):
        service.merge_person(FROM_LOPENR, TO_LOPENR)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_merging_a_person_into_itself_is_rejected():
    service = build_service(FakeConnection())

    with pytest.raises(CristinDatabaseError, match="den samme"):
        service.merge_person(FROM_LOPENR, FROM_LOPENR)


def test_close_closes_connection():
    connection = FakeConnection()
    with build_service(connection):
        pass

    assert connection.closed is True
