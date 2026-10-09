import oracledb
import pytest

from commands.services.cristin_db import (
    DEFAULT_DB_USER,
    MERGE_PERSON_STATEMENT,
    PERSON_KEY_COLUMN,
    PERSON_TABLE,
    PROD_DSN,
    PROD_VAULT_PATH,
    RELATED_COUNT_TABLES,
    SCHEMA,
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
ROW_COUNT = 3
PROD_PROFILE = "a-profile-named-prod"
NON_PROD_PROFILE = "a-profile-named-anything-else"

PERSON_COLUMNS = ("PERSONLOPENR", "FORNAVN", "ETTERNAVN", "EPOST")
PERSON_ROWS = {
    FROM_LOPENR: (FROM_LOPENR, "Ola", "Nordmann", "ola@example.no"),
    TO_LOPENR: (TO_LOPENR, "Ola", "Nordmann", "ola@uio.no"),
}

KNOWN_TABLES = ("ANSETTELSE",)
PERSON_LOOKUP_STATEMENT = (
    f"select * from {SCHEMA}.{PERSON_TABLE} where {PERSON_KEY_COLUMN} = :lopenr"
)
COLUMN_LOOKUP_STATEMENT = (
    "select count(*) from all_tab_columns "
    "where owner = :owner and table_name = :table_name and column_name = :column_name"
)
COUNT_STATEMENTS = {
    f"select count(*) from {SCHEMA}.{table_name} where {PERSON_KEY_COLUMN} = :lopenr"
    for _, table_name in RELATED_COUNT_TABLES
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

    def var(self, _type, size=None):
        return FakeVariable()

    def execute(self, statement, **parameters):
        self.connection.statements.append((statement, parameters))
        self._rows = self._rows_for(statement, parameters)

    def _rows_for(self, statement, parameters):
        if statement == MERGE_PERSON_STATEMENT:
            if self.connection.merge_error:
                raise oracledb.DatabaseError(self.connection.merge_error)
            parameters["session_id"].value = SESSION_ID
            return []
        if statement == COLUMN_LOOKUP_STATEMENT:
            return [(1 if parameters["table_name"] in KNOWN_TABLES else 0,)]
        if statement in COUNT_STATEMENTS:
            return [(ROW_COUNT,)]
        if statement != PERSON_LOOKUP_STATEMENT:
            raise AssertionError(f"Unexpected statement: {statement}")
        if self.connection.lookup_error:
            raise oracledb.DatabaseError(self.connection.lookup_error)
        self.description = [(column,) for column in PERSON_COLUMNS]
        row = PERSON_ROWS.get(parameters["lopenr"])
        return [row] if row else []

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def callproc(self, name, arguments):
        self.connection.procedures.append(name)
        if name == "dbms_output.get_line":
            if self.connection.output_failure:
                raise self.connection.output_failure
            line, status = arguments
            if self.connection.output_lines:
                line.value = self.connection.output_lines.pop(0)
                status.value = 0
            else:
                status.value = 1


class FakeConnection:
    def __init__(self, output_lines=None, merge_error=None):
        self.statements = []
        self.procedures = []
        self.output_lines = list(output_lines or [])
        self.merge_error = merge_error
        self.output_failure = None
        self.lookup_error = None
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


def build_service(connection: FakeConnection, profile: str = NON_PROD_PROFILE):
    return CristinDatabaseService(profile, connection=connection)


def test_profile_decides_environment():
    assert dsn_for(PROD_PROFILE) == PROD_DSN
    assert vault_path_for(PROD_PROFILE) == PROD_VAULT_PATH
    assert dsn_for(NON_PROD_PROFILE) == TEST_DSN
    assert vault_path_for(None) == TEST_VAULT_PATH


def test_extract_credentials_reads_the_password_of_the_default_user():
    secret = {"FRIDA": "frida-password", "FRIDA_SYSTEM": "system-password"}

    assert extract_credentials(secret) == (DEFAULT_DB_USER, "frida-password")


def test_extract_credentials_reads_the_password_of_the_requested_user():
    secret = {"FRIDA": "frida-password", "FRIDA_SYSTEM": "system-password"}

    assert extract_credentials(secret, "frida_system") == (
        "frida_system",
        "system-password",
    )


def test_extract_credentials_lists_the_users_it_found_when_asked_for_another():
    with pytest.raises(CristinDatabaseError, match="FRIDA_SYSTEM"):
        extract_credentials({"FRIDA_SYSTEM": "system-password"}, "NOBODY")


def test_extract_credentials_rejects_an_empty_secret():
    with pytest.raises(CristinDatabaseError, match="no database users"):
        extract_credentials({})


def test_fetch_person_returns_attributes_and_counts():
    service = build_service(FakeConnection())

    person = service.fetch_person(FROM_LOPENR)

    assert person is not None
    assert person.full_name == "Ola Nordmann"
    assert person.attributes["PERSONLOPENR"] == FROM_LOPENR
    assert "EPOST" not in person.attributes
    assert person.related_counts == {"Ansettelser": ROW_COUNT}


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


def test_merge_enables_and_reads_output_before_committing():
    connection = FakeConnection(output_lines=["Sessionid: 4711"])
    service = build_service(connection)

    service.merge_person(FROM_LOPENR, TO_LOPENR)

    assert connection.procedures[0] == "dbms_output.enable"
    assert "dbms_output.get_line" in connection.procedures
    assert connection.commits == 1


def test_merge_rolls_back_on_error():
    connection = FakeConnection(merge_error="ORA-20001")
    service = build_service(connection)

    with pytest.raises(CristinDatabaseError, match="ORA-20001"):
        service.merge_person(FROM_LOPENR, TO_LOPENR)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_merge_rolls_back_when_reading_output_fails():
    connection = FakeConnection()
    service = build_service(connection)
    connection.output_failure = RuntimeError("connection lost")

    with pytest.raises(RuntimeError):
        service.merge_person(FROM_LOPENR, TO_LOPENR)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_fetch_person_translates_oracle_errors():
    connection = FakeConnection()
    service = build_service(connection)
    connection.lookup_error = "ORA-00942: table or view does not exist"

    with pytest.raises(CristinDatabaseError, match="ORA-00942"):
        service.fetch_person(FROM_LOPENR)


def test_merging_a_person_into_itself_is_rejected():
    service = build_service(FakeConnection())

    with pytest.raises(CristinDatabaseError, match="den samme"):
        service.merge_person(FROM_LOPENR, FROM_LOPENR)


def test_close_closes_connection():
    connection = FakeConnection()
    with build_service(connection):
        pass

    assert connection.closed is True
