from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Self

import oracledb

from commands.services.vault import VaultClient, VaultError

logger = logging.getLogger(__name__)

PROD_DSN = "cmanora-prod05.uio.no:5435/CRISPRD.uio.no"
TEST_DSN = "cmanora-prod05.uio.no:5434/CRISTST.uio.no"

PROD_VAULT_PATH = "service/cristin/database/prod"
TEST_VAULT_PATH = "service/cristin/database/test"

USERNAME_KEYS = ("username", "user", "login")
PASSWORD_KEYS = ("password", "pass", "passwd")

SCHEMA = "FRIDA"
PERSON_TABLE = "PERSON"
PERSON_KEY_COLUMN = "PERSONLOPENR"

PERSON_COLUMNS_OF_INTEREST = (
    "PERSONLOPENR",
    "FORNAVN",
    "ETTERNAVN",
    "FODSELSDATO",
    "PERSONNR",
    "STATUS_AKTIV",
    "DATO_OPPRETTET",
    "DATO_ENDRET",
)

RELATED_COUNT_TABLES = (
    ("Ansettelser", "ANSETTELSE"),
    ("Resultater", "VARBEID_PERSON"),
    ("Prosjekter", "PRESENTASJON_PERSON"),
)

MERGE_PERSON_STATEMENT = """
begin
  PK_FDS200010.P_Merge_Person(
    inOppdDB     => :update_db,
    inVLopenrFra => :from_lopenr,
    inVLopenrTil => :to_lopenr,
    outSid       => :session_id);
end;
"""

DBMS_OUTPUT_BUFFER_SIZE = 1_000_000
DBMS_OUTPUT_LINE_SIZE = 32767
UPDATE_DATABASE = 1


class CristinDatabaseError(Exception):
    pass


@dataclass
class PersonProfile:
    lopenr: int
    attributes: dict[str, Any]
    related_counts: dict[str, int] = field(default_factory=dict)

    @property
    def full_name(self) -> str:
        first_name = self.attributes.get("FORNAVN") or ""
        last_name = self.attributes.get("ETTERNAVN") or ""
        return f"{first_name} {last_name}".strip() or "(ukjent navn)"


@dataclass
class MergeResult:
    session_id: int | None
    output_lines: list[str]


def is_production(profile: str | None) -> bool:
    return bool(profile) and "prod" in profile.lower()


def dsn_for(profile: str | None) -> str:
    return PROD_DSN if is_production(profile) else TEST_DSN


def vault_path_for(profile: str | None) -> str:
    return PROD_VAULT_PATH if is_production(profile) else TEST_VAULT_PATH


def extract_credentials(secret: dict[str, Any]) -> tuple[str, str]:
    username = _first_present(secret, USERNAME_KEYS)
    password = _first_present(secret, PASSWORD_KEYS)
    if not username or not password:
        available = ", ".join(sorted(secret)) or "(no fields)"
        raise CristinDatabaseError(
            f"Vault secret has no recognizable username/password fields. Fields: {available}"
        )
    return username, password


def _first_present(secret: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    lowercased = {key.lower(): value for key, value in secret.items()}
    for key in keys:
        value = lowercased.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


class CristinDatabaseService:
    def __init__(
        self,
        profile: str | None,
        vault_path: str | None = None,
        vault_client: VaultClient | None = None,
        connection: Any | None = None,
    ) -> None:
        self.profile = profile
        self.dsn = dsn_for(profile)
        self.vault_path = vault_path or vault_path_for(profile)
        self._vault_client = vault_client
        self._connection = connection

    @property
    def environment(self) -> str:
        return "PROD" if is_production(self.profile) else "TEST"

    def connect(self) -> Any:
        if self._connection is None:
            username, password = self._credentials()
            self._connection = self._open_connection(username, password)
        return self._connection

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, exception_type, exception, traceback) -> None:
        self.close()

    def fetch_person(self, lopenr: int) -> PersonProfile | None:
        try:
            return self._fetch_person(lopenr)
        except oracledb.Error as error:
            raise CristinDatabaseError(
                f"Oppslag av person {lopenr} i {self.dsn} feilet: {error}"
            ) from error

    def _fetch_person(self, lopenr: int) -> PersonProfile | None:
        connection = self.connect()
        with connection.cursor() as cursor:
            cursor.execute(
                f"select * from {SCHEMA}.{PERSON_TABLE} where {PERSON_KEY_COLUMN} = :lopenr",
                lopenr=lopenr,
            )
            row = cursor.fetchone()
            if row is None:
                return None
            column_names = [
                description[0].upper() for description in cursor.description
            ]
        attributes = {
            name: value
            for name, value in zip(column_names, row, strict=False)
            if name in PERSON_COLUMNS_OF_INTEREST
        }
        return PersonProfile(
            lopenr=lopenr,
            attributes=attributes,
            related_counts=self._count_related_rows(lopenr),
        )

    def merge_person(self, from_lopenr: int, to_lopenr: int) -> MergeResult:
        if from_lopenr == to_lopenr:
            raise CristinDatabaseError(
                "Personen som skal forsvinne og personen som skal beholdes er den samme"
            )
        connection = self.connect()
        self._enable_dbms_output(connection)
        committed = False
        with connection.cursor() as cursor:
            session_id = cursor.var(int)
            try:
                cursor.execute(
                    MERGE_PERSON_STATEMENT,
                    update_db=UPDATE_DATABASE,
                    from_lopenr=from_lopenr,
                    to_lopenr=to_lopenr,
                    session_id=session_id,
                )
                output_lines = self._read_dbms_output(connection)
                connection.commit()
                committed = True
            except oracledb.Error as error:
                raise CristinDatabaseError(
                    f"PK_FDS200010.P_Merge_Person feilet: {error}"
                    f"{self._reported_output(connection)}"
                ) from error
            finally:
                if not committed:
                    connection.rollback()
        return MergeResult(
            session_id=self._to_int(session_id.getvalue()),
            output_lines=output_lines,
        )

    def _credentials(self) -> tuple[str, str]:
        client = self._vault_client or VaultClient()
        try:
            secret = client.read_secret(self.vault_path)
        except VaultError as error:
            raise CristinDatabaseError(str(error)) from error
        return extract_credentials(secret)

    def _open_connection(self, username: str, password: str) -> Any:
        logger.debug("Connecting to %s as %s", self.dsn, username)
        try:
            return oracledb.connect(user=username, password=password, dsn=self.dsn)
        except oracledb.Error as error:
            raise CristinDatabaseError(
                f"Could not connect to {self.dsn}: {error}. Is Tailscale connected?"
            ) from error

    def _count_related_rows(self, lopenr: int) -> dict[str, int]:
        counts: dict[str, int] = {}
        for label, table_name in RELATED_COUNT_TABLES:
            if not self._has_person_column(table_name):
                continue
            counts[label] = self._count_rows(table_name, lopenr)
        return counts

    def _has_person_column(self, table_name: str) -> bool:
        connection = self.connect()
        with connection.cursor() as cursor:
            cursor.execute(
                "select count(*) from all_tab_columns "
                "where owner = :owner and table_name = :table_name and column_name = :column_name",
                owner=SCHEMA,
                table_name=table_name,
                column_name=PERSON_KEY_COLUMN,
            )
            return bool(self._to_int(cursor.fetchone()[0]))

    def _count_rows(self, table_name: str, lopenr: int) -> int:
        connection = self.connect()
        with connection.cursor() as cursor:
            cursor.execute(
                f"select count(*) from {SCHEMA}.{table_name} where {PERSON_KEY_COLUMN} = :lopenr",
                lopenr=lopenr,
            )
            return self._to_int(cursor.fetchone()[0]) or 0

    @staticmethod
    def _enable_dbms_output(connection: Any) -> None:
        with connection.cursor() as cursor:
            cursor.callproc("dbms_output.enable", (DBMS_OUTPUT_BUFFER_SIZE,))

    @staticmethod
    def _read_dbms_output(connection: Any) -> list[str]:
        lines: list[str] = []
        with connection.cursor() as cursor:
            line = cursor.var(str, DBMS_OUTPUT_LINE_SIZE)
            status = cursor.var(int)
            while True:
                cursor.callproc("dbms_output.get_line", (line, status))
                if status.getvalue() != 0:
                    break
                lines.append(line.getvalue() or "")
        return lines

    @classmethod
    def _reported_output(cls, connection: Any) -> str:
        try:
            lines = cls._read_dbms_output(connection)
        except oracledb.Error:
            return ""
        return "\n" + "\n".join(lines) if lines else ""

    @staticmethod
    def _to_int(value: Any) -> int | None:
        return None if value is None else int(value)
