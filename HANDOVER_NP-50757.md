# Handover NP-50757 — DLR-filer til NVA

> Lag for fersk Claude Code-økt. Start med å lese denne, så de tre dokumentene
> nederst. Branch: `NP-50757-dlr-file-upload`, alle endringer uncommitted.
> Jira: https://sikt.atlassian.net/browse/NP-50757

## Status

- ✅ **Branch:** `NP-50757-dlr-file-upload` (uncommitted endringer)
- ✅ **Tester:** 146 passerer, coverage 68.9%, ruff clean
- ✅ **Verifisert i dev:** create-test-publication, upload-one, publish-one,
  check-source, fix-log-source — alle virker
- ⏳ **add-links-manifest:** refaktorert til å sende `AdditionalIdentifier` via
  `UpdatePublicationRequest` (etter at `AssociatedLink` ble silently droppet av
  NVA). Tester grønne, men ikke verifisert end-to-end i dev ennå.
- ⏳ **Ikke kjørt i prod:** alt

## Lessons learned underveis

**NVA dropper `associatedArtifacts` silently på PUT** av publiserte ressurser
(`UpdatePublicationRequest`). Vi prøvde:
1. `UpdatePublicationRequest` (`type: "Publication"`) med `associatedArtifacts` →
   PUT 200, men feltet ikke persistert. Bekreftet ved at vår verifiseringskode
   (sjekker response) feilet.
2. Bestemte oss for å bytte strategi: bruke `additionalIdentifiers` i stedet, med
   `AdditionalIdentifier`-typen (generic) og `sourceName=dlr@<inst>`.
   `UpdatePublicationRequest` *applyer* `additionalIdentifiers` per Explore.
3. Round-tripper `entityDescription`, `projects`, `subjects`, `fundings`,
   `rightsHolder` fra GET — `UpdatePublicationRequest` erstatter alle felt den
   tar imot, så å utelate er det samme som å wipe.

## CLI-overflate (alt under `files`-gruppen)

| Kommando | Status | Formål |
|---|---|---|
| `upload-one` | ✅ Virker i dev | Single fil → publikasjon. Støtter `--s3-key`, `--local-file`, `--size`, `--mimetype` |
| `upload-manifest` | Skrevet, ikke kjørt prod | Batch fra manifest med `--state`-fil for resume |
| `publish-one` | ✅ Virker i dev | Publiser én draft (idempotent) |
| `publish-manifest` | Skrevet, ikke kjørt prod | Batch publish per institusjon |
| `create-test-publication` | ✅ Virker i dev | Bootstrap dev-smoke-test, `--system UNKNOWN` for å trigge OTHER |
| `check-source` | ✅ Virker i dev | Read-only sources via GSI `ResourcesByIdentifier`, owner-gate |
| `fix-log-source` | ✅ Virker i dev | OTHER→DLR, conditional update, owner-gate default, `--force` overstyrer |
| `add-links-manifest` | 🔴 Blokkert | Knytte `link`-content som AssociatedLink |
| `extract-handles` | Skrevet, ikke kjørt prod | Dump handles for `handle redirect-to-nva` |

## Service-arkitektur

`FileUploadApiService` ([commands/services/file_upload_api.py](commands/services/file_upload_api.py)):

- Auth: `ExternalClientToken.from_key_file(path)` — client-credentials, cacher/refresher token
- `upload(publication_id, source, ...)` — multipart create→prepare→PUT→complete
- `publish(publication_id)` — POST /publish, idempotent (409=OK)
- `create_publication(title)` — POST / med minimal DLR-shape (publicationDate, Event-context, én Contributor)
- `get_publication(id)` / `update_publication(id, body)` — GET/PUT mot publikasjonen
- `add_associated_links(id, urls)` — GET → merge → PUT → **verifiser i response** (🔴 må endre body-type)

Filkilder:
- `LocalFileSource(path)` — for dev-smoke-test uten S3
- `S3ObjectSource(s3, bucket, key, size_override=...)` — `size_override` lar oss
  hoppe over `head_object` (manifest har `dlr_content_size_bytes`)

HTTP-headers: `Authorization: Bearer <token>`, `Content-Type: application/json`,
`System: DLR` (default — kan overstyres med `--system UNKNOWN` for å trigge OTHER-source).

`_raise_for_status_with_body(response)` — alle HTTP-feil viser response-body
(viktig for å diagnostisere 400/401/403 fra NVA).

## Trygghet i `fix-log-source`

1. **Owner-gate (default):** Henter Resource-row via GSI `ResourcesByIdentifier`
   (`PK3=SK3=Resource:<id>`), inflater zlib `data`-blob, sjekker at
   `resourceOwner.owner` inneholder `dlr-import-integration`. Mismatch → skip
   hele partisjonen. `--force` overstyrer.
2. **Conditional UpdateItem:** `SET data.importSource.source = :dlr` betinget på
   at den fortsatt er `:other`. Hindrer dobbeltskriving.
3. **`--dry-run` default true.** `--no-dry-run` for å apply.
4. **Per-row fault tolerance:** én feilet rad stopper ikke batchen.

## Dokumenter å lese (i denne rekkefølgen)

1. **[FILE_UPLOAD_HANDOFF.md](FILE_UPLOAD_HANDOFF.md)** — fullstendig domene-kontekst:
   manifest-format, S3-bøtte, scopes, autorisasjon, API-kontrakt, root cause for
   `System: DLR`. Les hele.
2. **[KJOREPLAN_NP-50757.md](KJOREPLAN_NP-50757.md)** — operativ sjekkliste for
   prod-kjøring per institusjon (VID → UiB → UiT → OsloMet → USN → NTNU).
3. **[DEV_TEST_NP-50757.md](DEV_TEST_NP-50757.md)** — 8-stegs dev-smoke-test
   før prod. Bruker variabler `KEY`, `S3_KEY`, `FILENAME`, `MIMETYPE`, `SIZE`,
   `ID` definert i steg 0.

## Beslutninger som er låst

- `fileType = "OpenFile"` på alle uploads
- `publisherVersion = "AcceptedVersion"` (gjettet, må endelig avklares med produkteier)
- Per-institusjon-kjøring med separat nøkkelfil; NTNU-kjøringen tar også `hist.no`
- `--system DLR` på alle skrive-kall (rotårsak til feilaktig OTHER-logging)
- AssociatedLink-relation: `sameAs` (verifisert mot `RelationType`-enum i NVA)
- `sharing_link` ignoreres (kun pekere til samme fil)
- Backup før `fix-log-source` droppet — conditional update + dry-run anses tilstrekkelig

## Åpne spørsmål

1. `publisherVersion` — venter på produkteier-bekreftelse
2. `link`-AssociatedLink — fungerer det med PartialUpdate? (denne handoverens hovedpunkt)
3. Når dev-suiten er grønn: når går vi til prod? Skal det merges PR først?

## Test-suite

```bash
uv run pytest              # 145 tester, ~28s
uv run ruff check          # 0 issues
```

Test-mønster:
- Service-laget: `responses` lib for HTTP-mocking (`test_file_upload_api.py`)
- CLI-laget: `CliRunner` + `moto` + `responses` (`test_files_cli.py`)
- Helpers: `_resource_row` lager zlib-komprimert Resource-row med PK3/SK3 for GSI;
  `_log_entry_row` lager LogEntry-rad uten zlib

## Hva jeg ville gjort som første handling

1. Les dokumentene i rekkefølgen over (10 min)
2. Endre `update_publication`/`add_associated_links` til å sende
   `PartialUpdatePublicationRequest`-body:
   ```python
   body = {
       "type": "PartialUpdatePublicationRequest",
       "identifier": publication_identifier,
       "associatedArtifacts": artifacts + new_links,
   }
   ```
3. Oppdater 2 tester (`test_add_associated_links_*`) til å forvente ny body-shape
4. Be brukeren re-kjøre `add-links-manifest` mot dev og bekrefte at AssociatedLink
   faktisk persisteres
5. Hvis det fungerer: oppdater [DEV_TEST_NP-50757.md](DEV_TEST_NP-50757.md) steg 7.5
   og [KJOREPLAN_NP-50757.md](KJOREPLAN_NP-50757.md) steg 2.5 om nødvendig
6. Kjør hele dev-test-suiten (steg 1-8) som final go/no-go før prod

## Bruker-preferanser (fra CLAUDE.md og samtale)

- **Aldri kjør `git commit`** — bruker gjør det selv
- Bruker `AWS_PROFILE` env var, ikke `--profile`-flagg
- Conventional commits: `feat(files): description #NP-50757`
- Type hints på Python-kode
- Korte private metoder med navn, ikke single-char variabler
- Ingen inline-kommentarer med mindre absolutt nødvendig
- Norsk og engelsk om hverandre i samtaler — kode/identifiers på engelsk
