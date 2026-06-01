# Kjøreplan NP-50757 — DLR-filer → NVA

> Operativ sjekkliste for selve overføringen. Bakgrunn/kontrakt ligger i
> [`FILE_UPLOAD_HANDOFF.md`](./FILE_UPLOAD_HANDOFF.md).
> Jira: https://sikt.atlassian.net/browse/NP-50757

## 0. Pre-flight (engang)

```bash
# Branch + verifiser kode
git checkout NP-50757-dlr-file-upload
uv run pytest commands/services/tests/test_file_upload_api.py
uv run ruff check
uv run cli.py files --help    # skal liste 5 subkommandoer
```

**Sjekkliste før noe rør prod:**
- [ ] PR merget eller godkjent
- [ ] Alle 6 nøkkelfiler (`*.json` fra produkteier) ligger lokalt, IKKE i repoet
- [ ] `data_to_keep.json` + `data_to_keep_usn.json` på plass (`~/Downloads/`)
- [ ] Bekreft `cristinOrgUri` i hver nøkkel matcher institusjonens topp-org
      (jf. `TrustedThirdPartyGrantStrategy` — feil match = 403)
- [ ] AWS-profil for prod satt: `export AWS_PROFILE=<prod-profil>`

---

## 1. Smoke-test mot ÉN publikasjon

**Mål:** verifisere at fila lander, OG at LogEntry får `source=DLR`.

Velg én liten ressurs fra ett av manifestene — helst med kun én master-fil.

```bash
export AWS_PROFILE=<prod>

# 1a. Tørrkjør for én institusjon
uv run cli.py files upload-manifest ~/Downloads/data_to_keep.json \
    --key-file ~/keys/uib.json --institution uib.no --dry-run

# 1b. Plukk én linje fra output → kjør upload-one
uv run cli.py files upload-one \
    --key-file ~/keys/uib.json \
    --publication <result_id-fra-dry-run> \
    --s3-key <dlr_content_identifier-fra-dry-run> \
    --license "https://creativecommons.org/licenses/by/4.0/"

# 1c. Publiser samme ressurs
uv run cli.py files publish-one \
    --key-file ~/keys/uib.json \
    --publication <result_id-fra-dry-run>
```

**Verifiser ALLE tre punkter før vi går videre:**

1. **Filen vises på publikasjonen i NVA-UI.**
2. **LogEntry-source = DLR** (ikke OTHER):
   ```bash
   uv run cli.py dynamodb export --table resources \
       --filter "PK0:eq:Resource:<result_id>" \
       --output-dir verify/smoke
   # Åpne JSONL, søk etter "LogEntry:" — sjekk data.importSource.source == "DLR"
   ```
3. **Publisering OK** (status `PUBLISHED` i UI).

> ⚠️ Hvis steg 2 viser `OTHER` for den NYE LogEntry-raden → STOPP. `System: DLR`-header
> har ikke gått fram. Ikke kjør batch før dette er fikset.

---

## 2. Batch-opplasting per institusjon

Rekkefølge: minste først for å fange feil tidlig.

| # | Institusjon | Filer | `--institution` | `--key-file` | Manifest |
|---|---|---|---|---|---|
| 1 | VID | 2 | `vid.no` | `~/keys/vid.json` | `data_to_keep.json` |
| 2 | UiB | 11 | `uib.no` | `~/keys/uib.json` | `data_to_keep.json` |
| 3 | UiT | 17 | `uit.no` | `~/keys/uit.json` | `data_to_keep.json` |
| 4 | OsloMet | 21 | `oslomet.no` | `~/keys/oslomet.json` | `data_to_keep.json` |
| 5 | USN | 35 | `usn.no` | `~/keys/usn.json` | `data_to_keep_usn.json` |
| 6 | NTNU | 150 | `ntnu.no,hist.no` | `~/keys/ntnu.json` | `data_to_keep.json` |

For hver institusjon:

```bash
INST=vid
KEY=~/keys/${INST}.json
MANIFEST=~/Downloads/data_to_keep.json     # bytt til _usn.json for USN
DOMAINS=${INST}.no                          # NTNU: ntnu.no,hist.no
STATE=state/${INST}-upload.jsonl
mkdir -p state

# Tørrkjør
uv run cli.py files upload-manifest $MANIFEST \
    --key-file $KEY --institution $DOMAINS --dry-run

# Ekte kjøring (resumable via state-fila)
uv run cli.py files upload-manifest $MANIFEST \
    --key-file $KEY --institution $DOMAINS \
    --state $STATE
```

**Stikkprøve etter hver institusjon:**
- Én tilfeldig ressurs har alle forventede filer i UI
- LogEntry-source på samme ressurs = DLR

Resume ved feil: kjør samme kommando på nytt — state-fila hopper over `ok`-rader.

---

## 3. Publiser draftene

Først etter at *alle* filer for institusjonen er oppe (slik at pending filer blir
auto-godkjent via `publishResourceWithPendingFiles`).

```bash
# Tørrkjør
uv run cli.py files publish-manifest $MANIFEST \
    --key-file $KEY --institution $DOMAINS --dry-run

# Ekte
uv run cli.py files publish-manifest $MANIFEST \
    --key-file $KEY --institution $DOMAINS
```

Idempotent (409 = allerede publisert = OK), trygt å re-kjøre.

---

## 4. Rett historiske `OTHER`→`DLR` på LogEntry

**Backup først** (eksporter LogEntry-rader med `OTHER`-source bredt):

```bash
uv run cli.py dynamodb export --table resources \
    --filter "SK0:begins_with:LogEntry:" \
    --output-dir backups/logentries-$(date +%Y%m%d-%H%M)
```

**Tørrkjør** (default — trygt):

```bash
uv run cli.py files fix-log-source \
    ~/Downloads/data_to_keep.json ~/Downloads/data_to_keep_usn.json
```

Les output, telle kandidater. De nye LogEntry-radene fra steg 1-3 skal være DLR
allerede; tørrkjøringen lister altså de *gamle* `Other`-radene fra tidligere migrering.

**Apply** (når tallene ser fornuftige ut):

```bash
uv run cli.py files fix-log-source \
    ~/Downloads/data_to_keep.json ~/Downloads/data_to_keep_usn.json \
    --no-dry-run
```

Conditional update (`source = OTHER`) hindrer dobbeltskriving.

---

## 5. Etterkontroll

- [ ] Antall publiserte ressurser ≈ 147
- [ ] 5 tilfeldige ressurser har alle filene fra manifestet, korrekt mimetype/størrelse
- [ ] Spot-check LogEntry: alle nye entries har `source=DLR`, ingen gjenværende `Other`
      på de migrerte ressursene
- [ ] State-filer arkivert (intern lagring eller `state/`-folder pushet til repoet
      som artefakt — vurder hva som er passende)

---

## Hvis ting går sidelengs

| Symptom | Sjekk |
|---|---|
| 403 på complete/publish | `cristinOrgUri` i nøkkel ≠ `ownerAffiliation`. Feil institusjons-nøkkel. |
| 401 | Token utløpt — bare restart, `ExternalClientToken` re-fetcher. |
| LogEntry source = OTHER etter NY opplasting | `System: DLR`-header gikk ikke fram. Stopp og inspiser `_headers()` i `file_upload_api.py`. |
| 5MiB-feil fra S3 PUT | Del-størrelse < 5 MiB på ikke-siste del. Bug i `_upload_parts`. |
| Duplikat-fil på en ressurs | Kjørt uten state-fil. Manuell rydding via admin-UI. |
| `--institution` matcher 0 ressurser | Domenet matcher ikke `dlr_submitter_email`. Sjekk manifest. |
