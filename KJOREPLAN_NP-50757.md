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
uv run cli.py files --help    # skal liste 7 subkommandoer
mkdir -p state handles
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
   # Lag et lite engangs-manifest med kun denne ene ressursen, eller bruk
   # det fulle manifestet + --institution for å avgrense.
   uv run cli.py files check-source ~/Downloads/data_to_keep.json \
       --institution uib.no --detail
   # Sjekk: ressursens nye LogEntry-rader har source=DLR (ikke OTHER)
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
- LogEntry-source på alle ressurser i denne kjøringen:
  ```bash
  uv run cli.py files check-source $MANIFEST --institution $DOMAINS
  ```
  Skal vise `OTHER`-rader kun for *gamle* entries (rettes i steg 4).

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

## 3.5 Migrér handles til NVA

Hver ressurs som har `handle`-felt i manifestet skal redirigeres til sin nye
NVA-side. Først dumpes handle-listen til fil (for audit + resume), så kjøres
`handle redirect-to-nva` som har egen state via `handle-done.csv`.

```bash
# 3.5a — Hent ut handles per institusjon
uv run cli.py files extract-handles $MANIFEST --institution $DOMAINS \
    --output handles/${INST}.txt

# 3.5b — Tørrkjør (én linje per handle, viser planlagt NVA-URL)
xargs uv run cli.py handle redirect-to-nva --dry-run < handles/${INST}.txt

# 3.5c — Ekte
xargs uv run cli.py handle redirect-to-nva < handles/${INST}.txt
```

`handle redirect-to-nva` søker hver handle opp i NVA og oppdaterer den til
`https://<ApplicationDomain>/registration/<identifier>` hvis det finnes nøyaktig
ett treff. Resultatene appendes til `handle-done.csv` slik at re-kjøring hopper
over allerede prosesserte handles.

---

## 4. Rett historiske `OTHER`→`DLR` på LogEntry

**Forhåndsbilde** (read-only sjekk, ingen scan):

```bash
uv run cli.py files check-source \
    ~/Downloads/data_to_keep.json ~/Downloads/data_to_keep_usn.json
# Noter:
#   - Total OTHER-count (skal matche dry-run-tellingen i neste steg)
#   - "Resources with owner mismatch" SKAL være 0. Er den ikke det,
#     stopp og verifiser hvilke ressurser som ikke eies av dlr-import-integration.
```

**Tørrkjør** (default — trygt). Owner-gate er på som default og hopper over
partisjoner der `resourceOwner.owner` ikke inneholder `dlr-import-integration`:

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

> ⚠️ **Owner-gate**: hvis output viser `skipped_owner > 0`, har minst én ressurs
> en eier som ikke matcher `dlr-import-integration`. Det er som regel et signal
> om at feil result_id har sneket seg inn i manifestet — *ikke* en grunn til å
> kjøre `--force`. Verifiser først.

---

## 5. Etterkontroll

- [ ] Antall publiserte ressurser ≈ 147
- [ ] 5 tilfeldige ressurser har alle filene fra manifestet, korrekt mimetype/størrelse
- [ ] LogEntry-status: `files check-source` på alle manifestene viser 0 `OTHER`-rader
- [ ] Handles: åpne et utvalg `https://hdl.handle.net/...`-URLer fra manifestet og
      bekreft at de redirigerer til NVA-registrering, ikke gammel DLR-side
- [ ] State-filer (`state/`) + handles-filer (`handles/`) + `handle-done.csv`
      arkivert (intern lagring eller pushet til repoet som artefakt)

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
