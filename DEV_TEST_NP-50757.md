# Dev-test plan for NP-50757

> Operativ sjekkliste for å verifisere upload/publish/check/fix-flyten i dev
> *før* prod-batchen i [`KJOREPLAN_NP-50757.md`](./KJOREPLAN_NP-50757.md).

## Forutsetninger

- [ ] **Dev AWS-profil**: `export AWS_PROFILE=<nva-sandbox-eller-similar>`
      (alt som *ikke* inneholder "prod" treffer test-API per `AWS_PROFILE`-konvensjon)
- [ ] **Ekstern-klient-nøkkel for dev** med scopes:
  - `https://api.nva.unit.no/scopes/third-party/publication-upsert` (create + publish + delete)
  - `https://api.nva.unit.no/scopes/third-party/file-upload`
- [ ] **Testfil i `loke.storage`** — kjenn UUID. Eller bruk én av prod-filene fra et
      manifest (read-only — påvirker ikke prod).
- [ ] **`/NVA/ApiDomain` SSM-param** peker på dev (sjekk: `aws ssm get-parameter --name /NVA/ApiDomain`).

## Steg

### 0. Sett variabler én gang for hele rutinen

```bash
# uv run cli.py users create-external -c bb3d0c0c-5065-4623-9b98-5810983c2478 --shortname sikt -i dlr-import -s https://api.nva.unit.no/scopes/third-party/publication-read,https://api.nva.unit.no/scopes/third-party/publication-upsert,https://api.nva.unit.no/scopes/third-party/file-upload


export AWS_PROFILE=<dev>
export KEY=~/keys/<dev-key.json>

# Plukk én linje fra et data_to_keep*-manifest og fyll inn:
export S3_KEY=<dlr_content_identifier>          # f.eks. 57b10bd2-e9fd-...
export FILENAME="DigSam-podkast om digital sikkerhet - transkribert.txt"                  # f.eks. "DigSam-podkast.txt"
export MIMETYPE="text/plain"          # f.eks. text/plain
export SIZE=21334             # f.eks. 21334
```

### 1. Verifiser nøkkel + auth

```bash
uv run python -c "
from commands.services.file_upload_api import ExternalClientToken
import os
t = ExternalClientToken.from_key_file(os.path.expanduser(os.environ['KEY']))
print('token OK, customer:', t.customer_uri)
"
```

Sammenlign `customerUri`/`cristinOrgUri` med dev-tenantens forventede org.
Avvik → 403 senere.

### 2. Lag testdraft via API (med nøkkelen)

Bruk `--system UNKNOWN` slik at `PublicationCreated`-LogEntry-en får
`source=OTHER` (ukjent System-header → backend faller tilbake til OTHER).
Da kan vi senere verifisere både at `upload-one`/`publish-one` produserer
`DLR`-rader, OG at `fix-log-source` retter den ene `OTHER`-raden — alt på
samme test-draft.

```bash
ID=$(uv run cli.py files create-test-publication \
    --key-file $KEY \
    --title "Smoke NP-50757 $(date +%H:%M)" \
    --system UNKNOWN)
echo "Created: $ID"
```

`resourceOwner.owner` blir automatisk DLR-integrasjonsbrukeren (slik at
owner-gate i `check-source`/`fix-log-source` aksepterer den).

### 3. Upload én fil

```bash
uv run cli.py files upload-one \
    --key-file $KEY \
    --publication $ID \
    --s3-key $S3_KEY \
    --filename "$FILENAME" \
    --mimetype "$MIMETYPE" \
    --size $SIZE \
    --license "https://creativecommons.org/licenses/by/4.0/"
```

Filnavn + mimetype + size sendes manuelt her fordi vi ikke har manifestet i loopen
(slik `upload-manifest` har). Uten `--filename` ender NVA opp med UUID-en som
filnavn; uten `--mimetype` blir det `application/octet-stream` hvis HEAD ikke kan
gjette det; `--size` hopper over `head_object`-kallet helt.

**Verifiser:**
- [ ] HTTP 200/201 på complete (File-objekt i output)
- [ ] Filen vises i dev-UI på draften
- [ ] `check-source` viser source=DLR (se steg 5)

### 4. Publish

```bash
uv run cli.py files publish-one --key-file $KEY --publication $ID
```

**Verifiser:**
- [ ] 202 fra API
- [ ] Status PUBLISHED i dev-UI
- [ ] Re-kjør → 409 håndteres som OK
- [ ] Ny `PublishedResourceEvent`-LogEntry har source=DLR

### 5. Verifiser sources med check-source

Bygg et mini-manifest med kun din test-ID:

```bash
mkdir -p dev-test
cat > dev-test/manifest.json <<JSON
{
  "smoke-1": {
    "result_id": "$ID",
    "handle": null,
    "content": [{
      "dlr_content_identifier": "$S3_KEY",
      "dlr_content_type": "file",
      "dlr_submitter_email": "smoke@example.org"
    }]
  }
}
JSON

uv run cli.py files check-source dev-test/manifest.json --detail
```

**Forventet output:**
- `logs=3` (PublicationCreated + FileUploadedEvent + PublishedResourceEvent)
- Sources: `OTHER=1` (fra steg 2 med `--system UNKNOWN`), `DLR=2`
  (FileUploadedEvent + PublishedResourceEvent — disse kjørte med default DLR)
- `owner=<din-integrasjonsbruker>` uten OWNER-MISMATCH-tag
- `Resources with owner mismatch: 0`

### 6. Verifiser fix-log-source virker

Test-draften fra steg 2 har allerede én `OTHER`-LogEntry (fra `--system UNKNOWN`
på `PublicationCreated`), så vi kan kjøre rett mot samme manifest:

```bash
# Dry-run
uv run cli.py files fix-log-source dev-test/manifest.json
# Forventet:
#   DRY-RUN <id> log=... topic=PublicationCreated OTHER→DLR
#   candidates=1 updated=0 failed=0 skipped_owner=0

# Apply
uv run cli.py files fix-log-source dev-test/manifest.json --no-dry-run
# Forventet: candidates=1 updated=1 failed=0 skipped_owner=0

# Verifiser
uv run cli.py files check-source dev-test/manifest.json
# Det viktige: OTHER=0. DLR-tallet avhenger av hvor mange opplastinger
# du har gjort. <missing>=N er forventet for PublicationUpdated/
# PublicationPublished — de event-typene har ikke importSource og
# blir aldri kandidater for fix.
```

> **Hvorfor `<missing>`?** Nyere event-typer (`PublicationUpdated`,
> `PublicationPublished`) lagrer ikke `data.importSource`. De er intern flyt
> i NVA, ikke ekstern import. `fix-log-source` rører *kun* rader med
> `source = OTHER`, så `<missing>`-rader blir alltid stående urørt — som
> ønsket.

### 7. Verifiser owner-gate

Lag et "fremmed" mini-manifest med en result_id som ikke eies av din
DLR-integrasjon (f.eks. en eksisterende test-publikasjon laget via UI). Åpne en
vilkårlig ikke-DLR-publikasjon i dev-UI og kopier `identifier` fra URL-en:

```bash
export FOREIGN_ID=0198cbfb6cbc-c4a9d3f5-b1c7-47d1-8780-15243bc1ced9

cat > dev-test/foreign-manifest.json <<JSON
{
  "foreign-1": {
    "result_id": "$FOREIGN_ID",
    "handle": null,
    "content": [{
      "dlr_content_identifier": "n/a",
      "dlr_content_type": "file",
      "dlr_submitter_email": "foreign@example.org"
    }]
  }
}
JSON

uv run cli.py files check-source dev-test/foreign-manifest.json
# Skal vise [OWNER-MISMATCH]

uv run cli.py files fix-log-source dev-test/foreign-manifest.json --no-dry-run
# Skal vise SKIP <id> ... (OWNER-MISMATCH)
# skipped_owner > 0, updated=0
```

### 7.5 Verifiser add-links-manifest

Draften fra steg 2-4 er nå publisert og har én godkjent fil (lastet opp i
steg 3). Vi legger til en lenke *etter* publisering — samme rekkefølge som prod
(KJØREPLAN steg 3.3) — og verifiserer både at lenken kommer på, og at den
godkjente fila round-trippes (ikke wipes) når hele `associatedArtifacts` sendes
tilbake i PUT-en.

Lag et lite manifest med både `link` og `sharing_link`:

```bash
cat > dev-test/links-manifest.json <<JSON
{
  "links-1": {
    "result_id": "$ID",
    "content": [
      {
        "dlr_content_type": "link",
        "dlr_content": "https://example.org/dev-test-link-1",
        "dlr_submitter_email": "smoke@example.org"
      },
      {
        "dlr_content_type": "sharing_link",
        "dlr_content": "https://example.org/should-be-ignored",
        "dlr_submitter_email": "smoke@example.org"
      }
    ]
  }
}
JSON

# Tørrkjør — skal liste 1 URL, ignorere sharing_link
uv run cli.py files add-links-manifest dev-test/links-manifest.json \
    --key-file $KEY --institution example.org --dry-run
# Forventet: "relation=sameAs" i header-linja

# Ekte
uv run cli.py files add-links-manifest dev-test/links-manifest.json \
    --key-file $KEY --institution example.org
# Forventet: added=1 skipped_existing=0

# Re-kjør — idempotent
uv run cli.py files add-links-manifest dev-test/links-manifest.json \
    --key-file $KEY --institution example.org
# Forventet: added=0 skipped_existing=1
```

Bekreft i dev-UI / via `curl` at lenken vises som `AssociatedLink` med
`relation=sameAs` under `associatedArtifacts` på publikasjonen. Sjekk samtidig
at eventuelle allerede opplastede filer fortsatt ligger på `associatedArtifacts`
(de skal round-trippes, ikke wipes).

### 8. (Valgfritt) Handles

Hopp over `handle redirect-to-nva` i dev — handles er globale via hdl.handle.net.
Kun extract-kommandoen er trygg å teste:

```bash
uv run cli.py files extract-handles dev-test/manifest.json
# Tom output (vi satte handle=null)
```

## Avgjørelsesmatrise — alt grønt før prod?

| Sjekk | OK? |
|---|---|
| upload-one + check-source viser `DLR` for ny FileUploadedEvent | |
| publish-one + check-source viser `DLR` for ny PublishedResourceEvent | |
| fix-log-source dry-run + apply gir `OTHER=0` etterpå | |
| Owner-gate skipper fremmed result_id | |
| `--force` på fix-log-source bypasser owner-gate (test isolert) | |
| add-links-manifest legger til AssociatedLink (relation sameAs) + er idempotent ved re-kjøring | |
| add-links-manifest round-tripper eksisterende filer (associatedArtifacts wipes ikke) | |

## Opprydding

Draftene som testen lager ligger igjen i dev. Slett via dev-UI hvis ønskelig —
ikke kritisk siden de er drafts og isolert i test-miljøet.
