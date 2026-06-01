# Handoff: Overføring av DLR-filer til NVA-publikasjoner (NP-50757)

> Skrevet for å kunne fortsette i en fersk Claude Code-økt i dette repoet.
> Jira: https://sikt.atlassian.net/browse/NP-50757

## Oppgaven

DLR-metadata er migrert til NVA og ligger nå som **drafts i prod**. Gjenstående steg (NVAs ansvar):
1. **Overføre filene** og knytte dem til publikasjonene (multipart-upload).
2. **Publisere** hver publikasjon direkte (uten ticket).
3. **Rette feil source** (`Other` → `DLR`) på logg-innslag som ble laget av fil-/oppdaterings-
   operasjonene.

Filopplastingen bruker det eksisterende multipart-upload-API-et (samme flyt som frontend):
`create → prepare (per del) → PUT til presignert URL → complete`. Krever ikke direkte tilgang til
NVAs S3-bøtte, kun et gyldig bearer-token.

## Kilde for filene

- S3-bøtte: **`loke.storage`**. Alle miljøene våre har allerede tilgang (boto3 via profil/`AWS_PROFILE`).
- S3-nøkkel per fil: **`dlr_content_identifier`** (UUID) fra manifestet — **bekreftet: ingen prefix**.

## Auth / eierskap — VIKTIG

Vi bruker **eksterne klient-nøkler** (ikke standard `BackendCognitoClientCredentials`), slik at
filene får **riktig eierskap**. Hver nøkkel ligger i en JSON-fil på dette formatet:

```json
{
  "clientId": "4u....om",
  "clientSecret": "1ps....bee",
  "tokenUrl": "https://...oauth2/token",
  "clientName": "dmp-approvals-integration",
  "customerUri": "https://api...t.no/customer/7...6d0",
  "cristinOrgUri": "http...it.no/cristin/organization/5941.0.0.0",
  "actingUser": "approvals-integration@dmp",
  "scopes": ["https://api.nva.unit.no/scopes/third-party/approval-upsert", ...]
}
```

- Token hentes med client-credentials grant mot **`tokenUrl` i selve fila** (ingen SSM nødvendig).
- Eierskap utledes av hvilken ekstern klient (`customerUri`) som gjør opplastingen.
- Derfor brukes `complete`-varianten `ExternalCompleteUpload` (API-et sjekker eksplisitt at
  brukeren er en ekstern klient for denne typen, og lar oss sette `fileType`/`license`/`publisherVersion`).
- Nøkkelen må ha en third-party upload-scope (jf. `scopes`).

## Manifest-format (`data_to_keep*.json`)

Eksempelfiler i `~/Downloads/`: `data_to_keep.json` og `data_to_keep (1).json` er **identiske og
blandet institusjon** (ntnu/uit/uib/oslomet/vid/hist), `data_to_keep_usn.json` er ren USN.

Toppnivå er en dict nøklet på DLR-resurs-UUID. Hver verdi:

```json
"7910e1cb-...": {
  "result_id": "019de8452706-7afd4395-bc2e-44d4-86d9-5192a042f61f",   // = NVA publicationIdentifier
  "license": "https://creativecommons.org/licenses/by/4.0/",          // brukes på complete
  "handle": "https://hdl.handle.net/11250/3107928",
  "doi": null,
  "content": [
    {
      "dlr_content": "Fag -og forskingsartiklar 2022.mp4",            // filnavn
      "dlr_content_identifier": "965e4003-0aa3-40d9-ad1f-25d6bedfefa8", // antatt S3-nøkkel i loke.storage
      "dlr_content_mime_type": "video/mp4",                            // mimetype
      "dlr_content_size_bytes": "72334946",                            // størrelse
      "dlr_content_type": "file",                                      // "file" | "link" | "sharing_link"
      "dlr_content_master": "true",
      "dlr_content_original": "true",
      "dlr_submitter_email": "evamos@vid.no",                          // domenet => institusjon
      "dlr_content_generated": "true"                                  // finnes kun på auto-genererte
    }
  ]
}
```

### `dlr_content_type` — bekreftede verdier (telt i `data_to_keep.json`)
- `file` (239) — faktiske filer i `loke.storage`, **disse lastes opp**.
- `link` (89) — eksterne URL-er (f.eks. `https://www.solstien3.no/`), **ikke filer** (AssociatedLink-metadata).
- `sharing_link` (2) — delings-URL-er, **ikke filer**.

### master vs ikke-master (blant ekte `file`-items)
- `master=true` (116): hovedfila i ressursen.
- `master=false`, ekte/`original=true` (84): **tilleggsfiler** — ekte innhold (ekstra docx, bilder,
  PDF, slides) som hører til samme ressurs, bare ikke merket som hoved.
- `master=false` + `generated=true` (38): auto-genererte derivater (thumbnails, `metadata_external.json`).

### Utvelgelsesregel
Last opp alle `dlr_content_type == "file"` der `dlr_content_generated != "true"`
(både master og ikke-master ≈ 201 filer). Hopp over `link`/`sharing_link` og genererte derivater.
⚠️ Noen få filer mangler mimetype (`null`, navn som `0_w3lh1pv3_download.dat`) → fall tilbake til
gjetting/`application/octet-stream` (håndtert i `S3ObjectSource`).
**BESLUTTET:** `fileType = "OpenFile"` (filene går rett til Open). `publisherVersion` valgfri —
avklar med produkteier om den skal settes (kan utelates).

## Eierskap-mapping (BESLUTTET: én kjøring per institusjon)

Hver institusjon kjøres separat med sin egen nøkkelfil. Institusjon avgjøres av
`dlr_submitter_email`-domenet per ressurs — **ingen ressurs har blandede domener**, så det er
entydig. De to hovedfilene (`data_to_keep.json` og `data_to_keep (1).json`) er **identiske**;
bruk én av dem + `data_to_keep_usn.json`.

Opplastbare filer (`dlr_content_type == "file"` og ikke `generated`):

| Institusjon | Manifest | Ressurser | Filer |
|---|---|---|---|
| NTNU (`ntnu.no` + `hist.no`) | data_to_keep.json | 82 | 150 |
| OsloMet (`oslomet.no`) | data_to_keep.json | 21 | 21 |
| UiT (`uit.no`) | data_to_keep.json | 13 | 17 |
| UiB (`uib.no`) | data_to_keep.json | 9 | 11 |
| VID (`vid.no`) | data_to_keep.json | 2 | 2 |
| USN (`usn.no`) | data_to_keep_usn.json | 20 | 35 |
| **Sum** | | **147** | **236** |

→ 6 nøkkelfiler, 6 kjøringer. CLI-en filtrerer manifestet på et `--institution <domene>` (eller
leser domenet fra nøkkelfilas `customerUri`/`clientName`) og laster kun opp ressurser som matcher.

**BESLUTTET:** `hist.no` (1 ressurs / 1 fil) kjøres sammen med NTNU-nøkkelen — NTNU-kjøringen
matcher derfor både `ntnu.no` og `hist.no`.

## Verifisert API-kontrakt (lest fra kildekoden i nva-publication-api)

Kilde: `publication-file/src/main/java/no/unit/nva/publication/file/upload/restmodel/`

Base-URL: `https://{api_domain}/publication/{publicationIdentifier}/file-upload/{action}`
(`api_domain` = SSM-param `/NVA/ApiDomain`). Alle kall: `POST`, `Authorization: Bearer <token>`.

### `create`
- Request: `{ "filename": "...", "size": "<bytes-som-string>", "mimetype": "application/pdf" }`
  - **Ingen `key`, ingen `bucket`.** `mimetype` må parse som en gyldig MediaType.
- Response: `{ "uploadId": "...", "key": "<server-generert-uuid>" }`
- ⚠️ **Serveren genererer nøkkelen.** Bruk `key` fra responsen i alle påfølgende kall.

### `prepare`
- Request: `{ "uploadId": "...", "key": "<fra create>", "number": "<delnummer-som-string>" }`
  - ⚠️ Feltet heter **`number`**, ikke `partNumber`. Ingen `bucket`.
- Response: `{ "url": "<presignert PUT-url>" }`

### PUT av del
- `PUT <presignert url>` med del-bytene som body. Les `ETag` fra responsheaderen, strip anførselstegn.

### `complete`
```json
{
  "type": "ExternalCompleteUpload",
  "uploadId": "...",
  "key": "<fra create>",
  "parts": [ { "partNumber": 1, "etag": "..." } ],
  "fileType": "OpenFile",
  "license": "https://creativecommons.org/licenses/by/4.0/",
  "publisherVersion": "PublishedVersion",
  "embargoDate": null
}
```
- ⚠️ `publisherVersion` (ikke `publisherVesrion`). `parts` bruker `partNumber`+`etag`
  (`@JsonAlias` godtar både `partNumber`/`PartNumber` og `etag`/`ETag`).
- `fileType`: `OpenFile`/`InternalFile`. `license`/`publisherVersion`/`embargoDate` valgfrie.
- Response: et `File`-objekt med bl.a. `identifier`, `name`, `mimeType`, `size`.

### Detaljer
- Del-størrelse: minimum 5 MiB for alle deler unntatt siste (S3-krav).
- `abort` og `listparts` finnes også (`{uploadId, key}`) for opprydding/sjekk.

## Steg 2 — Publisere direkte (uten ticket)

Verifisert i `nva-publication-api`:
- Handler: `PublishPublicationHandler` (`publication-rest/.../update/PublishPublicationHandler.java`).
- Kall: **`POST https://{api_domain}/publication/{publicationIdentifier}/publish`**, **tom body**,
  headere `Authorization: Bearer <token>` + **`System: DLR`**. Suksess = **202 Accepted**.
- `PublishingService.publishResource(...)`:
  - **Idempotent**: hvis allerede `PUBLISHED` → returnerer uten endring (409/no-op trygt å retry).
  - Setter status `PUBLISHED`, `publishedDate`, og `PublishedResourceEvent` (source fra `System`-header).
  - **Approver pending filer samtidig**: har ressursen pending filer kalles
    `publishResourceWithPendingFiles(...)` — auto-complete avhengig av kundens `publicationWorkflow`.
- Forhåndskrav (`ResourcePublishValidator`): status DRAFT/PUBLISHED_METADATA/UNPUBLISHED, må ha
  `mainTitle`, og bestå instanstype-spesifikk validering.
- ⚠️ Det finnes **ingen** måte å publisere via PUT (`UpdatePublicationHandler` setter ikke status).
  Eneste vei er `/publish`-endepunktet.
- Implementert: `FileUploadApiService.publish(publication_identifier)`.

### ⚠️ Autorisasjon: org må matche (gjelder BÅDE upload og publish)
`TrustedThirdPartyGrantStrategy.canModify()` krever `userInstance.isExternalClient()` **og**
`topLevelOrgCristinId == resource.resourceOwner.ownerAffiliation`. Samme sjekk gjelder
`UPLOAD_FILE` og `UPDATE` (publish). Dvs. nøkkelens topp-org (`cristinOrgUri`) **må matche
publikasjonens `ownerAffiliation`** — ellers 403. Dette er den tekniske grunnen til per-institusjon-
nøkkel, ikke bare «riktig eierskap». Verifiser matchen før batch.

## Steg 3 — Rette historiske `Other`-innslag til `DLR` (BESLUTTET: vi retter dem)

«Logg-innslag» = persisterte `LogEntry`-rader (`LogEntryDao`) i **`resources`-tabellen**.
Ekstern-klient-operasjoner utleder `importSource` fra HTTP-headeren `System`
(`RequestUtil` → `ThirdPartySystem.fromValue`); mangler den, faller den tilbake til `OTHER`.
Hvert event (`FileUploadedEvent`/`PublishedResourceEvent`/`UpdatedResourceEvent`) persisteres
som et eget LogEntry, og de gamle innslagene ble laget av tidligere fil-/oppdaterings-
operasjoner uten headeren. Nye innslag fra denne kjøringen er trygge (vi sender `System: DLR`
fra `file_upload_api.py`); de gamle rettes med direkte DynamoDB-skriving (ingen API:
`UpdatePublicationRequest.importDetails` ignoreres i `generateUpdate(...)`).

### Lagringsmodell (verifisert)
- Nøkler: **PK0 = `Resource:{resourceIdentifier}`**, **SK0 = `LogEntry:{logEntryIdentifier}`**.
- `data` lagres som **nestet, ukomprimert DynamoDB-map** (i motsetning til ressursens `data`-blob
  som er zlib-komprimert). Source ligger derfor direkte på:
  **`data.importSource.source`** (verdi `"OTHER"` → skal bli `"DLR"`), `data.importSource.archive` = null.
- Gjelder både `PublicationLogEntry` og `FileLogEntry` (begge har `importSource`).

### Fikseplan (CLI-kommando `files fix-log-source` e.l.)
For hver `result_id` fra manifestene:
1. `Query` partisjonen `PK0 = "Resource:{result_id}"` med `SK0 begins_with "LogEntry:"`.
2. For hver rad med `data.importSource.source == "OTHER"`:
   `UpdateItem` med `SET #data.#importSource.#source = :dlr` (`:dlr = "DLR"`), betinget på at den
   fortsatt er `"OTHER"`.
3. **Kun** de kjente result_id-ene fra `data_to_keep*.json` (ikke en bred scan).

### Trygghet for prod-DynamoDB-skriving
- **Eksporter/backup** de berørte radene først (repoet har `dynamodb export` + `s3 get-versions`).
- `--dry-run` som default: list hva som ville endres (resourceIdentifier, logEntryIdentifier,
  topic, gammel→ny source) uten å skrive.
- **BESLUTTET:** kun `LogEntry`-radene rettes. `resourceEvent.importSource` på selve ressursen
  (i den komprimerte `data`-bloben) skal **ikke** røres.
- Bruk prod-profil eksplisitt; aldri uten godkjenning.

## ✅ Allerede gjort

`commands/services/file_upload_api.py` — kontrakt-korrekt, auth-/kilde-agnostisk kjerne, lint-ren:
- [x] `ExternalClientToken.from_key_file(path)` — leser `clientId`/`clientSecret`/`tokenUrl`/
  `customerUri`/`clientName`, henter token via client-credentials, cacher/refresher.
- [x] `FileUploadApiService(api_domain, token, system="DLR")`:
  - [x] `upload(...)` — hele `create → prepare → PUT → complete`-flyten (`ExternalCompleteUpload`,
    `fileType=OpenFile` default).
  - [x] `publish(publication_identifier)` — direkte publisering (idempotent, 409 = ok).
  - [x] `System: DLR` på alle skrive-kall via `_headers()`.
- [x] `LocalFileSource` og `S3ObjectSource` (Range-GET fra `loke.storage`).
- [x] `resolve_api_domain(session)`.
- Bruker **ikke** `ApiClient` (med vilje — feil credential).
- [x] Alle datavalg/beslutninger avklart (se «Avklart» nederst).

## 📋 TODO — CLI-kommandoer (`commands/files.py`, Click-gruppe `files`, registrer i `cli.py`)

Bruk `ctx.obj` (`AppContext`) som de andre kommandoene; `tqdm` + Rich for output.
Felles wiring: `token = ExternalClientToken.from_key_file(key_file)`,
`service = FileUploadApiService(resolve_api_domain(ctx.session), token)`,
`s3 = ctx.session.client("s3")`.

- [ ] **`files upload-one`** — test av én fil.
  `--key-file <key.json> --publication <result_id> --s3-key <uuid> [--mimetype] [--license]`.
  Bygg `S3ObjectSource(s3, "loke.storage", s3_key, ...)` → `service.upload(publication, source, ...)`.
- [ ] **`files upload-manifest`** — batch-opplasting per institusjon.
  `<data_to_keep.json> --key-file <key.json> --institution <domene[,domene]> [--dry-run] [--state <fil>]`.
  - Filtrer ressurser på `dlr_submitter_email`-domene = `--institution` (NTNU = `ntnu.no,hist.no`).
  - Per ressurs: velg content der `dlr_content_type=="file"` og ikke `generated`; for hver →
    `S3ObjectSource(s3, "loke.storage", c["dlr_content_identifier"], filename_override=c["dlr_content"],
    mimetype_override=c.get("dlr_content_mime_type"))` → `service.upload(result_id, source, license=v["license"])`.
  - Hopp over allerede ferdige `(result_id, dlr_content_identifier)` via `--state`-fil (ikke idempotent).
- [ ] **`files publish-manifest`** (eller flagg på upload) — `service.publish(result_id)` for hver
  ferdig-opplastede ressurs. Idempotent, så trygt å kjøre separat etter opplasting.
- [ ] **`files fix-log-source`** — Steg 3, retter `data.importSource.source` `OTHER`→`DLR` på
  `LogEntry`-rader for de kjente result_id-ene (se Steg 3-seksjonen). `--dry-run` default + backup først.
- [ ] **Test** `commands/services/tests/test_file_upload_api.py` med `responses` + `LocalFileSource`
  (uten nettverk). Sjekk bl.a. at `System: DLR` sendes på complete og publish.

## Avklart
- **S3-nøkkel** = `dlr_content_identifier`, ingen prefix i `loke.storage`.
- **content_type**: kun `file` lastes opp; `link`/`sharing_link` er ikke filer.
- **Utvelgelse**: `file` + ikke `generated` (master *og* ikke-master).
- **`fileType` = `OpenFile`** (filene går rett til Open).
- **Eierskap**: én kjøring per institusjon med egen nøkkelfil (NTNU-kjøringen tar også `hist.no`).
- **`System: DLR`** sendes på alle skrive-kall (rotårsak til feil source).

## Åpne spørsmål (venter på produkteier)
1. **`publisherVersion`**: skal den settes (f.eks. `PublishedVersion`), eller utelates?

## Trygghetsråd for prod-kjøring

- Test mot **én publikasjon** og verifiser at fila vises riktig **og at logg-innslaget får source
  DLR (ikke Other)** før batch (`--dry-run` + én ekte).
- Opplasting er ikke idempotent — bruk state-fil for resume og for å unngå duplikate filer.
- 5 MiB minimum per del (siste kan være mindre) — håndtert i `file_upload_api.py`.
- Verifiser at riktig nøkkel (`customerUri`) brukes for riktig institusjon **før** masseopplasting.
- Publiser først **etter** at filene er lastet opp, slik at pending filer blir med i publiseringen.
