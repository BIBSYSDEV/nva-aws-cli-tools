# Tilgangsrapporter (roller per bruker)

Kort oppskrift for å lage tilgangsrapportene (Excel) som sendes til Jan Erik.

## Forutsetninger

- `AWS_PROFILE` må peke på riktig miljø (typisk prod).
- Avhengigheter installert: `uv sync`.

## Enkleste vei: kjør scriptet

Scriptet lager alle rapportene i én kjøring, datostemplet med dagens dato:

```bash
AWS_PROFILE=sikt-nva-prod ./docs/generate-access-reports.sh
# eller legg filene i en egen mappe:
AWS_PROFILE=sikt-nva-prod ./docs/generate-access-reports.sh reports/
```

Rapporter som lages:

| Rapport | Filnavn | Filter |
| --- | --- | --- |
| App-admin | `nva-app-admin-<dato>.xlsx` | `--include-roles "App-admin"` |
| Redaktører | `nva-redaktor-<dato>.xlsx` | `--include-roles "Editor"` |
| Institusjonsadmin | `nva-admin-<dato>.xlsx` | `--include-roles "Institution-admin"` |
| Internal-importer | `nva-internal-importer-<dato>.xlsx` | `--include-roles "Internal-importer"` |
| Alle kuratorer | `nva-curators-<dato>.xlsx` | alle roller som inneholder «Curator» |
| Alle unntatt kun Creator | `nva-all-except-only-creator-<dato>.xlsx` | `--exclude-only-roles "Creator"` |

## Manuelt / enkeltrapporter

Se først hvilke roller som finnes og hvor mange brukere hver har:

```bash
uv run cli.py users role-summary
```

Eksporter én eller flere roller (kommaseparert). `--include-roles` matcher **eksakt**
rollenavn — så alle kurator-typer må listes eksplisitt:

```bash
uv run cli.py users export-roles \
  --include-roles "Nvi-Curator,Support-Curator,Publishing-Curator,Doi-Curator,Curator-thesis,Curator-thesis-embargo" \
  --output nva-curators-2026-08-19.xlsx
```

Alle brukere med minst én tildelt rolle utover Creator (dvs. ekskluder de som *kun* har Creator):

```bash
uv run cli.py users export-roles \
  --exclude-only-roles "Creator" \
  --output nva-all-except-only-creator-2026-08-19.xlsx
```

## Merknader

- `--include-roles` og `--exclude-only-roles` kan ikke brukes samtidig.
- `--include-roles` er eksakt match, ikke «contains». Nye kurator-roller må
  legges til i listen over `CURATOR_ROLES` i scriptet ved behov — sjekk mot
  `role-summary`.
- En bruker med flere roller telles kun én gang i hver rapport.
