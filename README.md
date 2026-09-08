# nva-aws-cli-tools

This repository contains a CLI tool wrapping Python scripts for managing AWS resources.
These are intended for various administrative tasks related to the NVA project, as an alternative to using the AWS Console or AWS CLI directly.

## Setup with uv (optional)

This section describes how to use `uv`, a new package manager for Python.
Following this guide will set up an isolated virtual environment with the Python version specified in `.python-version`.
Ignore the section if you prefer to manage dependencies in some other way.

To use `uv`, follow the installation guide at [astral.sh/uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
# Install Python
uv python install

# Install dependencies
uv sync

# Optional install step: in VS Code, CMD+shift+p and find "shell command: install..". This is useful for editing dynamodb records

# Example: Add or remove dependencies from the project
uv add boto3 click rich
uv remove rich click

# Run a script using the managed virtual environment
uv run cli.py help
```

### Development

```bash
# Run tests
uv run pytest

# Run linter checks
uv run ruff check

# Reformat code
uv run ruff format
```

#### Pre-commit hooks

The repo has a `.pre-commit-config.yaml` that runs `ruff format` and `ruff check --fix` on staged Python files before each commit, using the ruff version pinned in `pyproject.toml`. After cloning, install the hooks once:

```bash
uvx pre-commit install
```

If the hook reformats files during a commit, the commit fails with a diff. Re-stage the changes (`git add -u`) and commit again.

#### Output vs logging in commands

Commands have two channels for text:

* `click.echo(...)` for **command output**: the data the user invoked the command to get (JSON results, identifiers, file paths).
  Goes to stdout, is unaffected by `--quiet`, and is meant to be piped (`| jq`, `> file.json`).
* `logger.info(...)` (and `logger.warning`, `logger.error`) for **status and progress messages**: which queue is being read, retry notices, deletion confirmations.
  Goes through the rich handler to stderr and is suppressed by `--quiet` / amplified by `--verbose`.

A useful test: if you pipe the command into `jq`, the result you assume is fed in is what should go through `click.echo`.
Everything else is a log line.

## Usage

Preqrequisites to use this project:

* Python 3.2 or newer
* All dependencies listed in `pyproject.toml`
* AWS credentials available

### Log in to get AWS credentials

Follow manual here:
<https://platon.sikt.no/aws/account-access>

To skip the `--profile` option, do `export AWS_PROFILE=sikt-nva-sandbox` with your preferred profile name.

## CLI

```bash
Usage: cli.py [OPTIONS] COMMAND [ARGS]...

Options:
  -v, --verbose       Verbose output
  -q, --quiet         Quiet output
  -p, --profile TEXT  Name of the local AWS profile to use
                      (default: AWS_PROFILE environment variable or "default")
  --help              Show this message and exit.

Commands:
  approvals               Manage data owned by nva-handle-service (approvals table)
  awslambda               Manage AWS Lambda functions
  cognito                 Search Cognito users
  cristin                 Cristin integration commands
  customers               Customer data validation
  dlq                     Dead letter queue handling
  handle                  Handle registration tasks
  organization-migration  Publication organization migrations
  pipelines               AWS pipeline management
  publications            Publication CRUD, export, migration
  sqs                     SQS queue management
  users                   User search and management
```

### Global Options

The `--profile` option is available at the root level and applies to all subcommands:

```bash
# Using the profile option
uv run cli.py --profile sikt-nva-sandbox users search "john"

# Using environment variable instead
export AWS_PROFILE=sikt-nva-sandbox
uv run cli.py users search "john"
```

### **CLI Commands Summary**

#### **`awslambda delete-old-versions`**

- **Description**: Cleans old versions of AWS Lambda functions.

* **Options**:
  * `--delete`: If set, deletes old Lambda function versions.

---

#### **`customers list-missing`**

* **Description**: Searches for customer references in users that do not exist in the customer table.

#### **`customers list-duplicate`**

* **Description**: Searches for duplicate customer references (same Cristin ID).

---

#### **`users search`**

* **Description**: Searches for users by user values.

* **Arguments**:
  * `search_term`: One or more terms to search for users.

#### **`users create-external`**

* **Description**: Add external API user.

* **Options**:
  * `--customer`: Customer UUID. e.g. bb3d0c0c-5065-4623-9b98-5810983c2478 [required]
  * `--intended_purpose`: The intended purpose. e.g. oslomet-thesis-integration  [required]
  * `--scopes`: Comma-separated list of scopes without whitespace, e.g., <https://api.nva.unit.no/scopes/third-party/publication-read,https://api.nva.unit.no/scopes/third-party/publication-upsert>  [required]

---

#### **`cognito search`**

* **Description**: Searches for Cognito users by attribute values.

* **Arguments**:
  * `search_term`: One or more terms to search for users.

---

#### **`handle prepare`**

* **Description**: Prepares handle tasks based on DynamoDB data.

* **Options**:
  * `--customer`: Customer UUID (required).
  * `--resource-owner`: Resource owner ID (required).
  * `--output-folder`: Path to save output files (optional).

#### **`handle execute`**

* **Description**: Executes handle tasks from prepared files.

* **Options**:
  * `--input-folder`: Path to the folder containing input files (required).

---

#### **`organization-migration list-publications`**

* **Description**: List all publication that are affected by an organization change for a given organization identifier either through contributor or resource owner affiliation.

* **Options**:
  * `--filename`: The name of the file to write the report to. The default is `report.json`.

* **Arguments**:
  * `organization identifier`, e.g. 7497.6.4.0 (required)

* **Examples**:
  * `> uv run cli.py --profile sikt-nva-sandbox organization-migration list-publications 7497.6.4.0 --filename=report-7497.6.4.0.json`

#### **`organization-migration update-publications`**

* **Description**: Updates all publication based on a report generated by `list-publications`.

* **Options**:
  * `--filename`: The name of the file to read the report from. The default is `report.json`.

* **Arguments**:
  * `old organization identifier`, e.g. 7497.6.4.0 (required)
  * `new organization identifier`, e.g. 7497.6.6.0 (required)

* **Examples**:
  * `> uv run cli.py --profile sikt-nva-sandbox organization-migration update-publications 7497.6.4.0 7497.6.6.0 --filename=report-7497.6.4.0.json`

---

#### **`publications logs`**

* **Description**: Export log entries for a publication to a JSON file.

* **Arguments**:
  * `publication_identifier`: The publication identifier (required)

* **Options**:
  * `--output`: Output file path (default: `{identifier}.json` in current directory)

* **Examples**:
  * `> uv run cli.py --profile sikt-nva-sandbox publications logs 019aa050798d-54f5e9a6-2f77-47f3-b59a-0c78d60728db`
  * `> uv run cli.py --profile sikt-nva-sandbox publications logs 019aa050798d-54f5e9a6-2f77-47f3-b59a-0c78d60728db --output /tmp/logs.json`

* **Output**: JSON file with structure:

  ```json
  {
    "identifier": "...",
    "exportedAt": "2025-12-16T14:30:00Z",
    "count": 15,
    "logEntries": [...]
  }
  ```

---

#### **`approvals policies`**

* **Description**: Manages the `IdentifierPolicy` rows in the approvals DynamoDB table owned by
  [nva-handle-service](https://github.com/BIBSYSDEV/nva-handle-service). A policy lists which identifier names
  (e.g. `ctis`, `dmp`, `rek`) a customer may use when creating approvals. There is at most one policy per customer,
  stored under `PK0=Customer:<customer-uuid>` / `SK0=IdentifierPolicy`. Names are trimmed and lower-cased before they
  are stored, matching the normalization in nva-handle-service.

* **Common options** (every subcommand):
  * `--table`: Substring of the approvals table name (default `nva-approvals-`). Make it more specific if several
    stacks are deployed in the account.

* **Arguments**:
  * `customer_identifier`: The customer UUID, or the full customer URI (`https://api.nva.unit.no/customer/<uuid>`).

* **Subcommands**:
  * `list [--json]`: Show every policy with the customer name (resolved from the customers table when available).
  * `get <customer_identifier>`: Print one policy as JSON.
  * `add <customer_identifier> <name>...`: Create the policy. Fails if the customer already has one.
  * `update <customer_identifier> [--add <name>]... [--remove <name>]...`: Add and/or remove allowed names.
  * `delete <customer_identifier> [--yes]`: Delete the policy (asks for confirmation unless `--yes`).

* **Examples**:
  * `> uv run cli.py approvals policies list`
  * `> uv run cli.py approvals policies add f8a1c0e2-3b4d-4a5e-9c7f-1d2e3f4a5b6c ctis dmp`
  * `> uv run cli.py approvals policies update f8a1c0e2-3b4d-4a5e-9c7f-1d2e3f4a5b6c --add rek --remove dmp`
  * `> uv run cli.py approvals policies delete f8a1c0e2-3b4d-4a5e-9c7f-1d2e3f4a5b6c --yes`
