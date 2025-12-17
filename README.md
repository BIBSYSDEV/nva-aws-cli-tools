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

# The pipelines command supports comma-separated profiles
uv run cli.py --profile sikt-nva-dev,sikt-nva-test pipelines branches
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
