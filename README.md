# nva-aws-cli-tools

Python scripts using aws-cli

## Prerequisites

* python 3.2 or newer
* pip3 install boto3
* aws credentials available

## Setup with uv (experimental)

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

### Log in to get AWS credentials

Follow manual here:
<https://platon.sikt.no/aws/account-access>

To to skip --profile option, do `export AWS_PROFILE=LimitedAdmin-123456789000` with your prefered account number 

## CLI 

```
Usage: cli.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  awslambda
  cognito
  customers
  handle
  users
```

### **CLI Commands Summary**

#### **`awslambda delete-old-versions`**
- **Description**: Cleans old versions of AWS Lambda functions.
- **Options**:
  - `--profile`: Specifies the AWS profile to use (defaults to "default"). Will use the `AWS_PROFILE` environment variable if available.
  - `--delete`: If set, deletes old Lambda function versions.

---

#### **`customers list-missing`**
- **Description**: Searches for customer references in users that do not exist in the customer table.
- **Options**:
  - `--profile`: Specifies the AWS profile to use (defaults to "default"). Will use the `AWS_PROFILE` environment variable if available.

#### **`customers list-duplicate`**
- **Description**: Searches for duplicate customer references (same Cristin ID).
- **Options**:
  - `--profile`: Specifies the AWS profile to use (defaults to "default"). Will use the `AWS_PROFILE` environment variable if available.

---

#### **`users search`**
- **Description**: Searches for users by user values.
- **Options**:
  - `--profile`: Specifies the AWS profile to use (defaults to "default"). Will use the `AWS_PROFILE` environment variable if available.
- **Arguments**:
  - `search_term`: One or more terms to search for users.

#### **`users create-external`**
- **Description**: Add external API user.
- **Options**:
  - `--profile`: Specifies the AWS profile to use (defaults to "default"). Will use the `AWS_PROFILE` environment variable if available.
  - `--customer`: Customer UUID. e.g. bb3d0c0c-5065-4623-9b98-5810983c2478 [required]
  - `--intended_purpose`: The intended purpose. e.g. oslomet-thesis-integration  [required]
  - `--scopes`: Comma-separated list of scopes without whitespace, e.g., https://api.nva.unit.no/scopes/third-party/publication-read,https://api.nva.unit.no/scopes/third-party/publication-upsert  [required]

---

#### **`cognito search`**
- **Description**: Searches for Cognito users by attribute values.
- **Options**:
  - `--profile`: Specifies the AWS profile to use (defaults to "default"). Will use the `AWS_PROFILE` environment variable if available.
- **Arguments**:
  - `search_term`: One or more terms to search for users.

---

#### **`handle prepare`**
- **Description**: Prepares handle tasks based on DynamoDB data.
- **Options**:
  - `--profile`: Specifies the AWS profile to use (defaults to "default"). Will use the `AWS_PROFILE` environment variable if available.
  - `--customer`: Customer UUID (required).
  - `--resource-owner`: Resource owner ID (required).
  - `--output-folder`: Path to save output files (optional).

#### **`handle execute`**
- **Description**: Executes handle tasks from prepared files.
- **Options**:
  - `--profile`: Specifies the AWS profile to use (defaults to "default"). Will use the `AWS_PROFILE` environment variable if available.
  - `--input-folder`: Path to the folder containing input files (required).
