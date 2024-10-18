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
uv run roles.py help
```

### Log in to get AWS credentials

Follow manual here:
<https://gitlab.sikt.no/platon/aws-cli-tools/-/tree/master/samlauth>

## Scripts

### list_old_function_versions.py

```bash
> python3 list_old_function_versions.py -h
> usage: list_old_function_versions.py [-h] [-d]

options:
  -h, --help    show this help message and exit
  -d, --delete  delete old versions
```

### roles.py

```bash
python3 roles.py help           

    Please, specify one of the following actions:

    - 'read' to get user roles. 
      Use it as: python3 roles.py read [key] > output.json
      This will return a JSON of roles for a given key. The output is redirected to output.json file.

    - 'write' to write roles from a file to a user. 
      Use it as: python3 roles.py write [key] [filename]
      This will read a JSON file of roles and write them to a user defined by the key. 
      The filename should be a JSON file with the roles.

    - 'lookup' to lookup a value in the whole table. 
      Use it as: python3 roles.py lookup [value]
      It will return a list of items where at least one attribute contains the given value. 
      The items are returned as a list of dictionaries with 'PrimaryKeyHashKey', 'givenName', 
      and 'familyName' as keys.
      
    - 'clookup' to perform a lookup directly in cognito user pool. 
      Use it as: python3 roles.py clookup [value]
      This will return a list of users where at least one attribute contains the given value. 
      The users are returned as a serialized JSON string.
      
    - 'help' to see this message again.
```

Lists all but the current and aliased versions of any function in the current AWS account.
Use the `-d` or `--delete` command line option to delete the function versions.

Inspired by [this gist](https://gist.github.com/tobywf/6eb494f4b46cef367540074512161334).
