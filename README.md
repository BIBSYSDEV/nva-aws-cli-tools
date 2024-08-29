# nva-aws-cli-tools
Python scripts using aws-cli 

# Prerequisites
* python 3.2 or newer
* pip3 install boto3
* aws credentials available

# Log in to get AWS credentials

Follow manual here:
https://gitlab.sikt.no/platon/aws-cli-tools/-/tree/master/samlauth


# Scripts

## list_old_function_versions.py
```
> python3 list_old_function_versions.py -h
> usage: list_old_function_versions.py [-h] [-d]

options:
  -h, --help    show this help message and exit
  -d, --delete  delete old versions
```
## roles.py
```
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

    - 'help' to see this message again.
```

Lists all but the current and aliased versions of any function in the current AWS account.
Use the `-d` or `--delete` command line option to delete the function versions.

Inspired by [this gist](https://gist.github.com/tobywf/6eb494f4b46cef367540074512161334).
