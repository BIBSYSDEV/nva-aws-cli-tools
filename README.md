# nva-aws-cli-tools
Python scripts using aws-cli 

# Prerequisites
* python 3.2 or newer
* pip3 install boto3

# Scripts

## list_old_function_versions.py
```
> python3 list_old_function_versions.py -h
> usage: list_old_function_versions.py [-h] [-d]

options:
  -h, --help    show this help message and exit
  -d, --delete  delete old versions
```

Lists all but the current and aliased versions of any function in the current AWS account.
Use the `-d` or `--delete` command line option to delete the function versions.

Inspired by [this gist](https://gist.github.com/tobywf/6eb494f4b46cef367540074512161334).