# nva-aws-cli-tools
Python scripts using aws-cli 

# Prerequisites
* python 3
* pip3 install boto3

# Scripts

## delete_old_function_versions.py
Deletes all but the current or aliased versions of any function in the current AWS account.

Inspired by [this gist](https://gist.github.com/tobywf/6eb494f4b46cef367540074512161334).