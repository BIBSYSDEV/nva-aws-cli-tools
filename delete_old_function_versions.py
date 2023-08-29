from __future__ import absolute_import, print_function, unicode_literals
import boto3


def clean_old_lambda_versions():
    client = boto3.client('lambda')
    functions = client.list_functions()['Functions']
    for function in functions:
        versions = client.list_versions_by_function(FunctionName=function['FunctionArn'])['Versions']
        aliases = client.list_aliases(FunctionName=function['FunctionArn'])
        alias_versions = [alias['FunctionVersion'] for alias in aliases['Aliases']]
        # print('Function: {}'.format(function['FunctionArn']))
        # print('Function version: {}'.format(function['Version']))
        # print('Alias versions: {}'.format(alias_versions))
        for version in versions:
            if version['Version'] != function['Version'] and not version['Version'] in alias_versions:
                arn = version['FunctionArn']
                print('delete_function(FunctionName={})'.format(arn))
                client.delete_function(FunctionName=arn)  # uncomment me once you've checked


if __name__ == '__main__':
    clean_old_lambda_versions()

