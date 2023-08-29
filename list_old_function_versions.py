from __future__ import absolute_import, print_function, unicode_literals
import boto3, argparse

def list_or_delete_old_lambda_versions(delete):
    client = boto3.client('lambda')
    functions = client.list_functions()['Functions']
    for function in functions:
        versions = client.list_versions_by_function(FunctionName=function['FunctionArn'])['Versions']
        aliases = client.list_aliases(FunctionName=function['FunctionArn'])
        alias_versions = [alias['FunctionVersion'] for alias in aliases['Aliases']]
        for version in versions:
            if version['Version'] != function['Version'] and not version['Version'] in alias_versions:
                arn = version['FunctionArn']
                if (delete):
                    client.delete_function(FunctionName=arn)
                    print('Deleted {}'.format(arn))
                else:
                    print(arn)


if __name__ == '__main__':
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-d", "--delete", action='store_true', help="delete old versions")

    args = argParser.parse_args()
    print("args=%s" % args)

    list_or_delete_old_lambda_versions(args.delete)

