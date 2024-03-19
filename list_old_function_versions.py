import boto3, argparse

def clean_old_lambda_versions(client, delete):
    functions_paginator = client.get_paginator('list_functions')
    version_paginator = client.get_paginator('list_versions_by_function')

    for function_page in functions_paginator.paginate():
        for function in function_page['Functions']:
                aliases = client.list_aliases(FunctionName=function['FunctionArn'])
                alias_versions = [alias['FunctionVersion'] for alias in aliases['Aliases']]
                for version_page in version_paginator.paginate(FunctionName=function['FunctionArn']):
                        for version in version_page['Versions']:
                                arn = version['FunctionArn']
                                if version['Version'] != function['Version'] and version['Version'] not in alias_versions:
                                        print('  🥊 {}'.format(arn))
                                        if delete:
                                            client.delete_function(FunctionName=arn)
                                else:
                                        print('  💚 {}'.format(arn))

if __name__ == '__main__':
    argParser = argparse.ArgumentParser()
    argParser.add_argument("-d", "--delete", action='store_true', help="delete old versions")
    args = argParser.parse_args()

    client = boto3.client('lambda', region_name='eu-west-1')
    clean_old_lambda_versions(client, args.delete)
