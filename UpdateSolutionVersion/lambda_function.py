import sys
import logging
import json
import boto3


'''PARAMETERS'''

#DataSetGroupName
DataSetGroupArn = ""


logger = logging.getLogger()

def lambda_handler(event, context):
    
    '''  Create solution version '''

    personalize = boto3.client('personalize')

    response = personalize.list_solutions( datasetGroupArn=DataSetGroupArn)



    # for every solution 
    for i in range(len(response["solutions"])):
        
        solution_arn = response["solutions"][i]["solutionArn"]
        print("Solution arn: " + solution_arn)

        # Create solution version
        create_solution_version_response = personalize.create_solution_version( solutionArn= solution_arn )

        solution_version_arn = create_solution_version_response['solutionVersionArn']

        print(json.dumps(create_solution_version_response, indent=2))
        


    return {'statusCode': 200, 'body': json.dumps('hello world!')}
