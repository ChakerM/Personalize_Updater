import sys
import logging
import json
import boto3
from tzlocal import get_localzone
tz = get_localzone()
import datetime


'''PARAMETERS'''

''' The campaign arn and its corresponding solution version arn must match the same position in the lists arnCampaignList and arnSolutionList'''
#List of the campaign arn
arnCampaignList = []
#List of the solutions
arnSolutionList = []



logger = logging.getLogger()

def latestSolutionARN(solutionArn):
	#return the latest solution version ARN
		DateCompared =  tz.localize(datetime.datetime(2020, 1, 1, 10, 10, 10, 584000))
		ArnLastSaved = ""
		for i in range(len(personalize.list_solution_versions(solutionArn = solutionArn)["solutionVersions"])):
		    tmpDate = personalize.list_solution_versions(solutionArn = solutionArn)["solutionVersions"][i]["creationDateTime"]
		    if tmpDate > DateCompared:
		        DateCompared = tmpDate
		        ArnLastSaved = personalize.list_solution_versions(solutionArn = solutionArn)["solutionVersions"][i]["solutionVersionArn"]
	return ArnLastSaved


def lambda_handler(event, context):
	'''  Update Campaign '''

	personalize = boto3.client('personalize')

	for i in range(len(arnSolutionList)):
		LastSolutionVersionARN = latestSolutionARN(arnSolutionList[i])
		print("Last solution version ARN" + LastSolutionVersionARN)

		response = personalize.update_campaign(campaignArn = arnCampaignList[i], solutionVersionArn= LastSolutionVersionARN, minProvisionedTPS = 1)
		arnCampaign = response['campaignArn']
		description = personalize.describe_campaign(campaignArn = arnCampaign)['campaign']
		print('Name: ' + description['name'])
		print('ARN: ' + description['campaignArn'])
		print('Status: ' + description['status'])

	return { 'statusCode': 200, 'body': json.dumps('hello world!') }