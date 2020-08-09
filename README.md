# Personalize_Updater

This repository aims to automate the update of the data and the model used to bring recommendations with Personalize 

## Introduction

Updating automatically Personalize enables the recommender system to keep up to date all the data and processing again the solution to make it more performant.

 

## Automatic updates

Updating Personalize is done in 3 steps with 3 different lambda functions 

- Uploading new data and creating a new import job (ImportJob)
- Creating a new solution version (UpdateSolutionVersion)
- Uploading the campaign (UpdateCampaign)

The Lambda Python functions as well as their package can be updated after creating a zip file from the packages and the python file for each folder.

### Lambda triggers

The 3 lambda functions are triggered with CloudWatch Events every week.

The minimum trigger time interval between ImportJob and UpdateCampaign is 1 hour.

The minimum trigger time interval between UpdateSolutionVersion and UpdateCampaign  is 3 hours.

This minimum interval ensures that Personalize has finished the necessary step before the next one.

### Check the execution and the logs 

To check that the functions were executed successfully check the logs of CloudWatch Log and the console of Personalize (Dataset import jobs, solution version and campaign)



## Manual update

To update Personalize manually you can use the Personalize console on the website 


### Resources

AP documentation: https://docs.aws.amazon.com/personalize

Lambda documentation: https://docs.aws.amazon.com/fr_fr/lambda/latest/dg/welcome.html