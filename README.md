# RAT-O-Meter Service

## Introduction

This service will used data from [https://findarat.com.au](https://findarat.com.au) to generate aggergate analytics on the availability of RAT tests in retail locations around Australia.

There are two components included in this service:

 * Crawler: Periodically requests data from the Find-a-RAT site and saves as json into S3

 * Analyser: Utilises AWS Athena to generate aggergated data, saving the results into S3 for consumption from by the frontend

 The frontend is contained in a separate repository [https://github.com/aaronlecompte/rat-o-meter](https://github.com/aaronlecompte/rat-o-meter)

 ## Build instructions

 Each components is contained in a Docker image. The `build.sh` script will build the image and upload to ECR.

 The `.env.template` file can be used to author a `.env` file to fill in the details for the required AWS resources and associted environment settings.

 AWS Batch and CloudWatch events are used to schedule periodic invocations of these services.

 ## License 

 