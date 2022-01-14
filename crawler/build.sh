#! /bin/bash

accountid=`aws sts get-caller-identity --query Account --output text`

docker build . -t ratometer/crawler
docker tag ratometer/crawler:latest $accountid.dkr.ecr.ap-southeast-2.amazonaws.com/ratometer/crawler:latest
aws ecr get-login-password --region ap-southeast-2 | docker login --username AWS --password-stdin $accountid.dkr.ecr.ap-southeast-2.amazonaws.com
docker push $accountid.dkr.ecr.ap-southeast-2.amazonaws.com/ratometer/crawler:latest

