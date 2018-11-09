import boto3
import redis
# Needs to do the following
#  
# - Insert Redis string key "s3_endpoint" with static s3 website endpoint (from CFN)
# - Update Dashboard HTML with API GW Endpoint (from CFN)
# - Copy Dashboard HTML to S3 bucket (s3 bucket name from CFN)

cfn = boto3.client('cloudformation')
cfn_response = cfn.describe_stacks(StackName="RedisWorkshop")
print(cfn_response)
