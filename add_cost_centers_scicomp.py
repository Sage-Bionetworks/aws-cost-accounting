#
# Given an AWS account, iterate through the EC2 instances and
# S3 buckets. For each resource that has Project tag in the
# project-to-cost-center map, add a CostCenter tag using the 
# value drawn from the map
#
	
import boto3
import pandas as pd
from botocore.exceptions import ClientError

PROFILE_NAME = 'scicomp-tagger'

PROJECT_TO_COST_CENTER_FILE = 'project_to_cost_center.tsv'
DRY_RUN = False

def main():
	# This specifies the account to process
	
	# Read in the map of Project to CostCenter
	pcc_columns = {
		'Project':'str',
		'CostCenter':'str'
	}
	pcc_dict = {}
	pcc = pd.read_csv(PROJECT_TO_COST_CENTER_FILE, sep='\t', usecols=pcc_columns.keys(), dtype=pcc_columns)	
	for index, row in pcc.iterrows():
		pcc_dict[row['Project']] = row['CostCenter']   
    
	#
	# First do the EC2 instances
	#
	print("\nEC2\n")
	session = boto3.Session(profile_name=PROFILE_NAME)
	client = session.client('ec2', 'us-east-1')
	next_page_token = None
	filter = [
        {'Name': 'tag-key',
            'Values': ['Project']}
    ]
	for i in range(1000):
		if next_page_token is None:
			response = client.describe_instances(
				Filters = filter,
				MaxResults=50
			)
		else:
			response = client.describe_instances(
				Filters = filter,
				MaxResults=50,
				NextToken=next_page_token
			)
		for rs in response['Reservations']:
			for instance in rs['Instances']:
				owner_email = None
				project = None
				cost_center = None
				for tag in instance['Tags']:
					key = tag['Key']
					value = tag['Value']
					if key=='OwnerEmail':
						owner_email = value
					if key=='Project':
						project = value
					if key=='CostCenter':
						cost_center = value
				if not (project is None) and project in pcc_dict:
					if not (cost_center is None):
						print(f'{instance["InstanceId"]} already has cost center {cost_center}')
						continue
					cc = pcc_dict[project]
					print(f'Instance: {instance["InstanceId"]}, OwnerEmail: {owner_email}, project: {project}, cost center: {cc}')
					if not DRY_RUN:
						client.create_tags(
    						DryRun=DRY_RUN,
    						Resources=[instance['InstanceId']],
    						Tags=[{'Key': 'CostCenter','Value': cc}]
						)
		if not 'NextToken' in response:
			break
		next_page_token=response['NextToken']
		
    
	#
	# Now do the S3 buckets
	#
	print("\nS3\n")
	session = boto3.Session(profile_name=PROFILE_NAME)
	client = session.client('s3', 'us-east-1')
	next_page_token = None

	response = client.list_buckets()

	for bucket in response['Buckets']:
		bucket_name = bucket['Name']
		try:
			response = client.get_bucket_tagging(Bucket=bucket_name)
		except ClientError as e:
			msg = str(e)
			if msg.find("NoSuchTagSet")<0:
				raise e
			response = {'TagSet':[]}
			
		if not ('TagSet' in response):
			#print(f"No tags for bucket: {bucket_name}")
			continue
		tags = response['TagSet']
		#print(f'bucket: {bucket_name}, tags:{tags}')

		owner_email = None
		project = None
		cost_center = None
		for tag in tags:
			key = tag['Key']
			value = tag['Value']
			if key=='OwnerEmail':
				owner_email = value
			if key=='Project':
				project = value
			if key=='CostCenter':
				cost_center = value
		if not (project is None) and project in pcc_dict:
			if not (cost_center is None):
				print(f'{bucket_name} already has cost center {cost_center}')
				continue
			cc = pcc_dict[project]
			print(f'{bucket_name}, owner: {owner_email}, project: {project}, cost center: {cc}')
			tags.append({'Key':'CostCenter','Value':cc})
			if not DRY_RUN:
				client.put_bucket_tagging(
	    				Bucket=bucket_name,
	    				Tagging={'TagSet': tags},
				)
		
if __name__ == "__main__":
    main()

