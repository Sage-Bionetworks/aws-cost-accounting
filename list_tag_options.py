import boto3


def main():
	session = boto3.Session(profile_name='scipool-prod-sc-reader')
	client = session.client('servicecatalog', 'us-east-1')
	project_tag_options = {}
	next_page_token = None
	for i in range(1000):
		if next_page_token is None:
			response = client.list_tag_options()
		else:
			response = client.list_tag_options(
    			PageToken=next_page_token
			)
		for tod in response['TagOptionDetails']:
			#print(f'{tod}')
			if tod['Key']=='Project':
				# this creates a map from TagOption to Project name
				project_tag_options[tod['Id']] = tod['Value']
		if not 'NextPageToken' in response:
			break
		next_page_token=response['NextPageToken']

	for tod_id in project_tag_options.keys():
		project = project_tag_options[tod_id]
		next_page_token = None
		for i in range(1000):
			if next_page_token is None:
				response = client.list_resources_for_tag_option(TagOptionId=tod_id)
			else:
				response = client.list_resources_for_tag_option(TagOptionId=tod_id, PageToken=next_page_token)
			for resource in response['ResourceDetails']:
				print(f'{project}: {resource["Name"]}')
			if not 'NextPageToken' in response:
				break
			next_page_token=response['NextPageToken']

if __name__ == "__main__":
    main()

