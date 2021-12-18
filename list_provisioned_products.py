import boto3


def main():
	session = boto3.Session(profile_name='scipool-prod-sc-reader')
	client = session.client('servicecatalog', 'us-east-1')
	next_page_token = None
	for i in range(1000):
		if next_page_token is None:
			response = client.scan_provisioned_products(
    			AccessLevelFilter={'Key':'Account', 'Value':'self'}
			)
		else:
			response = client.scan_provisioned_products(
    			AccessLevelFilter={'Key':'Account', 'Value':'self'},
    			PageToken=next_page_token
			)
		for pp in response['ProvisionedProducts']:
			#pp_metadata = client.describe_provisioned_product(Id=pp['Id'])
			tag_option = client.describe_tag_option(Id=pp['Id'])
			print(f'{pp["Name"]}, tag-option: {tag_option}')
		if not 'NextPageToken' in response:
			break
		next_page_token=response['NextPageToken']
if __name__ == "__main__":
    main()

