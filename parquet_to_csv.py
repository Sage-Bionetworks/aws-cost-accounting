import pandas as pd

PREFIX = '2021Oct_CUR'
CUR_FILE = PREFIX+'.parquet'
OUTPUT_FILE = PREFIX+"_sandbox.tsv"

# These are the accounts that require special handling
SCICOMP_ACCOUNT =       '055273631518'
SANDBOX_ACCOUNT =       '563295687221'
SCIPOOL_PROD_ACCOUNT =  '237179673806'
SYNAPSE_PROD_ACCOUNT = '325565585839'

TOP_MIXED_ACCOUNT_IDS = [SCICOMP_ACCOUNT, SANDBOX_ACCOUNT, SCIPOOL_PROD_ACCOUNT]

STRUCTURED_COMPUTE_ACCOUNT_IDS = [SCICOMP_ACCOUNT, SCIPOOL_PROD_ACCOUNT]

ACCOUNT_ID_COLUMN = 'line_item_usage_account_id'
VALUE_COLUMN = 'line_item_unblended_cost'
PROJECT_COLUMN = 'resource_tags_user_project'
PRODUCT_NAME_COLUMN = 'product_product_name'
DISPLAY_VALUE_COLUMN = 'Cost'
DISPLAY_PROJECT_COLUMN = 'Project'
USAGE_TYPE = 'line_item_usage_type'
DESCRIPTION = 'line_item_line_item_description'
PRODUCT_CODE_COLUMN = "line_item_product_code"



def main():

	df = pd.read_parquet(CUR_FILE)
	
	# Just get the rows for Sandbox
	acct_rows = df.loc[df.apply(
		lambda x: x[ACCOUNT_ID_COLUMN] in [SANDBOX_ACCOUNT] 
		and x[PRODUCT_CODE_COLUMN] in ['AmazonEC2']
	, axis=1)]
	
	# show all values for one column
	cols=[
		"bill_billing_entity",
	 	"line_item_line_item_type",
	 	PRODUCT_CODE_COLUMN,
	 	#line_item_usage_type
	 	#line_item_operation
	 	#line_item_line_item_description
	 	"line_item_tax_type",
	 	#"product_product_name", # This just looks like a readable version of line_item_product_code
	 	"product_product_family",
	 	"product_servicecode",
	 	"product_usagetype",
	 	"resource_tags_aws_created_by"
	]
	for col in cols:
		print(f"{col}: {set(acct_rows[col])}")

	
	#acct_rows.to_csv(OUTPUT_FILE, sep="\t")

	
	
if __name__ == "__main__":
    main()
