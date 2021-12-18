import pandas as pd
from IPython.display import display

# TODO CUR report will be read from AWS, not from a local file
#CUR_FILE = '2021Sept_CUR.parquet'
CUR_FILE = '2021Oct_CUR.parquet'

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

ACCOUNTS_FILE = 'aws_accounts.tsv'

MAIN_PRODUCTS = ['Amazon Elastic Compute Cloud', 
	'Amazon Simple Storage Service', 'AWS Support (Developer)', 
	'Amazon DocumentDB (with MongoDB compatibility)']

# any items having one of these values for 'resource_tags_user_project' is an indirect cost
SCICOMP_INDIRECT_TAGS = ['Infrastructure', 'Infra', 'Infrastucture', 'General']
INDIRECT_LABEL = 'INDIRECT'

def main():
	# Read in the account labels, for those accounts whose costs are all allocated the same way
	accounts_columns = {
		'account_id':'str',
		'account_name':'str',
		'account_label':'str'
	}
	accounts = pd.read_csv(ACCOUNTS_FILE, sep='\t', usecols=accounts_columns.keys(), dtype=accounts_columns)	

	# Read in the month's cost and usage report
	columns=[
  		VALUE_COLUMN,
  		ACCOUNT_ID_COLUMN,
  		PROJECT_COLUMN,
  		PRODUCT_NAME_COLUMN,
  		USAGE_TYPE,
  		DESCRIPTION
	]
	df = pd.read_parquet(CUR_FILE, columns=columns)
	
	#acct_row_indices = [df.at[i,ACCOUNT_ID_COLUMN]==SYNAPSE_PROD_ACCOUNT for i in df.index]
	#acct_rows = df[acct_row_indices]
	
	acct_rows = df.loc[df[ACCOUNT_ID_COLUMN].apply(lambda x: x in TOP_MIXED_ACCOUNT_IDS)]

	# Compute total cost for the month
	total_cost = acct_rows[VALUE_COLUMN].sum()
		
	# we now want to inner join df with accounts on ACCOUNT_ID_COLUMN <-> 'account_id'
	accounts = accounts.rename(columns={'account_id': ACCOUNT_ID_COLUMN})
	joined = acct_rows.join(accounts.set_index(ACCOUNT_ID_COLUMN), on=ACCOUNT_ID_COLUMN, how="inner")
	
	# group expenses by their category.  See group_by() for details
	gb = acct_rows.groupby((lambda row_index: break_down_sandbox_by_tag_and_product(joined,row_index)), dropna=False, group_keys=False)
	
	# sum up each category
	summarized = gb.sum().sort_values(VALUE_COLUMN)
	
	# sort, descending
	sorted = summarized.sort_values(VALUE_COLUMN, ascending=False)[[VALUE_COLUMN]]
	
	# display the result
	sorted = sorted.rename(columns={VALUE_COLUMN:DISPLAY_VALUE_COLUMN})
	with pd.option_context( 'display.precision', 2),\
		pd.option_context('display.max_rows', 500),\
		pd.option_context('display.max_colwidth', 200),\
		pd.option_context('display.float_format', (lambda x : f'${x:.2f}')):
		display(sorted)
		
	total = sorted[DISPLAY_VALUE_COLUMN].sum()
	if (abs(total-total_cost)>0.01):
		raise Exception("categorized costs to not add up to total bill.")
	print(f"\nTotal: ${total:.2f}")
	

def group_by_product_type(df, row_index): 
	product = df.at[row_index, PRODUCT_NAME_COLUMN]
	if product not in MAIN_PRODUCTS:
		product = "Other"
	return product

def group_by_tag_for_main_products(df, row_index): 
	product = df.at[row_index, PRODUCT_NAME_COLUMN]
	if product not in MAIN_PRODUCTS:
		return( "Not a major product" )
	project = df.at[row_index, PROJECT_COLUMN]
	if project is None:
		return("No project tag")
	return "Has project tag"
	
def group_by_account_and_tag(df, row_index):
	project = df.at[row_index, PROJECT_COLUMN]
	product = df.at[row_index, PRODUCT_NAME_COLUMN]
	if product not in MAIN_PRODUCTS:
		product = "Other"
	if project is None:
		project = 'NO TAG'
	return df.at[row_index, 'account_name']+' '+project+' '+product


def break_down_sandbox_by_tag_and_product(df, row_index):
	account = df.at[row_index, 'account_name']
	if account != 'Sandbox':
		return 'Other account'
	project = df.at[row_index, PROJECT_COLUMN]
	product = df.at[row_index, PRODUCT_NAME_COLUMN]
	if product not in MAIN_PRODUCTS:
		return "Other AWS products"
	if project is None:
		project = 'NO TAG'
	return project+' '+product

def group_by_account_and_ec2_details(df, row_index):
	account = df.at[row_index, 'account_name']
	project = df.at[row_index, PROJECT_COLUMN]
	product = df.at[row_index, PRODUCT_NAME_COLUMN]
	usage_type = df.at[row_index, USAGE_TYPE]
	description = df.at[row_index, DESCRIPTION]
	if description.startswith("Tax"):
		description = "TAX"
	elif description.find("provisioned storage")!=-1:
		description = "provisioned storage"
	elif description.find("snapshot data")!=-1:
		description = "snapshot data"

	if product=='Amazon Elastic Compute Cloud' and project is None:
		return account+" untagged EC2 "+description
	return account+" well-tagged or not EC2"
	
	
if __name__ == "__main__":
    main()
