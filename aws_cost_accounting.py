import pandas as pd
from IPython.display import display

# TODO CUR report will be read from AWS, not from a local file
#CUR_FILE = '2021Sept_CUR.parquet'
CUR_FILE = '2021Oct_CUR.parquet'

ACCOUNTS_FILE = 'aws_accounts.tsv'

# These are the accounts that require special handling
SCICOMP_ACCOUNT =       '055273631518'
SANDBOX_ACCOUNT =       '563295687221'
SCIPOOL_PROD_ACCOUNT =  '237179673806'

ACCOUNT_ID_COLUMN = 'line_item_usage_account_id'
VALUE_COLUMN = 'line_item_unblended_cost'
PROJECT_COLUMN = 'resource_tags_user_project'
PRODUCT_NAME_COLUMN = 'product_product_name'
DISPLAY_VALUE_COLUMN = 'Cost'
DISPLAY_PROJECT_COLUMN = 'Project'

MAIN_PRODUCTS = ['Amazon Elastic Compute Cloud', 
	'Amazon Simple Storage Service', 'AWS Support (Developer)', 
	'Amazon Comprehend', 'Amazon DocumentDB (with MongoDB compatibility)']

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
	#accounts = accounts[['account_id','account_label']]

	# Read in the month's cost and usage report
	columns=[
  		VALUE_COLUMN,
  		ACCOUNT_ID_COLUMN,
  		PROJECT_COLUMN,
  		PRODUCT_NAME_COLUMN
	]
	df = pd.read_parquet(CUR_FILE, columns=columns)
	
	# Compute total cost for the month
	total_cost = df[VALUE_COLUMN].sum()
	
	# we now want to inner join df with accounts on ACCOUNT_ID_COLUMN <-> 'account_id'
	accounts = accounts.rename(columns={'account_id': ACCOUNT_ID_COLUMN})
	joined = df.join(accounts.set_index(ACCOUNT_ID_COLUMN), on=ACCOUNT_ID_COLUMN, how="inner")
	
	# group expenses by their category.  See group_by() for details
	gb = joined.groupby((lambda row_index: group_by_account_and_project_tags(joined,row_index)), dropna=False, group_keys=False)
	
	# sum up each category
	summarized = gb.sum()
	
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
	
	
	# Outputs:
	# 1. Report for Finance on billing amount by program code, remainder being INDIRECT
	# 2. Itemize indirect costs by account
	# 3. Itemize the direct costs, by account, department, user, etc. (This can go to the PI)


# for each row (expense) in the given data frame, return its category:
# If the account for the row has a label (program code or "INDIRECT") return that label as the category
# if the PROJECT is missing for the row or PROJECT is an indirect expense, return INDIRECT
# return the PROJECT
#
def group_by_account_and_project_tags(df, row_index): 
	if df.at[row_index, 'account_label'] != 'na':
		return df.at[row_index, 'account_label']
	# if it IS a 'na' then we are in one of the accounts that needs further evaluation
	if df.at[row_index, PROJECT_COLUMN] is None:
		return('INDIRECT') # these are costs we can't map, so we just call them indirect
	if df.at[row_index, PROJECT_COLUMN] in SCICOMP_INDIRECT_TAGS:
		return('INDIRECT') # these are tagged with an indirect tag
	return df.at[row_index, PROJECT_COLUMN] # these have a direct tag

# for each row (expense) in the given data frame, return its category:
# If the account for the row has a label (program code or "INDIRECT") return that label as the category
# If the PROJECT is an indirect expense, return INDIRECT
# Otherwise return TBD
#
def group_by_agnostic(df, row_index): 
	if df.at[row_index, 'account_label'] != 'na':
		return df.at[row_index, 'account_label']
	# if it IS a 'na' then we are in one of the accounts that needs further evaluation
	if df.at[row_index, PROJECT_COLUMN] is None:
		return('TBD') # these are costs we can't map, so we acknowledge that we don't know
	if df.at[row_index, PROJECT_COLUMN] in SCICOMP_INDIRECT_TAGS:
		return('INDIRECT') # these are tagged with an indirect tag
	return 'TBD'

def group_by_account(df, row_index): 
	return df.at[row_index, 'account_name']

def group_by_account_category(df, row_index): 
	label = df.at[row_index, 'account_label']
	if label=="na":
		return "Mixed"
	if label=="INDIRECT":
		return "Indirect"
	return "Direct"

def group_by_product_type(df, row_index): 
	label = df.at[row_index, 'account_label']
	if label=="na":
		product = df.at[row_index, PRODUCT_NAME_COLUMN]
		if product not in MAIN_PRODUCTS:
			product = "Other"
		return df.at[row_index, 'account_name']+' '+product
	if label=="INDIRECT":
		return "Indirect"
	return "Direct"

	
if __name__ == "__main__":
    main()
