import pandas as pd
from IPython.display import display

# TODO CUR report will be read from AWS, not from a local file
#CUR_FILE = '2021Sept_CUR.parquet'
#CUR_FILE = '2021Oct_CUR.parquet'
#CUR_FILE = '2021Nov_CUR.parquet'
CUR_FILE = '2021Dec18_PARTIAL_CUR.parquet'

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
PRODUCT_CODE_COLUMN = "line_item_product_code"
PRODUCT_USAGE_TYPE_COLUMN = "product_usagetype"
PRODUCT_LINE_ITEM_TYPE_COLUMN = "line_item_line_item_type" # tax or non/tax
CREATED_BY_COLUMN = "resource_tags_aws_created_by"
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
  		DESCRIPTION,
  		PRODUCT_USAGE_TYPE_COLUMN,
  		PRODUCT_LINE_ITEM_TYPE_COLUMN,
  		PRODUCT_CODE_COLUMN,
  		CREATED_BY_COLUMN
	]
	df = pd.read_parquet(CUR_FILE, columns=columns)
	
	#acct_rows = df.loc[df[ACCOUNT_ID_COLUMN].apply(lambda x: x in TOP_MIXED_ACCOUNT_IDS)]
	acct_rows = df.loc[df.apply(
		lambda x: x[ACCOUNT_ID_COLUMN] in [SANDBOX_ACCOUNT] 
		and x[PRODUCT_CODE_COLUMN] in ['AmazonEC2']
	, axis=1)]
	
	# Compute total cost for the month
	total_cost = acct_rows[VALUE_COLUMN].sum()
		
	# we now want to inner join df with accounts on ACCOUNT_ID_COLUMN <-> 'account_id'
	accounts = accounts.rename(columns={'account_id': ACCOUNT_ID_COLUMN})
	joined = acct_rows.join(accounts.set_index(ACCOUNT_ID_COLUMN), on=ACCOUNT_ID_COLUMN, how="inner")
	
	# group expenses by their category.  See group_by() for details
	gb = acct_rows.groupby((lambda row_index: group_by_ec2_usage(joined,row_index)), dropna=False, group_keys=False)
	
	if True:
		# add up small rows to make table legible
		sorted = merge_small_rows(gb.sum(), .99, "Other")
	else:
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
	
def merge_small_rows(df, threshold, etc_label):
    '''
    df is a data frame with numerical values in the rightmost row
    threshold is a value in [0,1], the percentage of the total to
    keep itemized. 
    
    Returns a copy of df having the rows with the largest values itemized,
    such that their total is > threshold.   The remainder goes into a final 
    row labeled with the value of the parameter 'etc_label' 
    '''
    nrow = df.shape[0]
    ncol = df.shape[1]
    col_label = df.columns[ncol-1]
    total = sum(df[col_label])
    result = df.sort_values(col_label, ascending=False)

    partial_sum = 0
    row_count = 0
    for i in range(nrow):
        if partial_sum > total * threshold:
            break
        partial_sum = partial_sum + result.iat[i,ncol-1]
        row_count = row_count + 1
    result = result.head(row_count)
    if total>partial_sum+1e-5:
        new_row = {col_label:(total-partial_sum)}
        for i in range(ncol-1):
            new_row[result.columns[i]]="-"
        new_row = pd.Series(data=new_row, name=etc_label)
        result = result.append(new_row, ignore_index=False)
    return result

def group_by_ec2_usage(df, row_index): 
	usage_type = df.at[row_index, PRODUCT_USAGE_TYPE_COLUMN]
	if usage_type is None or usage_type=="Usage":
		usage_type = ""
	is_tax = df.at[row_index, PRODUCT_LINE_ITEM_TYPE_COLUMN]
	if is_tax is None:
		is_tax = ""
	created_by = df.at[row_index, CREATED_BY_COLUMN]
	if created_by is None:
		created_by = ""
	return created_by+" "+usage_type+" "+is_tax
	

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
