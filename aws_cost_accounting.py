import pandas as pd
import math
from IPython.display import display

# TODO CUR report will be read from AWS, not from a local file
CUR_FILE = '2021Sept_CUR.parquet'

ACCOUNTS_FILE = 'aws_accounts.tsv'

SCICOMP_ACCOUNT =       '055273631518'
SANDBOX_ACCOUNT =       '563295687221'
SCIPOOL_PROD_ACCOUNT =  '237179673806'

ACCOUNT_ID_COLUMN = 'line_item_usage_account_id'
VALUE_COLUMN = 'line_item_unblended_cost'
PROJECT_COLUMN = 'resource_tags_user_project'
DISPLAY_VALUE_COLUMN = 'Cost'
DISPLAY_PROJECT_COLUMN = 'Project'

# any items having one of these values for 'resource_tags_user_project' is an indirect cost
SCICOMP_INDIRECT_TAGS = ['Infrastructure', 'Infra', 'Infrastucture', 'General']
INDIRECT_LABEL = 'INDIRECT'

def main():
	accounts_columns = {
		'account_id':'str',
		'account_name':'str',
		'account_label':'str'
	}
	accounts = pd.read_csv(ACCOUNTS_FILE, sep='\t', usecols=accounts_columns.keys(), dtype=accounts_columns)	
	accounts = accounts[['account_id','account_label']]

	columns=[
  		VALUE_COLUMN,
  		ACCOUNT_ID_COLUMN,
  		PROJECT_COLUMN
	]
	df = pd.read_parquet(CUR_FILE, columns=columns)
	
	# Compute total cost
	total_cost = df[VALUE_COLUMN].sum()
	
	# we now want to inner join df with accounts on ACCOUNT_ID_COLUMN <-> 'account_id'
	accounts = accounts.rename(columns={'account_id': ACCOUNT_ID_COLUMN})
	joined = df.set_index(ACCOUNT_ID_COLUMN).join(accounts.set_index(ACCOUNT_ID_COLUMN), on=ACCOUNT_ID_COLUMN, how="inner")
	
	# group expenses by their category.  See group_by() for details
	gb = joined.groupby((lambda row_index: group_by(joined,row_index)), dropna=False, group_keys=False)
	# sum up each category
	summarized = gb.sum().sort_values(VALUE_COLUMN)
	# sort, descending
	sorted = summarized.sort_values(VALUE_COLUMN, ascending=False)[[VALUE_COLUMN]]
	# display the result
	sorted = sorted.rename(columns={VALUE_COLUMN:DISPLAY_VALUE_COLUMN})
	with pd.option_context( 'display.precision', 2),\
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


def group_by(df, row_index): 
	if df.at[row_index, 'account_label'][0] != 'na':
		return df.at[row_index, 'account_label'][0]
	# if it IS a 'na' then we are in one of the accounts that needs further evaluation
	if df.at[row_index, PROJECT_COLUMN][0] is None or df.at[row_index, PROJECT_COLUMN][0].isna():
		return('INDIRECT') # these are costs we can't map, so we just call them indirect
	if df.at[row_index, PROJECT_COLUMN][0] in SCICOMP_INDIRECT_TAGS:
		return('INDIRECT') # these are tagged with an indirect tag
	return df.at[row_index, PROJECT_COLUMN][0] # these have a direct tag

	
if __name__ == "__main__":
    main()
