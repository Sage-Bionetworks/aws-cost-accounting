import pandas as pd
from IPython.display import display

# TODO CUR report will be read from AWS, not from a local file
CUR_FILE = '2021Sept_CUR.parquet'

ACCOUNTS_FILE = 'aws_accounts.tsv'

SCICOMP_ACCOUNT =       '055273631518'
SANDBOX_ACCOUNT =       '563295687221'
SCIPOOL_PROD_ACCOUNT =  '237179673806'

# TODO capitalize
account_id_column = 'line_item_usage_account_id'
value_column = 'line_item_unblended_cost'
pretty_value_column = 'Cost'
PROJECT_COLUMN = 'Project'

# any items having one of these values for 'resource_tags_user_project' is an indirect cost
SCICOMP_INDIRECT_TAGS = ['Infrastructure', 'Infra', 'Infrastucture', 'General']


def main():
	accounts_columns = {
		'account_id':'str',
		'account_name':'str',
		'is_direct':'boolean',
		'account_program_code':'str'
	}
	accounts = pd.read_csv(ACCOUNTS_FILE, sep='\t', usecols=accounts_columns.keys(), dtype=accounts_columns)	
	
	indirect_accounts = accounts.loc[accounts['is_direct'] == False]
	
	
	columns=[
  		value_column,
  		account_id_column
	]
	df = pd.read_parquet(CUR_FILE, columns=columns)
	
	# 1. compute total cost
	total_cost = df[value_column].sum()
	print(f"Total cost is ${total_cost:.2f}.\n")


	# 2. compute total cost for indirect accounts
	indirect_account_ids = indirect_accounts['account_id'].tolist()
	indirect_costs = df.loc[df[account_id_column].apply(lambda x: x in indirect_account_ids)]
	
	indirect_account_total_cost = indirect_costs[value_column].sum()
	print(f"Total costs for indirect accounts is ${indirect_account_total_cost:.2f}.\n")


	# 3. compute costs for direct accounts that have a program code for the account
	direct_with_codes_indexes = (accounts['is_direct']==True) & accounts['account_program_code'].notna()
	direct_accounts_with_codes = accounts.loc[direct_with_codes_indexes]
	
	# we now want to inner join df with direct_accounts_with_codes on account_id_column <-> 'account_id'
	direct_accounts_with_codes = direct_accounts_with_codes.rename(columns={'account_id': account_id_column})
	joined  = df.set_index(account_id_column).join(direct_accounts_with_codes.set_index(account_id_column), on=account_id_column, how="inner")
	gb = joined.groupby('account_program_code', dropna=False, group_keys=False)
	direct_account_costs_by_code = gb.sum().sort_values(value_column)
	sorted = direct_account_costs_by_code.sort_values(value_column, ascending=False)[[value_column]]
	print("Costs for direct AWS accounts, by program code:\n")
	with pd.option_context( 'display.precision', 2):
		display(sorted)
	print(f"Total: ${sorted[value_column].sum():.2f}")
	
	# 4. compute costs for SciComp and SciComp Sandbox
	account_ids = [SCICOMP_ACCOUNT, SANDBOX_ACCOUNT]
	metadata_columns = {
		'resource_tags_user_department':'Dept',
		'resource_tags_user_name':'User',
		'resource_tags_user_owner_email':'Email',
		'resource_tags_user_project':PROJECT_COLUMN
	}
	result = summarize_accounts_by_project(account_ids, metadata_columns)
	
	print('SciComp and SciComp Sandbox costs:')
	with pd.option_context( 'display.precision', 2),\
		pd.option_context('display.float_format', (lambda x : f'${x:.2f}')):
		display(result)

	# TODO:  Subtract out the NaNs and indirects, both of which will be added to indirect
	
	
	# 5. compute costs for Service Catalog
	account_ids = [SCIPOOL_PROD_ACCOUNT]
	metadata_columns = {
		'resource_tags_user_department':'Dept',
		'resource_tags_user_name':'User',
		'resource_tags_user_synapse_user_name':'SynapseUser',
		'resource_tags_user_project':PROJECT_COLUMN
	}
	
	result = summarize_accounts_by_project(account_ids, metadata_columns)
	
	print('Service Catalog costs:')
	with pd.option_context( 'display.precision', 2),\
		pd.option_context('display.float_format', (lambda x : f'${x:.2f}')):
		display(result)


	# TODO:  Subtract out the NaNs and indirects, both of which will be added to indirect
	
	# TODO:  Make sure amounts add up to monthly bill
	
	
	# Outputs:
	# 1. Report for Finance on billing amount by program code, remainder being INDIRECT
	# 2. Sanity check
	


def summarize_accounts_by_project(account_ids, metadata_columns):
	columns=[
  		value_column,
  		account_id_column
	]

	columns.extend(metadata_columns.keys())

	df = pd.read_parquet(CUR_FILE, columns=columns)
	df = df.rename(columns = metadata_columns)
	df = df.rename(columns = {value_column:pretty_value_column})

	account_rows = df.loc[df[account_id_column].apply(lambda x: x in account_ids)]
	
	by = (lambda row_index: scicomp_group_by(df,row_index))
	gb = account_rows.groupby(by, dropna=False, group_keys=False)
	costs_by_category = gb.sum().sort_values(pretty_value_column)
	sorted = costs_by_category.sort_values(pretty_value_column, ascending=False)
	return(sorted)

def scicomp_group_by(df, row_index): # TODO rename
	if df.at[row_index, PROJECT_COLUMN] in SCICOMP_INDIRECT_TAGS:
		return('INDIRECT')
	return df.at[row_index, PROJECT_COLUMN]

	
if __name__ == "__main__":
    main()
