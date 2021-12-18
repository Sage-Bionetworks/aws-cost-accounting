import pandas as pd
from IPython.display import display

#CUR_FILE = '2021Sept_CUR.parquet'
CUR_FILE = '2021Oct_CUR.parquet'

ACCOUNT_ID_COLUMN = 'line_item_usage_account_id'
VALUE_COLUMN = 'line_item_unblended_cost'
PROJECT_COLUMN = 'resource_tags_user_project'
DISPLAY_VALUE_COLUMN = 'Cost'
DISPLAY_PROJECT_COLUMN = 'Project'



def main():

	# Read in the month's cost and usage report
	columns=[
  		#'line_item_product_code',
  		'line_item_usage_type',
  		'product_product_name'
  		#'product_product_family',
  		#'line_item_line_item_description'
	]
	df = pd.read_parquet(CUR_FILE, columns=columns)
	
	for col in columns:
		print(f"\n{col}:")
		print(set(df[col].tolist()))

	



	
if __name__ == "__main__":
    main()
