import pandas as pd
from IPython.display import display
import math


SCICOMP_ACCOUNT =       '055273631518'
SANDBOX_ACCOUNT =       '563295687221'
SCIPOOL_PROD_ACCOUNT =  '237179673806'
account_ids = [SCICOMP_ACCOUNT, SANDBOX_ACCOUNT, SCIPOOL_PROD_ACCOUNT]
account_id_column = 'line_item_usage_account_id'
value_column = 'line_item_unblended_cost'
                     
metadata_columns = [
 'resource_tags_user_department',
 'resource_tags_user_name',
 'resource_tags_user_owner_email',
 'resource_tags_user_project'
]

columns=[
  account_id_column,
  value_column
]

columns.extend(metadata_columns)

df = pd.read_parquet(file_path, columns=columns)
# Just the accounts for which we use tags to categorize expenses
df = df.loc[df[account_id_column].apply(lambda x: x in account_ids)]

# now get rid of the account id
metadata_and_cost = df.drop(columns=account_id_column)

# now group-by, sum, and sort
costs_by_metadata = metadata_and_cost.groupby('resource_tags_user_project', dropna=False, group_keys=False).sum()
sorted = costs_by_metadata.sort_values(value_column, ascending=False)
#display(sorted.style.format('${0:,.2f}'))
sorted.to_csv("tags_with_costs.csv")

project_and_email = df[['resource_tags_user_project', 'resource_tags_user_owner_email']]
gb = project_and_email.groupby('resource_tags_user_project', dropna=False, group_keys=False)

emails_for_tags = gb.aggregate((lambda x: set(filter(None, x))))
#display(emails_for_tags)



scores = []
max_score = 0
for row_label, row in emails_for_tags.iterrows():
    tag = row_label
    try:
        if math.isnan(tag):
            continue
    except:
        pass
    cost = costs_by_metadata.at[tag, value_column]
    emails = row['resource_tags_user_owner_email']
    for project, worker_last_names in project_to_workers.items():
        score = 0
        matches = []
        for worker_last_name in worker_last_names:
            for email in emails:
                #print(email.lower(), worker_last_name, email.lower().find(worker_last_name))
                if email.lower().find(worker_last_name)>=0:
                    score = score + 1
                    matches.append(worker_last_name)
        if score>0:
            scores.append([tag, cost, project, matches, score])
            if score > max_score:
                max_score=score;
    
tag_to_program_code = pd.DataFrame(scores, columns=["Tag", "Cost", "Project Code", "Workers", "Score"])
tag_to_program_code = tag_to_program_code.sort_values(["Cost","Score"], ascending=False)

#pd.set_option('display.max_rows', 500)
#display(tag_to_program_code)

tag_to_program_code.to_csv("tag_to_program_code.csv")
 
	
if __name__ == "__main__":
    main()
