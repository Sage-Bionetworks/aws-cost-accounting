# aws-cost-accounting
Read a AWS cost and usage report and create an accounting of costs by Sage program codes


Each AWS expense must either be marked as an indirect expense or assigned to a program code.
The file `aws_accounts.tsv` says whether each AWS account is direct or indirect.  If the account is
indirect, then all expenses in the account are considered indirect. (Note:  In the future we will
allocate S3 storage costs directly, but for now they are considered indirect.) If the account is direct
and if the 	`aws_acccounts.tsv` file gives a program code for the account, then all expenses in the
account will be assigned to that program code.  If there is no program code then the 'direct AWS account' 
will be further analyzed.  There are three accounts that require further analysis:

SciComp

SciComp Sandbox

Service Catalog

Each account has at least some items which are indirect, i.e., baseline infrastructure for security
and account management.