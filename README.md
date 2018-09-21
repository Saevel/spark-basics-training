
Task One

Implement the "BalacklistFilter.apply" method. It takes two RDDs on Strings, each containing Customer data as CSV with
the following sequence of fields, separated by ", ":
    * id
    * name
    * surname
    * age
    * accountBalance
    
The returned RDD[Customer] should contain all Customers, who: 
    * are in the allCustomers RDD
    * are younger than 30 years of age
    * have an account balance of more than 15 000
    * are not in the "blacklist" RDD
    

Task Two        

Implement the TransactionProcessor.apply method. It should take the configuration (TransactionProcessorConfiguration) with:
    * accountsFile = the path to the file where Accounts are stored in the CSV format
    * customersFile = the path to the file where Customers are stored in the CSV format
    * transactionsFile = the path to the file where Transactions are stored in the CSV format
    * suspiciousCustomersFile = the output path for Customers that the program will find to be "suspicious"
    * customersWithDebitFile = the output path for Customers that the program will find to be "with debit"
    
First, read Customers, Accounts and Transactions from their respective files and deserialize from CSV (separated by ",")
to case classes.

Then, join Transactions with Accounts on account id and join them all together with Customers on customer id.

Check Accounts and find out, which Customers have Accounts, whose total values of Transactions does not correspond
to the balance of those accounts. Those customers will be considered "suspicious".

Check Accounts and find out, which Customers, out of the "non-suspicious" ones, have negative sum of account balances. 
Those customers will be considered "with debit".

Save all the "suspicious" customer ids, line-by-line to a file given by "suspiciousCustomersFile" as a text file. Note
that all ids should be strictly in one file.

Save all the "with debit" customer ids, line-by-line to a file given by "customersWithDebitFile" as a text file.Note
that all ids should be strictly in one file.