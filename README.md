
Task One

Implement the "BalcklistFilter.apply" method. It takes two RDDs on Strings, each containing Customer data as CSV with
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
    
        