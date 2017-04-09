Q1. For all the businesses that are located in “Palo Alto”, output their full address and also how many businesses are in each address. 
You can use the full_address column as the filter column. 

Q2. Modify Q1 to output business id and full_address of Restaurants that are located in the state of NY.

Q3. You would like to find the top 10 zip codes where the most businesses are located.

To accomplish this, you will emit the following (K,V) pair from mapper (ZipCode, 1).

Then in the reducer, you will sort by the value and emit the top 10 elements.

Q4. Find the top ten rated businesses using the average ratings. Recall that star column in review.csv file represents the rating.

Please answer the question by calculating the average ratings given to each business using the review.csv file. 

You can reuse part of the logic for sorting by values from Q3.

Sample Output:

eebUeWSJDlmtz80tT2kDuA 5.0

H7VLT9-UbaDVKbxfLAMqwg 5.0

dLJgjRFphvHoQQsC9tEyTQ 5.0 5.0

Q5. Modify Q4 to find out the 10 businesses that have received the lowest average ratings.

* Hint: You just have to output hashmap in reverse order and stop at counter value of 10 *
