"""
Read file into texts and calls.
It's ok if you don't understand how to read files.
"""
import csv
with open('texts.csv', 'r') as f:
    reader = csv.reader(f)
    texts = list(reader)

with open('calls.csv', 'r') as f:
    reader = csv.reader(f)
    calls = list(reader)


"""
TASK 1:
How many different telephone numbers are there in the records? 
Print a message:
"There are <count> different telephone numbers in the records."
"""

##
# Total complexity of this script is O(2n + 3).
# It includes 2 loops, a length call, and a print statement.
##

def CountUniqueTelephoneNumbers(calls:list, texts:list):
    """Counts all the unique telephone numbers in our dataset.
    
    COMPLEXITY: This has O(2n + 2) complexity.
    REASON: There are 2 loops, 1 set initialization, and 1 call for the length.
    """
    uniques = set()
    for i in calls:
        uniques.update([i[0], i[1]])
    for i in texts:
        uniques.update([i[0], i[1]])
    return len(uniques)
    
def PrintTask1MessageTelephoneNumbers():
    """Prints the answer for Task 1: count unique telephone numbers.
    
    COMPLEXITY: This has O(1) complexity + CountUniqueTelephoneNumbers complexity.
    REASON: The print statement is 1 line to run.
    """
    count = CountUniqueTelephoneNumbers(calls, texts)
    print(f"There are {count} different telephone numbers in the records.")
    
if __name__ == "__main__":
    PrintTask1MessageTelephoneNumbers()