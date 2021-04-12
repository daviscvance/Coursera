"""
Read file into texts and calls.
It's ok if you don't understand how to read files.
"""
import csv
from collections import Counter

with open('texts.csv', 'r') as f:
    reader = csv.reader(f)
    texts = list(reader)

with open('calls.csv', 'r') as f:
    reader = csv.reader(f)
    calls = list(reader)

"""
TASK 4:
The telephone company want to identify numbers that might be doing
telephone marketing. Create a set of possible telemarketers:
these are numbers that make outgoing calls but never send texts,
receive texts or receive incoming calls.

Print a message:
"These numbers could be telemarketers: "
<list of numbers>
The list of numbers should be print out one per line in lexicographic order with no duplicates.
"""

##
# Total complexity of this script is O(13n + n log n + 6).
# It includes 4 loops, 13 operations per loop in worst case scenario, a sort and 6 normal operations.
##

def IdentifyTelemarketers(calls:list, texts:list) -> list:
    """Finds sets of phone activity for each phone number and identifies unique telemarketers.
    
    COMPLEXITY: This has O(12n + 5) complexity.
    REASON: 
        There are 3 loops.
        The first and second loops have 4 operations each in the absolute worst case scenario:
            1 for checking if its an existing caller.
            1 for appending to the caller list if its not (assuming 1 per user in worst case).
            1 for checking if its an existing call receiver.
            1 for appending to the call receiver list if its not (assuming 1 per user in worst case).
            
            Repeat for texters and text receivers.
            This is O(4n * 2).
        The third loop has O(4n):
            n = number of callers and
            n operations to append an item to a list (assuming worst case scenario but its less than n)
        There are 5 list initializations: O(5)
    """
    callers = []
    call_receivers = []
    texters = []
    text_receivers = []
    
    for call in calls:        
        callers.append(call[0]) if call[0] not in callers else None
        call_receivers.append(call[1]) if call[1] not in call_receivers else None
        
    for text in texts:
        texters.append(text[0]) if text[0] not in texters else None
        text_receivers.append(text[1]) if text[1] not in text_receivers else None
        
    telemarketers = []
    for caller in callers:
        if (
            caller not in call_receivers and
            caller not in texters and 
            caller not in text_receivers):
            telemarketers.append(caller)

    return telemarketers

def PrintTask4MessageTelemarketers(telemarketers: list):
    """Prints the answer for Task 4: find telemarketers in lexicographic order.
    
    COMPLEXITY: This has O(n log n + n + 1) complexity.
    REASON: Theres a sort operation, a print statement, and n telemarketer numbers to print.
    """
    telemarketers.sort()
    print("These numbers could be telemarketers:")
    for number in telemarketers:
          print(number)

if __name__ == "__main__":
    telemarketers = IdentifyTelemarketers(calls, texts)
    PrintTask4MessageTelemarketers(telemarketers)