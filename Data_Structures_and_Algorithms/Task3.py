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
TASK 3:
(080) is the area code for fixed line telephones in Bangalore.
Fixed line numbers include parentheses, so Bangalore numbers
have the form (080)xxxxxxx.)

Part A: Find all of the area codes and mobile prefixes called by people
in Bangalore. In other words, the calls were initiated by "(080)" area code
to the following area codes and mobile prefixes:
 - Fixed lines start with an area code enclosed in brackets. The area
   codes vary in length but always begin with 0.
 - Mobile numbers have no parentheses, but have a space in the middle
   of the number to help readability. The prefix of a mobile number
   is its first four digits, and they always start with 7, 8 or 9.
 - Telemarketers' numbers have no parentheses or space, but they start
   with the area code 140.

Print the answer as part of a message:
"The numbers called by people in Bangalore have codes:"
 <list of codes>
The list of codes should be print out one per line in lexicographic order with no duplicates.

Part B: What percentage of calls from fixed lines in Bangalore are made
to fixed lines also in Bangalore? In other words, of all the calls made
from a number starting with "(080)", what percentage of these calls
were made to a number also starting with "(080)"?

Print the answer as a part of a message::
"<percentage> percent of calls from fixed lines in Bangalore are calls
to other fixed lines in Bangalore."
The percentage should have 2 decimal digits
"""

def GetCallsFromBangaloreCallers(calls: list) -> list:
    """Get all the calls from Bangalore fixed line callers
    
    COMPLEXITY: This has O(4n) complexity in worst case scenario.
    REASON: This has a loop on calls (n), a substr operation (n), a comparison evaluation (n), 
            and builds a tuple (n).
    """
    return [(call[0], call[1]) for call in calls if call[0][:5] == '(080)']
    
def LogAreaCodesOfReceivers(bangalore_calls: list):
    """Gets a set of area codes from call receivers and collects counts.
    
    The counts are for Part B to calculate the number of from bangalore to bangalore fixes line calls.
    
    COMPLEXITY: This has O(15n + 4) complexity in worst case scenario .
    REASON: This has a loop on calls (n), 7 substr operations (7n), 5 comparison evaluations (5n), 
            2 counters (2n), and initialization of 2 integer variables and a set (3), 
            and set to list (1).
    """
    area_codes = set()
    bangalore_recipients = 0
    total = 0
    
    for _, recipient in bangalore_calls:
        total += 1

        # Fixed lines start with an area code enclosed in parenthesis begin with 0.
        if recipient.find('(0') >= 0:
            area_codes.update([recipient[1:recipient.find(')')]])
            if recipient[:5] == '(080)':
                bangalore_recipients += 1
            
        # Mobile numbers have a space in the middle. The prefix is its first 
        # four digits, and they always start with 7, 8 or 9.
        if recipient.find(' ') > 0 and recipient[:1] in ('7', '8', '9'):
            area_codes.update([recipient[:4]])
            
        # Telemarketers' #s start with the area code 140.
        if recipient[:3] == '140':
            area_codes.update(['140'])

    return list(area_codes), bangalore_recipients, total


def PrintTask3PartAMessageBangaloreFixedLineCalls(area_codes: list):
    """Prints the answer for Task 3 Part A: 
    Find unique area codes called by Bangalore fixed lines in lexicographic order.
    
    COMPLEXITY: This has O(n log n + n + 1) complexity.
    REASON: Theres a sort operation, a print statement, and n area codes to print.
    """
    area_codes.sort()
    print("The numbers called by people in Bangalore have codes:")
    for area_code in area_codes:
          print(area_code)

def PrintTask3PartBMessageBangaloreFixedLineCallsPct(bangalore_recipients: int, total: int):
    """Prints the answer for Task 3 Part B:
    Calculate percentage of calls from fixed lines in Bangalore to fixed lines in Bangalore.

    COMPLEXITY: This has O(5) complexity.
    REASON: There are 3 math operations (3) and a formatted print statement (2).
    """
    pct = round(100 * bangalore_recipients / total, 2)
    print(f"{pct}% percent of calls from fixed lines in Bangalore are calls to other fixed " +
          "lines in Bangalore.")

if __name__ == "__main__":
    bangalore_calls = GetCallsFromBangaloreCallers(calls)
    area_codes, bangalore_recipients, total = LogAreaCodesOfReceivers(bangalore_calls)
    PrintTask3PartAMessageBangaloreFixedLineCalls(area_codes)
    PrintTask3PartBMessageBangaloreFixedLineCallsPct(bangalore_recipients, total)