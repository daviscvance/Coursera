"""
Read file into texts and calls.
It's ok if you don't understand how to read files
"""
# import csv
# with open('texts.csv', 'r') as f:
#     reader = csv.reader(f)
#     texts = list(reader)

# with open('calls.csv', 'r') as f:
#     reader = csv.reader(f)
#     calls = list(reader)

import pandas as pd
from datetime import datetime

texts_df = pd.read_csv('texts.csv', names = ['sender', 'receiver', 'ts'])
calls_df = pd.read_csv('calls.csv', names = ['caller', 'receiver', 'ts', 'duration_s'])

"""
TASK 2: Which telephone number spent the longest time on the phone
during the period? Don't forget that time spent answering a call is
also time spent on the phone.
Print a message:
"<telephone number> spent the longest time, <total time> seconds, on the phone during 
September 2016.".
"""

##
# Total complexity of this script is O(n + n log n + 6).
# It includes a sort, an apply function, and 6 regular operations.
##

def FormatTimestamp(ts_str):
    """Reformats dates into a datetime for sorting.
    
    COMPLEXITY: This has O(1) complexity.
    REASON: The datetime reformats a single string into a datetime.
    """
    return datetime.strptime(ts_str, '%d-%m-%Y %H:%M:%S')

def CleanData(calls:pd.DataFrame):
    """Cleans data from the calls dataframes and filters to September 2016.
    
    COMPLEXITY: This has O(n + 3) complexity.
    REASON: There are k=1 arguments (dataframe being passed).
            The apply is O(n) which extends FormatTimestamp's O(1)
            Theres 2 operations to filter on the year-month.
            Return is 1 operation.
    """
    calls.ts = calls.ts.apply(FormatTimestamp)
    return calls[calls.ts.dt.strftime('%Y-%m') == '2016-09']

def FindLongestCall(calls: pd.DataFrame):
    """Finds the longest duration spent on a call.
    
    COMPLEXITY: This has O(n log n + 2) complexity.
    REASON: There is 1 sort, a row pick, and a return operation
    """
    return calls.sort_values(by=['duration_s'], ascending=False).iloc[0]

def PrintTask2MessageLongestCall():
    """Prints the answer for Task 2: find the longest call in September 2016.
    
    COMPLEXITY: This has O(1) complexity + FindLongestCall complexity.
    REASON: The print statement is 1 line to run.
    """
    answer = FindLongestCall(calls_df)
    print(f"'{answer.caller}' spent the longest time, {answer.duration_s} seconds, " +
          "on the phone during September 2016.")

if __name__ == "__main__":
    calls = CleanData(calls_df)
    PrintTask2MessageLongestCall()