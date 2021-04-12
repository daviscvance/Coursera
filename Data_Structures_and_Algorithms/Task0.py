"""
Read file into texts and calls.
It's ok if you don't understand how to read files.
"""
import pandas as pd
from datetime import datetime

texts_df = pd.read_csv('texts.csv', names = ['sender', 'receiver', 'ts'])
calls_df = pd.read_csv('calls.csv', names = ['caller', 'receiver', 'ts', 'duration_s'])

"""
TASK 0:
What is the first record of texts and what is the last record of calls?
Print messages:
"First record of texts, <incoming number> texts <answering number> at time <time>"
"Last record of calls, <incoming number> calls <answering number> at time <time>, lasting <during> seconds"
"""

##
# Total complexity of this script is O(2n + n log n + 3).
# It includes an apply and a sort operation, as well as some basic calls.
##

def FormatTimestamp(ts_str):
    """Reformats dates into a datetime for sorting.
    
    COMPLEXITY: This has O(1) complexity.
    REASON: The datetime reformats a single string into a datetime.
    """
    return datetime.strptime(ts_str, '%d-%m-%Y %H:%M:%S')

def CleanData(texts: pd.DataFrame, calls:pd.DataFrame):
    """Cleans data from the texts and calls dataframes.
    
    COMPLEXITY: This has O(kn) complexity.
    REASON: There are k=2 arguments, 1 for each dataframe being passed.
            The apply is O(n) which extends FormatTimestamp's O(1).
    """
    texts.ts = texts.ts.apply(FormatTimestamp)
    calls.ts = calls.ts.apply(FormatTimestamp)

def GetTopRecord(df: pd.DataFrame, sort_asc: bool=True):
    """Finds the first or last record of a timestamp ordered list
    
    COMPLEXITY: This has O(n log n + 1) complexity.
    REASON: Sorting is worst case O(n log n) in python, for n as the length of the sorting column 'ts'.
            Then we return a single row for O(1) to add.
    """
    df = df.sort_values(by=['ts'], ascending=sort_asc)
    return df.iloc[0]

def PrintTask0MessageTexts(texts_df: pd.DataFrame):
    """Prints the answer for Task 0: First text record.
    
    COMPLEXITY: This has O(1) complexity.
    REASON: The print statement is 1 line to run (ignoring previous complexities from GetTopRecords).
    """
    answer = GetTopRecord(texts_df)
    print(f"First record of texts, '{answer.sender}' " +
          f"texts '{answer.receiver}' at time '{answer.ts}'.")

def PrintTask0MessageCalls(calls_df: pd.DataFrame):
    """Prints the answer for Task 0: First text record.
    
    COMPLEXITY: This has O(1) complexity.
    REASON: The print statement is 1 line to run (ignoring previous complexities from GetTopRecords).
    """
    answer = GetTopRecord(calls_df, sort_asc=False)
    print(f"Last record of calls, '{answer.caller}' " +
          f"calls '{answer.receiver}' at time '{answer.ts}', " +
          f"lasting '{answer.duration_s}' seconds.")
    
if __name__ == "__main__":
    CleanData(texts_df, calls_df)
    PrintTask0MessageTexts(texts_df)
    PrintTask0MessageCalls(calls_df)