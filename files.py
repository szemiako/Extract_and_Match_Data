from setup import BASE_DIR
from setup import ENGINE
from setup import DataFrame
from setup import datetime
from setup import pd
from setup import re

"""Used for loading stop words and input files, and for writing to export files."""

def load_stop_words() -> list:
    """Load the stop words compiled manually from previous reports. This file should be maintained over time and is expected to grow."""
    with open(f'{ENGINE}/stop_words.txt', 'r') as i:
        stop_words = i.read().splitlines()
        stop_words = list(map(lambda x: x.upper(), stop_words)) # Force all stop words to UPPER case.
    return stop_words

def make_stop_words_pattern(stop_words: list) -> re.Pattern:
    """Create a long Regex pattern from the stop words, used for quick conditional checking."""
    escaped = [re.escape(i) for i in stop_words] # Escape special characters in Stop Words (e.g., "." becomes "\.").
    stop_words_string = '|'.join(escaped) # Use the join method to create one string (separated by "or" operator, "|").
    pattern = re.compile(stop_words_string)
    return pattern

def to_upper(df: DataFrame) -> DataFrame:
    """Force all columns to upper case."""
    return df.apply(lambda x: x.str.upper() if x.dtype == 'object' else x)

def strip_columns(df: DataFrame) -> DataFrame:
    """Strip DataFrame columns of trailing whitespace."""
    return df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)

def prepare_input_df(df: DataFrame) -> DataFrame:
    """Apply generic preparation to each DataFrame."""
    df = df.fillna('') # Fill np.nan values with blanks ("").
    df = to_upper(df) # Force case to UPPER for all columns.
    df = strip_columns(df) # Remove trailing whitespace.
    return df

def get_flat_file_data(kind: str, server: str='PROD', ID: str='42') -> DataFrame:
    """Load source files for customers data and vendor data, and apply initial preparations to the file. Filename format is not expected to change, except for server and ID parameters."""
    k = {
        'c': 'customer_data_{0}_{1}_.csv',
        'b': 'vendor_data_{0}_{1}_.csv'
    }
    f = k[kind].format(server, ID)
    df = pd.read_csv(f'{BASE_DIR}/{f}', encoding='UTF-8')
    df = prepare_input_df(df)
    return df

def get_export_columns(kind: str) -> dict:
    """Build a mapping of the working DataFrame column names, and the "cleaned" version of the same."""
    c = {
        'u': {
            'vendor_name': 'Vendor Name',
            'number': 'Number',
            'name': 'Name',
            'assoc': 'Assocciated'
        },
        'm': {
            'email_address': 'Email Address',
            'first_name': 'First Name',
            'last_name': 'Last Name'
        }
    }
    columns = c['u']                        # Because the matched DataFrame has all the same columns
    if kind == 'm': columns.update(c['m'])  # as unmatched DataFrame, we use the dict.update() method
    return columns                          # to extend the columns of the unmatched DataFrame.

def prepare_output_df(df: DataFrame, kind: str) -> DataFrame:
    """Apply some minor transformations to the export """
    columns = get_export_columns(kind)
    to_drop = list(filter(lambda x: x not in columns.keys(), df.columns.to_list())) # For any columns not in the get_export_columns()
    df = df.drop(columns=to_drop)                                                   # mapping, drop them from the DataFrame.
    df = df.rename(columns=columns)
    return df

def create_report(m_df: DataFrame, u_df: DataFrame, server: str='JEFF', ID: str='11', date=datetime.now().strftime('%Y%m%d %H%M%S')):
    """Make the actual reports.
    m_df := Matched DataFrame.
    u_df := Unmatched DataFrame."""
    m_df = prepare_output_df(m_df, 'm')
    u_df = prepare_output_df(u_df, 'u')

    with pd.ExcelWriter(f'{BASE_DIR}/report_{server}_{ID}_{date}.xlsx') as o:
        m_df.to_excel(o, sheet_name='Matched', index=False)
        u_df.to_excel(o, sheet_name='Unmatched', index=False)

STOP_WORDS = load_stop_words() # Load the stop words into a CONSTANT list. This will be used later.
STOP_WORDS_PATTERN = make_stop_words_pattern(STOP_WORDS) # Turn the stop words into a CONSTANT pattern. This will be used later.