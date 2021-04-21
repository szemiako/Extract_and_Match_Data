from setup import Callable
from setup import DataFrame
from setup import np
from setup import pd
from setup import re
from files import get_flat_file_data
from files import get_export_columns
from files import BASE_DIR
from files import STOP_WORDS
from files import STOP_WORDS_PATTERN

"""Helper functions to be used for data normalization or string matching for matching accounts to possible users."""

def filter_columns(o_cols: list, to_filter: list) -> list:
    """Filter out undesired columns from a list of columns within a DataFrame."""
    return list(filter(lambda x: x not in o_cols, to_filter))

def apply_parsing_method(df: DataFrame, cols: list, overwrite: bool, func: Callable, **kwargs) -> DataFrame: # **kwargs == keyword arguments
    """Apply a specific method (from below) to a column. Use overwrite to create a new column prefixed with the name of the function to be applied, or keep the column in place."""
    for i in cols:
        if overwrite:
            df[i] = df[i].apply(lambda x: func(x, **kwargs))
        else:
            df[f'{func.__name__}_{i}'] = df[i].apply(lambda x: func(x, **kwargs))
    return df

def coalesce(df: DataFrame, cols: list, name: str='coalesce') -> DataFrame:
    """Coalesce values from a given list of columns. Has to be written to a new column (cannot be done inplace)."""
    df[name] = np.nan # Initialize named column as NaN (i.e., NULL) values.
    for i in cols:
        df[name] = df[name].combine_first(df[i])
    return df

def get_not_disclosed(c_df: DataFrame, v_df: DataFrame) -> DataFrame:
    """j_df is a joined data frame (from join_accounts). "nd" are "not disclosed" accounts."""
    def _join_accounts(c_df: DataFrame, v_df: DataFrame) -> DataFrame: # Helper function for joining the two (2) specific DataFrames together.
        """c_df is customer data from site; v_df is data from vendor_data."""
        return c_df.merge(
            v_df,
            how='outer',
            left_on=['number', 'vendor_name'],
            right_on=['number', 'vendor_name']
        )
    j_df = _join_accounts(c_df, v_df)
    nd_df = j_df[j_df['user_name'].isnull()] # We can also use cases where account_id is NaN (i.e., NULL).
    nd_df = nd_df.drop(columns=filter_columns(['number', 'vendor_name'], c_df.columns.to_list()))
    return nd_df
       
def remove_stop_words(name: str, threshold: int=10) -> str:
    """Remove the stop words from the account name."""
    # N.b.: Wanted to use STOP_WORDS_PATTERN.sub (to save a for loop) but boundries
    # of specific STOP_WORDS within the name field are important (i.e., like searching
    # for a string where "Find Whole Words Only" is used); do not want stemmed/lemmed
    # words.
    if name is np.nan: return np.nan
    _in = name
    
    if STOP_WORDS_PATTERN.search(name) is not None: # Check to see if any stop word exists within the given name.
        for i in STOP_WORDS: # If so, iterate through all stop words and remove entries.
            name = re.sub(r'\b{0}\b'.format(i), '', name) # Cannot use list comprehension because we want to iteratively remove stop words from name (i.e., modify name).
    name = re.sub(r'[^A-Z\s]+', '', name) # Remove any non-letters and non-whitespace from the final name. Helps with addresses.
    name = ' '.join(name.split()) # Remove any extra whitespace characters inside the name, and at the end of the name.

    if len(_in) > 0:
        if ((len(name) / len(_in)) * 100) >= threshold:
            return name
    
    return np.nan

def split_name(name: str) -> tuple:
    """Take any name and return only the "first" and "last" name."""
    if name is np.nan: return np.nan
    fields = name.split(' ')
    
    if fields[0] == fields[-1]:
        return np.nan
    else:
        return fields[0], fields[-1] # "Flatten" the name into only two (2) fields. Where Len(name) == 1, name[0] == name[-1].

def cross_validate(left_df: DataFrame, right_df: DataFrame, left_cols: list, right_cols: list, output_cols: list=get_export_columns('m').keys()) -> DataFrame:
    """Perform iterative joins on the left_df for each left_col against right_df for each right_col."""
    matched_df = pd.DataFrame()

    for i in left_cols:
        for j in right_cols:
            
            _left = left_df.loc[ left_df[i].notna() ] # If you do not filter out NaN (i.e., NULL) values, you will join NaN to NaN values.
            _right = right_df.loc[ right_df[j].notna() ]

            matches = _left.merge(_right, how='inner', left_on=[i], right_on=[j], suffixes=['', '_r'])
            
            if matched_df.empty:
                matched_df = matched_df.append(matches)
            else:
                pre_load = matched_df.merge(matches, how='right', left_on=['number', 'vendor_name'], right_on=['number', 'vendor_name'], suffixes=['_r', ''], indicator=True)
                pre_load = pre_load[ pre_load['_merge'] == 'right_only' ] # Add only new records to the matched DataFrame.
                pre_load = pre_load.drop(columns=filter_columns(output_cols, pre_load.columns.to_list()))
                matched_df = matched_df.append(pre_load)
                pre_load = pre_load.iloc[0:0]
            
            matched_df = matched_df.drop(columns=filter_columns(output_cols, matched_df.columns.to_list()))         
            matches = matches.iloc[0:0]

    matched_df = matched_df.drop_duplicates()
    return matched_df