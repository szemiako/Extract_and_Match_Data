from files import create_report
from files import get_flat_file_data
from match import apply_parsing_method
from match import cross_validate
from match import get_not_disclosed
from match import remove_stop_words
from match import split_name
from setup import make_arg_parser

def main(parameters):
    """Functional, script-based approach to create the orphan accounts report, because we heavily leverage the use of DataFrames to produce most of the report so as to create the report "in bulk" as opposed to row-by-row."""
    c_df = get_flat_file_data('c', server=parameters.server, ID=parameters.company) # Get the data in customer_data_{SERVER}_{ID}.csv
    v_df = get_flat_file_data('b', server=parameters.server, ID=parameters.company) # Get the data in vendor_data_{SERVER}_{ID}.csv
    c_cols = ['full_name', 'name', 'assoc'] # Columns that hold data used for matching from customer_data file.
    v_cols = ['assoc_1', 'assoc_2', 'assoc_other'] # Columns that hold data used for matching from vendor_data file.

    c_df['full_name'] = c_df['first_name'] + ' ' + c_df['last_name'] # Make "Full name" column for customer_data data.
    c_df = apply_parsing_method(c_df, ['name', 'assoc'], True, remove_stop_words) # Stop words not expected in firstname and lastname fields, so do not need to be applied there.
    c_df = apply_parsing_method(c_df, c_cols, True, split_name) # Needed for cross-validation. Hampers instances where an account name / account holder field has an address embedded within it.
    
    nd_df = get_not_disclosed(c_df, v_df)
    nd_df = apply_parsing_method(nd_df, v_cols, False, remove_stop_words) # Not disclosed DataFrame (nd_df) has only columns from vendor_data file.
    rsw_v_cols = list(map(lambda x: f'remove_stop_words_{x}', v_cols)) # Because we created a new column in preceding step, we need to modify v_cols to prefix with the previous function name.
    nd_df = apply_parsing_method(nd_df, rsw_v_cols, True, split_name)

    matched_df = cross_validate(nd_df, c_df, rsw_v_cols, c_cols)
    unmatched_df = nd_df.merge(matched_df, how='left', left_on=['number', 'vendor_name'], right_on=['number', 'vendor_name'], suffixes=['', '_r'], indicator=True)
    unmatched_df = unmatched_df[ unmatched_df['_merge'] == 'left_only' ]

    create_report(matched_df, unmatched_df) # Create the orphan accounts repot.
main(make_arg_parser())