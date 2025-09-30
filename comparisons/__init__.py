# Import all comparison functions to make them available from the package
from .title_vs_specifics import compare_title_vs_specifics
from .title_vs_table import compare_title_vs_table
from .specifics_vs_table import compare_specifics_vs_table
from .metadata_comparisons import compare_title_vs_metadata, compare_specifics_vs_metadata
from .multi_item_lists import compare_multi_item_lists

__all__ = [
    'compare_title_vs_specifics',
    'compare_title_vs_table', 
    'compare_specifics_vs_table',
    'compare_title_vs_metadata',
    'compare_specifics_vs_metadata',
    'compare_multi_item_lists'
]