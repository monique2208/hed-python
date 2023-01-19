from hed.tools.remodeling.operations.factor_column_op import FactorColumnOp
# from hed.tools.remodeling.operations.factor_hed_tags_op import FactorHedTagsOp
from hed.tools.remodeling.operations.factor_hed_type_op import FactorHedTypeOp
from hed.tools.remodeling.operations.merge_consecutive_op import MergeConsecutiveOp
from hed.tools.remodeling.operations.number_rows_op import NumberRowsOp
from hed.tools.remodeling.operations.group_by_start_stop_op import GroupByStartStopOp
from hed.tools.remodeling.operations.remove_columns_op import RemoveColumnsOp
from hed.tools.remodeling.operations.reorder_columns_op import ReorderColumnsOp
from hed.tools.remodeling.operations.remap_columns_op import RemapColumnsOp
from hed.tools.remodeling.operations.remove_rows_op import RemoveRowsOp
from hed.tools.remodeling.operations.rename_columns_op import RenameColumnsOp
from hed.tools.remodeling.operations.split_event_op import SplitEventOp
from hed.tools.remodeling.operations.summarize_column_names_op import SummarizeColumnNamesOp
from hed.tools.remodeling.operations.summarize_column_values_op import SummarizeColumnValuesOp
from hed.tools.remodeling.operations.summarize_events_to_sidecar_op import SummarizeEventsToSidecarOp
from hed.tools.remodeling.operations.summarize_hed_type_op import SummarizeHedTypeOp
from hed.tools.remodeling.operations.summarize_hed_tags_op import SummarizeHedTagsOp
from hed.tools.remodeling.operations.summarize_hed_validation_op import SummarizeHedValidationOp

valid_operations = {
    'factor_column': FactorColumnOp,
    # 'factor_hed_tags': FactorHedTagsOp,
    'factor_hed_type': FactorHedTypeOp,
    'merge_consecutive': MergeConsecutiveOp,
    'group_start_stop_op': GroupByStartStopOp,
    'number_rows_op': NumberRowsOp,
    'remap_columns': RemapColumnsOp,
    'remove_columns': RemoveColumnsOp,
    'remove_rows': RemoveRowsOp,
    'rename_columns': RenameColumnsOp,
    'reorder_columns': ReorderColumnsOp,
    'split_event': SplitEventOp,
    'summarize_column_names': SummarizeColumnNamesOp,
    'summarize_column_values': SummarizeColumnValuesOp,
    'summarize_events_to_sidecar': SummarizeEventsToSidecarOp,
    'summarize_hed_type': SummarizeHedTypeOp,
    'summarize_hed_tags': SummarizeHedTagsOp,
    'summarize_hed_validation': SummarizeHedValidationOp
}
