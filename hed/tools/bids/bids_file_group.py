import os
from hed.schema.hed_schema_io import load_schema_version
from hed.tools.bids.bids_timeseries_file import BidsTimeseriesFile
from hed.tools.bids.bids_tabular_file import BidsTabularFile
from hed.tools.bids.bids_sidecar_file import BidsSidecarFile
from hed.tools.bids.bids_tabular_summary import BidsTabularSummary
from hed.models.tabular_input import TabularInput
from hed.util.io_util import get_dir_dictionary, get_file_list, get_path_components


class BidsFileGroup:
    """ Container for a group of files with a specified suffix and their supporting sidecars in a BIDS dataset."""

    def __init__(self, root_path, suffix="_events", type="tabular"):
        self.root_path = os.path.realpath(root_path)
        self.suffix = suffix
        self.type = type
        self.sidecar_dict = self._make_sidecar_dict()
        self.datafile_dict = self._make_datafile_dict()
        self.sidecar_dir_dict = self._make_sidecar_dir_dict()

        for bids_obj in self.sidecar_dict.values():
            x = self.get_sidecars_from_path(bids_obj)
            bids_obj.set_contents(content_info=x)

        for bids_obj in self.datafile_dict.values():
            sidecar_list = self.get_sidecars_from_path(bids_obj)
            if sidecar_list:
                bids_obj.sidecar = sidecar_list[-1]

    def get_sidecars_from_path(self, obj):
        """ Creates a list of the applicable sidecars for the indicated object.

        Args:
            obj (BidsTabularFile or BidsSidecarFile):  The BIDS event file to get the sidecars for

        Returns:
            list:  A list of the applicable sidecars for obj starting at the root.

        """
        sidecar_list = []
        current_path = ''
        for comp in get_path_components(obj.file_path, self.root_path):
            current_path = os.path.realpath(os.path.join(current_path, comp))
            next_sidecar = self._get_sidecar_for_obj(obj, current_path)
            if next_sidecar:
                sidecar_list.append(next_sidecar)
        return sidecar_list

    def _get_sidecar_for_obj(self, obj, current_path):
        """ Return a single BidsSidecarFile relevant to obj from the sidecars in the current path """
        sidecars = self.sidecar_dir_dict.get(current_path, None)
        if not sidecars:
            return None
        for sidecar in sidecars:
            if sidecar.is_sidecar_for(obj):
                return sidecar
        return None

    def summarize(self, value_cols=None, skip_cols=None):
        """

        Args:
            value_cols (list):  Column names designated as value columns.
            skip_cols (list):   Column names designated as columns to skip.

        Returns:
            BidsTabularSummary:  A summary of the number of values in different columns.

        Notes: The columns that are not value_cols or skip_col are summarized by counting
        the number of times each unique value appears in that column.

        """
        info = BidsTabularSummary(value_cols=value_cols, skip_cols=skip_cols)
        for event_obj in self.datafile_dict.values():
            info.update(event_obj.file_path)
        return info

    def validate_sidecars(self, hed_ops, check_for_warnings=True):
        """ Validate merged sidecars.

        Args:
            hed_ops ([func or HedOps], func, HedOps):  Validation functions to apply.
            check_for_warnings (bool):  If True, include warnings in the check.

        Returns:
            (list):    A list of validation issues found. Each issue is a dictionary.

        """
        issues = []
        for sidecar in self.sidecar_dict.values():
            issues += sidecar.contents.validate_entries(hed_ops=hed_ops, check_for_warnings=check_for_warnings)
        return issues

    def validate_datafiles(self, hed_ops, check_for_warnings=True, keep_contents=False):
        """ Validate the datafiles and return an error list.

        Args:
            hed_ops ([func or HedOps], func, HedOps):  Validation functions to apply.
            check_for_warnings (bool):  If True, include warnings in the check.
            keep_contents (bool):       If True, the underlying data files are read and their contents retained.

        Returns:
            (list):    A list of validation issues found. Each issue is a dictionary.

        """
        issues = []
        for data_obj in self.datafile_dict.values():
            data_obj.set_contents(no_overwrite=True)
            data = data_obj.contents
            issues += data.validate_file(hed_ops=hed_ops, check_for_warnings=check_for_warnings)
            if not keep_contents:
                data_obj.clear_contents()
        return issues

    def _make_datafile_dict(self):
        """ Get a dictionary of BidsTabularFile objects with underlying EventInput objects not set.

        Returns:
            dict:   A dictionary of BidsTabularFile objects keyed by real path.

        """
        files = get_file_list(self.root_path, name_suffix=self.suffix, extensions=['.tsv'])
        file_dict = {}
        if self.type == "tabular":
            for file in files:
                file_dict[os.path.realpath(file)] = BidsTabularFile(file)
        else:
            for file in files:
                file_dict[os.path.realpath(file)] = BidsTimeseriesFile(file)
        return file_dict

    def _make_sidecar_dict(self):
        """ Create a dictionary of BidsSidecarFile objects for the specified entity type.

        Returns:
            dict:   a dictionary of BidsSidecarFile objects keyed by real path for the specified suffix type

        Notes:
            This function creates the sidecars and but does not set their contents.

        """
        files = get_file_list(self.root_path, name_suffix=self.suffix, extensions=['.json'])
        file_dict = {}
        for file in files:
            file_dict[os.path.realpath(file)] = BidsSidecarFile(os.path.realpath(file))
        return file_dict

    def _make_sidecar_dir_dict(self):
        """ Create a the dictionary with real paths of directories as keys and a list of sidecar file paths as values.

        Returns:
            dict: A dictionary of lists of sidecar BidsSidecarFiles

        """
        dir_dict = get_dir_dictionary(self.root_path, name_suffix=self.suffix, extensions=['.json'])
        sidecar_dir_dict = {}
        for this_dir, dir_list in dir_dict.items():
            new_dir_list = []
            for s_file in dir_list:
                new_dir_list.append(self.sidecar_dict[os.path.realpath(s_file)])
            sidecar_dir_dict[os.path.realpath(this_dir)] = new_dir_list
        return sidecar_dir_dict


if __name__ == '__main__':
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                        '../../../tests/data/bids/eeg_ds003654s_hed')
    bids = BidsFileGroup(path)

    for file_obj in bids.sidecar_dict.values():
        print(file_obj)

    for file_obj in bids.datafile_dict.values():
        print(file_obj)

    hed_schema = load_schema_version(xml_version="8.0.0")

    col_info = bids.summarize()