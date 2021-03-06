from __future__ import annotations  # allow typehinting of Preprocess

import dask.dataframe as dd
import json
from datetime import datetime
import os

import warnings
from typing import List, Union, Dict, Optional


class Preprocess:
    """Preprocess brazilian funds' data.

    Atrributes:
        type: if data is raw ('new') or already preprocessed ('existing')
        inpath: path with data preprocessed/to be preprocessed
        data: dask DataFrame with data being preprocessed
        files_read: list of inpath files
    """

    def __init__(
        self, inpath: Optional[Union[str, List[str]]] = None, data: Optional[dask.DataFrame] = None, 
        type: str = "new", info_status: Optional[Dict] = None
    ) -> None:
        """Initialize class for preprocessing.

        Specify data or inpath data to be preprocessed.
        
        Args:
            inpath: path with raw/preprocessed file(s) to read.
                It can be a directory (single string) or a list
                of files.
            data: raw or preprocessed dask dataframe. If both
                data and inpath are specified then inpath is
                ignored.
            type: if data is raw ('new') or already preprocessed 
                ('existing')
        """

        # CHECK IF TYPE IS OK
        accepted_types = ["existing", "new"]
        if type in accepted_types:
            self.type = type
        else:
            raise Exception(f"'type' must be in {accepted_types}")

        # CHECK IF INPATH AND DATA WERE BOTH NON-NULL OR BOTH NULL
        if inpath and (data is not None):
           self.inpath = None
           warnings.warn("Both inpath and data were specified. Descarting inpath and using only data.") 
        elif (not inpath) and (data is None):
            raise Exception("Neither inpath nor data was specified.")
       
        # CASE 1: USING DATA
        if data is not None:
            self.data = data
            self.files_read = None
        # CASE 2: USING INPATH
        elif inpath:
            self.inpath = inpath

            if self.type == "new":
                # READ FILES
                if (not isinstance(self.inpath, list)) and os.path.isfile(self.inpath):
                    self.inpath = [self.inpath]

                # CASE 1: IT'S A LIST (ASSUMED LIST OF FILES)
                if isinstance(self.inpath, list):

                    if len(list(filter(os.path.isfile, self.inpath))) != len(self.inpath):
                        raise Exception("Can't pass a list of directories. Pass a single string instead.")
                    else:    
                        filenames_inpath = [os.path.basename(fn)[:-4] for fn in self.inpath]

                # CASE 2: IT'S A SINGLE STRING (ASSUMING DIRECTORY PATH)
                else:
                    filenames_inpath = list(filter(
                        lambda x: os.path.isfile(os.path.join(self.inpath, x)) and x[-4:] == ".csv",
                        os.listdir(self.inpath)
                    ))
                    filenames_inpath = [fn[:-4] for fn in filenames_inpath]
                    self.inpath = os.path.join(self.inpath, "*.csv")

                columns_to_read = ['CNPJ_FUNDO', 'DT_COMPTC', 'VL_TOTAL', 'VL_QUOTA', 'VL_PATRIM_LIQ', 'CAPTC_DIA', 'RESG_DIA', 'NR_COTST']

                self.data = dd.read_csv(self.inpath, sep=";", usecols=columns_to_read, dtype={"CNPJ_FUNDO" : str})
                self.files_read = filenames_inpath
            else:
                if not os.path.isfile(self.inpath):
                    raise Exception(f"If type is 'existing', {self.inpath} should be a file.")
                else:
                    self.data = dd.read_csv(self.inpath, sep=",", assume_missing=True, dtype={"CNPJ_FUNDO" : str})
                    self.files_read = self.inpath

    def apply_merge(self, CNPJ_to_keep_path: str, sep: str = ",") -> Preprocess:
        """Filter by specific CNPJs by merging with a CNPJ given base.
        
        Base to merge by is assumed to have a 'CNPJ_FUNDO' column with
        desired CNPJs in string format (but just with numbers) and can
        have additional columns which will also be included and must
        be written in csv format in default specifications.

        Args:
            CNPJ_to_keep_path: path for data to be merged
            sep: csv separator of the base
        """    

        CNPJ_data_to_keep = (
            dd.read_csv(CNPJ_to_keep_path, sep=sep, dtype={"CNPJ_FUNDO" : str})
            .repartition(npartitions=1)
        )

        self.data = self.data.merge(CNPJ_data_to_keep, on="CNPJ_FUNDO", how="right")

        return Preprocess(data=self.data, type=self.type)

    def apply_filters(self, inrange_filters: List[Dict[str, List[float]]] = []) -> Preprocess:
        """Apply filters to data.

        Filters are stored in a dictionary, where key is the
        column to apply the filter and value is a two-length list, 
        indicating minimum and maximum values that column can have.
        A list of filters is asked, and filters will be applied in
        the order they are specified.

        Args:
            inrange_filters: list of filters.
        """
        CNPJs_to_keep = set()

        for dict_filter in inrange_filters:
            CNPJs_filtered_data = self.data.copy()

            for column_filter in dict_filter.keys():
                CNPJs_filtered_now = (
                    CNPJs_filtered_data[
                        CNPJs_filtered_data[column_filter]
                        .between(dict_filter[column_filter][0], dict_filter[column_filter][1], inclusive="both")
                    ]
                )
            CNPJs_filtered_now = (
                CNPJs_filtered_now["CNPJ_FUNDO"]
                    .unique().values
                    .compute()
            )
            if CNPJs_to_keep:
                CNPJs_to_keep = CNPJs_to_keep.intersection(set(CNPJs_filtered_now))
            else:
                CNPJs_to_keep = set(CNPJs_filtered_now)

        CNPJs_to_keep = list(CNPJs_to_keep)
        self.data = self.data[self.data["CNPJ_FUNDO"].isin(CNPJs_to_keep)]

        return Preprocess(data=self.data, type=self.type)

    def write(self, outpath: Optional[str] = None, overwrite: bool = True) -> Preprocess:
        """Write computations in disk.

        Output format is csv. An additional json file with
        information of actions performed is also written in
        the output file's directory.
        
        Args:
            outpath: file path to the output data
            overwrite: whether to overwrite existing preprocessed file
                in outpath
        """
        if os.path.exists(outpath):
            if os.path.isdir(outpath):
                raise Exception("Path exists and is a directory.")
            elif not overwrite:
                raise Exception("Path already exists. Set 'overwrite' to true if you wish to proceed.")

        # WRITING FILE  
        self.data.to_csv(outpath, single_file=True, index=False)

        return Preprocess(data=self.data, type=self.type)

    def format_new_data(
        self,
        outpath: Optional[str] = None, overwrite: bool = True,
        CNPJ_only_numbers: bool = True,
        CNPJ_to_keep_path: Optional[str] = None, sep: str = ",",
        inrange_filters: List[Dict[str, List[float]]] = []
    ) -> Preprocess:
        """Format raw data.

        A general routine to apply standard transformations
        to raw data (including writing in disk) without the
        need to evoke each of the routines separetely.

        Args:
            outpath: path to the output data
            overwrite: whether to overwrite existing preprocessed file in outpath
            CNPJ_only_numbers: remove CNPJ special characters and keep only numbers
            CNPJ_to_keep_path: path to csv data with CNPJs to keep
            sep: csv separator of the base to merge with
            inrange_filters: list of dicts with inrange filters to apply 
        """

        # CHECKING TYPE
        if self.type == "existing":
            raise Exception("Can't preprocess already preprocessated data.")

        # KEEP ONLY NUMBERS IN CNPJ
        if CNPJ_only_numbers:
            self.data = self.data.replace({'CNPJ_FUNDO': r'[^\d]'}, {'CNPJ_FUNDO': ''}, regex=True)  

        # CNPJ DATA TO KEEP
        if CNPJ_to_keep_path:
            self.apply_merge(CNPJ_to_keep_path, sep=sep)

        # INRANGE FILTERS:
        if inrange_filters:
            self.apply_filters(inrange_filters)

        # WRITING
        if outpath:
            return self.write(outpath=outpath, overwrite=overwrite)
        else:
            return Preprocess(data=self.data, type=self.type)