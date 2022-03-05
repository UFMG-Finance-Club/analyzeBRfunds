import dask.dataframe as dd
import numpy as np
import json
from datetime import datetime
import os
from typing import List, Union, Callable, Dict
import warnings

class Preprocess:

    def __init__(self, inpath: Union[str, List[str]] = None, data = None, type: str = "new", info_status: Dict = None):
        """Initialize class for preprocessing.
        
        :param inpath: path with raw/preprocessed file(s) to read
        :param data: raw or preprocessed dask dataframe
        :param type: whether data is raw ('new') or preprocessed ('existing')
        :param status: content to write in log file  
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
                
            self.data = dd.read_csv(self.inpath, sep=";")
            self.files_read = filenames_inpath
        
        if not info_status:
            self.info_status = {"type" : self.type, "status" : "unmodified"}

    def update_status(self, kind: str, description: Union[str, Dict[str, List[float]]]) -> None:
        if kind in self.info_status.keys():
            self.info_status[kind].append(description)
        else:
            self.info_status[kind] = description
        
        self.info_status["status"] = "written" if kind == "write" else "modified"

    def apply_merge(self, CNPJ_to_keep_path: str):
        """Filter by specific CNPJs by merging with a CNPJ given base.
        
        :param CNPJ_to_keep_path: path to csv data with CNPJs to keep  
        """    

        CNPJ_data_to_keep = (
            dd.read_csv(CNPJ_to_keep_path)
            .repartition(npartitions=1)
        )

        self.data = self.data.merge(CNPJ_data_to_keep, how="left", on="CNPJ_FUNDO")
        
        self.update_status(kind="merge", description=CNPJ_data_to_keep) 
        return Preprocess(data=self.data, type=self.type, info_status=self.info_status)

    def apply_filters(self, inrange_filters: List[Dict[str, List[float]]] = []):
        """ Apply standard filters to data.

        :param inrange_filters: list of dicts with inrange filters to apply. Format: {'column_to_filter' : [min_value, max_value]} 
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
        
        self.update_status(kind="filters", description=inrange_filters) 
        return Preprocess(data=self.data, type=self.type, info_status=self.info_status)

    def write(self, outpath: str = None, overwrite: bool = True):
        """Write computations in disk
        
        :param outpath: path to the output data
        :param overwrite: whether to overwrite existing preprocessed file in outpath
        """
        if os.path.exists(outpath):
            if os.path.isdir(outpath):
                raise Exception("Path exists and is a directory.")
            elif not overwrite:
                raise Exception("Path already exists. Set 'overwrite' to true if you wish to proceed.")

        # WRITING FILE  
        self.data.to_csv(outpath, single_file=True, index=False)

        self.update_status(kind="write", description=datetime.now()) 

        # WRITING LOG FILE
        logfile_name = os.path.basename(outpath)[:-4] + "_log.json"
        with open(logfile_name, 'w') as out_logfile:
            json.dump(self.info_status, out_logfile, default=str)

        return Preprocess(data=self.data, type=self.type, info_status=self.update_status)

    def format_new_data(
        self,
        write: bool = True, outpath: str = None, overwrite: bool = True,
        CNPJ_only_numbers: bool = True,
        CNPJ_to_keep_path: str = None,
        inrange_filters: List[Dict[str, List[float]]] = []
    ):
        """Format raw data.

        :param write: whether to write in disk after computations
        :param outpath: path to the output data
        :param overwrite: whether to overwrite existing preprocessed file in outpath
        :param CNPJ_only_numbers: remove CNPJ special characters and keep only numbers
        :param CNPJ_to_keep_path: path to csv data with CNPJs to keep 
        :param inrange_filters: list of dicts with inrange filters to apply. Format: {'column_to_filter' : [min_value, max_value]} 
        """

        # CHECKING TYPE
        if self.type == "existing":
            raise Exception("Can't preprocess already preprocessated data.")

        # CHECKING IF ARGUMENTS ARE CONSISTENT
        if write and not bool(outpath):
            raise Exception("If 'write' is true, then outpath must be specified.")

        # KEEP ONLY NUMBERS IN CNPJ
        if CNPJ_only_numbers:
            self.data = self.data.replace({'CNPJ_FUNDO': r'[^\d]'}, {'CNPJ_FUNDO': ''}, regex=True)  

        # CNPJ DATA TO KEEP
        if CNPJ_to_keep_path:
            self.apply_merge(CNPJ_to_keep_path)

        # INRANGE FILTERS:
        if inrange_filters:
            self.apply_filters(inrange_filters)

        # WRITING
        if write:
            return self.write(outpath=outpath, overwrite=overwrite)
        else:
            return Preprocess(data=self.data, type=self.type)