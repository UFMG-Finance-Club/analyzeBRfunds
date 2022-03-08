import pandas as pd
import datetime
import warnings
from typing import List, Dict

class PerformanceMetrics():

    def __init__(self, 
        inpath: str, first_date: List[int] = None, last_date: List[int] = None
    ) -> None:
        """A class to evalute several financial metrics to funds' data.
        
        :param inpath: path with preprocessed data to read
        :param first_date: list with first year and month
        :param last_date: list with last year and month
        """

        # READ FILES
        self.data = (
            pd.read_csv(
                inpath, parse_dates=["DT_COMPTC"], 
                names=["Name","Date", "VL_TOTAL", "Value", "VL_PATRIM_LIQ", "CAPTC_DIA", "RESG_DIA", "NR_COTST"],
                usecols=["Date", "Name", "Value"]
            )
        )
        self.data["Asset"] = "FUNDO"

        # FILTERING BY DATE
        if first_date:
            self.data = self.data[self.data["DT_COMPTC"] >= datetime.date(first_date[0], first_date[1], 1)]
        if last_date:
            self.data = self.data[self.data["DT_COMPTC"] <= datetime.date(last_date[0], last_date[1], 31)]

        # ADJUSTING OTHER PARAMS
        self.returns_data = None

    def increment_with(self, inpath_bases: Dict[str, str]):
        """Increment data with external sources
        
        :param *inpath_bases: dict with path to external bases and its names
        """

        columns_expected = ["Date", "Name", "Value"]

        for name_base, path_base in inpath_bases.items():
            data_iter = pd.read_csv(path_base, usecols=columns_expected)
            data_iter["Asset"] = name_base
            self.data = pd.concat([self.data, data_iter])

        # IF RETURN DATA WAS ALREADY CALCULATED, ASK TO UPDATE IT
        if self.returns_data:
            self.get_returns()

    def get_returns(self):
        """Compute returns
    
        """
        if not self.returns_data:
            self.returns_data = (
                self.data
                .pivot(index="DT_COMPTC", columns="CNPJ", values="VL_QUOTA")
                .dropna(axis = 1)
                .pct_change()
            )