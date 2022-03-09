import pandas as pd
import datetime
from ..utils import DataDownload
from typing import List, Dict


class PerformanceMetrics():

    def __init__(self, inpath: str, first_date: List[int] = None, last_date: List[int] = None) -> None:
        """A class to evalute several financial metrics to funds' data.
        
        :param inpath: path with preprocessed data to read
        :param first_date: list with first year and month
        :param last_date: list with last year and month
        """

        # READ FILES
        self.data = (
            pd.read_csv(
                inpath, parse_dates=["Date"],
                names=["Name","Date", "VL_TOTAL", "Value", "VL_PATRIM_LIQ", "CAPTC_DIA", "RESG_DIA", "NR_COTST"],
                usecols=["Date", "Name", "Value"]
            )
        )
        self.data["Asset"] = "FUNDO"

        # FILTERING BY DATE
        if first_date:
            self.data = self.data[self.data["Date"] >= datetime.date(first_date[0], first_date[1], 1)]
        if last_date:
            self.data = self.data[self.data["Date"] <= datetime.date(last_date[0], last_date[1], 31)]

        # ADJUSTING OTHER PARAMS
        self.update_correspondence()
        self.returns_data = None

    def update_correspondence(self) -> None:
        self.correspondence = self.data[["Asset", "Name"]].drop_duplicates()

    def increment_with(self, inpath_bases: Dict[str, str]):
        """Increment data with external sources

        :param inpath_bases: dict with path to external bases and its names
        """

        for name_base, path_base in inpath_bases.items():
            
            path_base = path_base if path_base else None

            data_iter = DataDownload.download_data(
                first_date=min(self.data["Date"]), last_date=max(self.data["Date"]),
                asset=name_base, outpath=path_base
            )
            data_iter["Asset"] = name_base
            self.data = pd.concat([self.data, data_iter])

        # IF RETURN DATA WAS ALREADY CALCULATED, ASK TO UPDATE IT
        if self.returns_data:
            self.get_returns()
        self.update_correspondence()

    def get_returns(self):
        """Compute returns

        """
        self.returns_data = (
            self.data
            .drop("Name", axis=1)
            .pivot(index="Date", columns="Asset", values="Value")
            .dropna(axis=1)
            .pct_change()
        )
