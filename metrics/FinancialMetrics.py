import pandas as pd

import datetime
from utils import DataDownload
from scipy import stats
import numpy as np

from typing import List, Union, Optional

class PerformanceMetrics():
    """A class to evalute several financial metrics to funds' data.
    
    While it is possible to use raw data here, it is recommended you
    preprocess it before and reduce amount of CNPJs considered to the
    minimum possible.
    
    Attributes:
        data: Pandas DataFrame with data being used
        correspondence: Pandas DataFrame with assets (categorized) 
            being used  
    """

    def __init__(self, inpath: str, first_date: List[int] = None, last_date: List[int] = None) -> None:
        """Initialize class. Read inpath files.
        
        Args:
            inpath: path with data to read
            first_date: optional integer list with minimum date to 
                filter by. Format is [year, month, day].
            last_date: optional list with maximum date to filter by.
                Format is [year, month, day].
        """

        # READ FILES
        self.data = pd.read_csv(
            inpath, usecols=["CNPJ_FUNDO", "DT_COMPTC", "VL_QUOTA"], 
            dtype={"CNPJ_FUNDO" : str, "VL_QUOTA" : float}
        )
        self.data.columns = ["Name", "Date", "Value"]
        self.data["Date"] = pd.to_datetime(self.data["Date"]).dt.date
        self.data["Asset"] = "FUNDO"

        # FILTERING BY DATE
        if first_date:
            self.data = self.data[self.data["Date"] >= datetime.date(first_date[0], first_date[1], first_date[2])]
        if last_date:
            self.data = self.data[self.data["Date"] <= datetime.date(last_date[0], last_date[1], last_date[2])]

        # ADJUSTING OTHER PARAMS
        self.update_correspondence()
        self.get_returns(silent=True)
        
    def update_correspondence(self) -> None:
        self.correspondence = self.data[["Asset", "Name"]].drop_duplicates()

    def increment_with(self, target_base: str, outpath_base: str = None) -> None:
        """Increment data with external sources.

        Data is re-downloaded even if it exists. Routines
        used for downloading are from utils/DataDownload.

        Args:
            target_base: wheter 'IBOV' or 'RISK_FREE'
            outpath_base: output to save base
        """
        
        date_interval = [min(self.data["Date"]), max(self.data["Date"])]

        if target_base == "IBOV":
            aditional_data = DataDownload.download_ibov(
                date_interval=date_interval, outpath=outpath_base
            )
            aditional_data["Date"] = aditional_data["Date"].dt.date
        elif target_base == "RISK_FREE":
            aditional_data = DataDownload.download_riskfree(
                date_interval=date_interval, outpath=outpath_base
            )

        aditional_data["Asset"] = target_base
        self.data = pd.concat([self.data, aditional_data])

        self.get_returns(silent=True)
        self.update_correspondence()

    def get_returns(self, silent: bool = False) -> Optional[pd.DataFrame]:
        """Compute returns

        Args:
            silent: whether to return data
        """
        self.returns_data = (
            self.data
            .drop("Asset", axis=1)
            .pivot(index="Date", columns="Name", values="Value")
            .pct_change()
        )

        if not silent:
            return self.returns_data

    def estimate_factors(self, selected: Union[str, List[str]] = "all"):
        """Estimate alphas and betas from the one-factor model.
        
        Risk free and market data need to be on funds' data. They can
        be included via *increment_with* method. To estimate beta and
        alpha, we use Ordinary Least Squared (OLS) in a simple
        regression where excess market returns are the preditor and
        excess funds' returns are the predictable variable.

        Args:
            selected: list of selected CNPJs to evalute this metric. 
        """

        if isinstance(selected, list) or selected in self.returns_data.columns:
            data_factors = self.returns_data[selected + ["IBOV", "Risk_free"]]
        elif selected == "all":
            data_factors = self.returns_data.copy()
        else:
            raise Exception("Option not recognized or asset isn't on data.")
        
        data_factors["Market"] = data_factors["IBOV"] - data_factors["Risk_free"]
        data_factors = data_factors.drop("IBOV", axis=1)

        linear_reg_data = dict()

        X = data_factors["Market"].to_numpy()
        for fund in data_factors.columns:
            
            if fund not in ["Market", "Risk_free"]:
                y = (data_factors[fund] - data_factors["Risk_free"]).to_numpy()
                mask = ~np.isnan(X) & ~np.isnan(y)

                linear_reg_data[fund] = stats.linregress(X[mask], y[mask])

        return linear_reg_data