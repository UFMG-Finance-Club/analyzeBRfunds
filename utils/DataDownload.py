import requests
import pandas as pd
import pandas_datareader as pdr
import datetime
import os
from collections import OrderedDict
from typing import List, Union

def get_months_interval(months_interval: List[datetime.date]) -> List[str]:
    """Return list of months between a given interval. You should not need to evoke this directly.
    
    :param months_interval: interval of months to download data ([first_month, last_month])
    """

    start_date, end_date = months_interval
    months_download = (
        OrderedDict(
            (
                (start_date + datetime.timedelta(_))
                .strftime(r"%Y%m"), None) for _ in range((end_date - start_date).days)
            ).keys()
    )
    return list(months_download)

# DOWNLOAD FUNDS DATA
def download_funds(months_interval: List[datetime.date], outpath: str) -> List[str]:
    """Download and write funds' data. Designed for downloading from CVM. It returns a list with names of the downloaded names, so you can
    use it as an argument in the preprocessing routine.  

    :param months_interval: list with date objects of first and last month/year
    :param outpath: (local) path to save downloaded data
    """

    inpath_structure = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_"

    # GETTING ALL MONTHS IN THE INTERVAL
    months_download = get_months_interval(months_interval)

    # GENERATING FILE NAMES
    input_file_names = [f"{inpath_structure}{d}.csv" for d in months_download]
    output_file_names = [os.path.join(outpath, f"{d}.csv") for d in months_download]

    # DOWNLOAD FILES

    for i in range(len(input_file_names)):
        r = requests.get(input_file_names[i], allow_redirects=True)
        open(output_file_names[i], "wb").write(r.content)

    return output_file_names

# DOWNLOAD IBOV
def download_ibov(date_interval: List[datetime.date], outpath: str = None) -> pd.DataFrame:
    """Download IBOV data from Yahoo Finance
    
    :param date_interval: date interval to download data
    :param outpath: optional outpath to write data
    """

    data = (
        pdr.data.DataReader("^BVSP", data_source="yahoo", start=date_interval[0], end=date_interval[1])
        .reset_index()
    )[["Date", "Adj Close"]]
    data.columns = ["Date", "Value"]
    data["Name"] = "IBOV"
    data = data[["Date", "Name", "Value"]]

    if outpath:
        data.to_csv(outpath, index=False)

    return data

# DOWNLOAD RISK FREE
def download_riskfree(date_interval: List[datetime.date], outpath: str) -> pd.DataFrame:
    """Download Risk Free data (30-day DI swap) from NEFIN

    :param date_interval: date interval to download data
    :param outpath: outpath to write data   
    """

    r = requests.get("https://nefin.com.br/resources/risk_factors/Risk_Free.xls", allow_redirects=True)
    outpath_xls = os.path.join(os.path.dirname(outpath), "Risk_free.xls")
    open(outpath_xls, "wb").write(r.content)

    data = pd.read_excel(outpath)
    data["Date"] = pd.to_datetime(f"{data['year']/data['month']/data['day']}")
    data = data[["Date", "Risk_free"]]
    data.columns = ["Date", "Value"]
    data["Name"] = "Risk_free"
    data = data[["Date", "Name", "Value"]]
    data = data[data["Date"].between(date_interval[0], date_interval[1], inclusive="both")]
    data.to_csv(outpath, index=False)

    return data

# DOWNLOAD DATA - GENERAL
def download_data(
    first_date: List[int], last_date: List[int], 
    asset: Union[str, List[str]] = "FUNDS", outpath: str = None
) -> None:
    """General function to download supported data
    
    :param first_date: list with first year and month
    :param last_date: list with last year and month
    :param asset: which asset data to download
    :param outpath: (local) path to save downloaded data
    """

    first_day = first_date[2] if len(first_date) == 3 else 5
    last_day = last_date[2] if len(last_date) == 3 else 5

    date_interval = [
        datetime.date(first_date[0], first_date[1], first_day), 
        datetime.date(last_date[0], last_date[1], last_day)
    ]
    
    if asset == "FUNDS":
        return download_funds(date_interval, outpath)
    elif asset == "IBOVESPA":
        return download_ibov(date_interval, outpath)
    elif asset == "RISK-FREE":
        return download_riskfree(date_interval, outpath)
    else:
        raise Exception(f"No support for asset '{asset}'")
