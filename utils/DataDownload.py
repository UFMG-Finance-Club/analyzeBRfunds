import requests
import pandas_datareader as pdr

import datetime
import pandas as pd
import os
from collections import OrderedDict

import warnings
from typing import List

def get_months_interval(months_interval: List[datetime.date]) -> List[str]:
    """Return list of months between a given interval.
    
    Args:
        months_interval: list with first and last dates. Day is ignored.
    Returns:
        A list of strings with months contained in the interval.
        By default, interval is assumed to be inclusive by left
        and exclusive by right. String format is '%Y%m'.
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
def download_funds(first_date: List[int], last_date: List[int], outpath: str) -> List[str]:
    """Download and write funds' data.
    
    Designed for downloading from CVM public sources and 
    writing in csv. Smaller unit of time to ask for 
    downloading is the month. 

    Args:
        first_date: list with first year and month
        last_date: list with last year and month
        outpath: directory path to save downloaded data
    Returns:
        A list of the output paths of downloaded data.
    """

    # CHECKING OUTPATH
    if not os.path.isdir(outpath):
        raise Exception(f"{outpath} is not a directory.")

    # GETTING ALL MONTHS IN THE INTERVAL
    months_interval = [
        datetime.date(first_date[0], first_date[1], 1),
        datetime.date(last_date[0], last_date[1], 1)
    ]
    if months_interval[0] == months_interval[1]:
        months_download = [months_interval[0].strftime(r"%Y%m")]
    else:
        months_download = get_months_interval(months_interval)

    # GENERATING FILE NAMES
    inpath_structure = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_"

    input_file_names = [f"{inpath_structure}{d}.csv" for d in months_download]
    output_file_names = [os.path.join(outpath, f"{d}.csv") for d in months_download]

    # DOWNLOAD FILES

    for i in range(len(input_file_names)):
        if os.path.exists(output_file_names[i]):
            warnings.warn(f"File {output_file_names[i]} skipped since already exists.")
        else:
            r = requests.get(input_file_names[i], allow_redirects=True)
            open(output_file_names[i], "wb").write(r.content)

    return output_file_names

# DOWNLOAD IBOV
def download_ibov(date_interval: List[datetime.date], outpath: str = None) -> pd.DataFrame:
    """Download IBOV data from Yahoo Finance
    
    Args:
        date_interval: list of datetime objects with first
            and last date of desired data.
        outpath: optional file outpath to write downloaded
            data in csv.
    Returns:
        A Pandas DataFrame with downloaded data.
    """

    data = (
        pdr.data.DataReader("^BVSP", data_source="yahoo", start=date_interval[0], end=date_interval[1])
        .reset_index()
        .loc[:, ["Date", "Adj Close"]]
        .rename({"Adj Close": "Value"}, axis=1)
        .assign(Name="IBOV")
    )
    data = data[["Date", "Name", "Value"]]

    if outpath:
        data.to_csv(outpath, index=False)

    return data

# DOWNLOAD RISK FREE
def download_riskfree(date_interval: List[datetime.date], outpath: str) -> pd.DataFrame:
    """Download Risk Free data (30-day DI swap) from NEFIN

    Args:
        date_interval: list of datetime objects with first
            and last date of desired data.
        outpath: file outpath to write downloaded data in csv.
    Returns:
        A Pandas DataFrame with downloaded data.
    """

    r = requests.get("https://nefin.com.br/resources/risk_factors/Risk_Free.xls", allow_redirects=True)
    outpath_xls = os.path.join(os.path.dirname(outpath), "Risk_free.xls")
    open(outpath_xls, "wb").write(r.content)

    data = pd.read_excel(outpath_xls, dtype={"year" : str, "month" : str, "day" : str})
    data["Date"] = pd.to_datetime(data["year"] + "/" + data["month"] + "/" + data["day"]).dt.date
    data = (
        data
        .loc[:, ["Date", "Risk_free"]]
        .rename({"Risk_free": "Value"}, axis=1)
        .assign(Name="Risk_free")
    )
    data = data[["Date", "Name", "Value"]]
    data = data[data["Date"].between(date_interval[0], date_interval[1], inclusive="both")]
    data.to_csv(outpath, index=False)

    return data