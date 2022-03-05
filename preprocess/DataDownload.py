import requests
import datetime
from collections import OrderedDict
from typing import List

# adicionar checagem se o diretório existe
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

def download_data(first_date: List[int], last_date: List[int], outpath: str) -> List[str]:
    """Download funds' data. Designed for downloading from CVM. It returns a list with names of the downloaded names, so you can
    use it as an argument in the preprocessing routine.  

        :param first_date: list with first year and month
        :param last_date: list with last year and month
        :param outpath: (local) path to save downloaded data
    """

    inpath_structure = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_"

    # GETTING ALL MONTHS IN THE INTERVAL
    months_interval = [
        datetime.date(first_date[0], first_date[1], 1), datetime.date(last_date[0], last_date[1], 2)
    ]
    months_download = get_months_interval(months_interval)

    # GENERATING FILE NAMES
    input_file_names = [f"{inpath_structure}{d}.csv" for d in months_download]
    output_file_names = [f"{outpath}/{d}.csv" for d in months_download]

    # DOWNLOAD FILES

    for i in range(len(input_file_names)):
        r = requests.get(input_file_names[i], allow_redirects=True)
        open(output_file_names[i], "wb").write(r.content)

    return output_file_names