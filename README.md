# analyzeBRfunds
Get, manipulate and analyze data on Brazilian investment funds.

We provide three main modules, **utils.DataDownload**, **preprocess.DataClean** e **metrics.FinancialMetrics**, aimed to download Brazilian funds data (and auxiliary data such as Brazilian risk-free asset or Ibovespa daily prices), apply some basic and general transformations and extract metrics such as return or alphas and betas, respectively. Behind the scenes, [Dask](https://docs.dask.org/en/stable/) is used in **preprocess.DataClean** and [Pandas](https://pandas.pydata.org/) in **metrics.FinancialMetrics**, so further transformations in data, in any of the steps, can be easily applied using these modules without the need to re-read the files.

We recognize data wrangling is an extremely flexible activity, and by no means we intend to provide a general framework to all the most frequent tasks one does when dealing with funds (and financial, in general) data. Indeed, we just introduce some very basic wrappers, developed in a particular context (presented in this notebook) where they were quite useful, and actually can still be for an external user, but it's obviously expected one can -- and should -- persist in data exploration with another Dask/Pandas/etc resources. 

As of March 2022, this code is highly experimental and should be used with cautious in a production environment. 

## Usage

A version in Portuguese of this tutorial is presented in this notebook (with output code shown).

To clone this repository in your computer:
```
git clone https://github.com/UFMG-Finance-Club/analyzeBRfunds.git
```

### DataDownload

This provides functions to download Brazilian funds data (from [this source](http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/)) and additional data (explored in **FinancialMetrics** section).

Funds data, while recorded in a daily basis, is stored monthly, so this is the smallest unit of time we allow for download. You should specify startind and ending month as a list of integers, the first one being the year and the last being the month.

```python
from analyzeBRfunds.utils import DataDownload

(
    DataDownload.download_funds(
        first_date=[2017, 1], last_date=[2022, 1], 
        outpath="data/raw"
    )
)
```

### DataClean

We provide three main data transformations, the last two directly aimed to reduce amount of funds (a generally welcomed practice in this context):

1. **Keeping only numbers in CNPJ**: done automatically, unless specified otherwise.
2. **Merging with another CNPJ base, and keeping only CNPJs in this new base**: to reduce funds to a particular desired subset and to optionally increment data with additional useful information presented in the new base (such as funds' name, for example). Later, we'll add functionality to get funds' name automatically from CVM.
3. **Applying a specific set of filters**: these filters take a column and search for funds whose specified column fall into a given range.

In this example we'll merge with a database in `data/auxiliary/funds.csv` that also provides funds' name information, add a filter that specifies funds must have data on 2018-01-03 and write output on **preprocessed/2017_2021.csv**.

```python
(
    DataClean.Preprocess(inpath="data/raw")
    .format_new_data(
        outpath="data/preprocessed/2017_2021.csv",
        inrange_filters=[{"DT_COMPTC" : ["2018-01-03", "2018-01-03"]}],
        CNPJ_to_keep_path="data/auxiliary/fundos.csv", sep=";"
    )
)
```

We can also apply these transformations separetely and later on. In the following example, we'll read the preprocessed base (result of the above code) and select only funds whose number of investors were greater than 250 on 2018-01-03. Since data was already preprocessed, we use the value *existing* in the argument *type*. 

```python
import numpy as np

prep_obj = (
    DataClean.Preprocess(inpath="/home/marcel/outros/ckrepo/analyzeBRfunds/data/preprocess/2017_2021.csv", type="existing")
    .apply_filters([{"DT_COMPTC" : ["2020-01-03", "2020-01-03"], "NR_COTST" : [250, np.inf]}])
)
```

Generated data can be accessed via 
