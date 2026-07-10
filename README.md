# Google Trends and Motor Vehicle Theft

Code and processed data for my doctoral dissertation:

> Liu, Yu-Hsuan (2024). *Exploring the Correlation Between Google Trends and Crime Statistics: A Focus on Motor Vehicle Theft in the United States.* CUNY Academic Works. https://academicworks.cuny.edu/gc_etds/5949

The dissertation validates motor vehicle theft estimates derived from Google Trends against official crime statistics (UCR, NCVS, NICB, NIBRS, and 911 calls for service) at three levels of aggregation: state-year, DMA-year, and DMA-month. Models include GLS, fixed effects, an interrupted time series around a natural experiment, and instrumental variables. Google Trends estimates track the official statistics well, with the NCVS as the main exception.

`yu_hsuan_liu_phd_dissertation_final_letter.Rmd` is the manuscript source and contains all statistical analyses, tables, and figures. `Liu_Dissertation_GT_Code/` holds the Python notebooks that collected and processed the raw inputs (Google Trends samples, ACS covariates, 911 calls from 22 city open-data portals, NOAA weather, BLS car-price series) together with the processed CSVs the Rmd reads.

The processed data are included, so the statistical models can be rerun directly. Rerunning the collection steps from scratch requires adjusting local paths inside the notebooks, and the San Antonio 911 scraper expects `msedgedriver.exe` next to it (download the version matching your Edge). Rendering the Rmd to PDF also needs the CUNY dissertation LaTeX class and the bibliography, which are not part of this repository.
