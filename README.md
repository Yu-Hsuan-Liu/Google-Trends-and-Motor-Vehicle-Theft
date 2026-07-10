# Google Trends and Motor Vehicle Theft

Analysis code and processed data for the doctoral dissertation:

> Liu, Yu-Hsuan (2024). *Exploring the Correlation Between Google Trends and Crime Statistics: A Focus on Motor Vehicle Theft in the United States.* CUNY Academic Works. https://academicworks.cuny.edu/gc_etds/5949

## Overview

This project examines the linear relationships between motor vehicle theft (MVT) estimates from Google Trends, a form of digital trace data, and official crime statistics: the Uniform Crime Reports (UCR), the National Crime Victimization Survey (NCVS), the National Insurance Crime Bureau (NICB), the National Incident-Based Reporting System (NIBRS), and 911 calls for service (CFS 911). The dissertation analyzes the limitations inherent in current crime statistics, discusses measurement error, and applies statistical models that mitigate its impact in order to validate Google Trends MVT data against official MVT statistics.

Drawing on social disorganization theory and routine activity theory, the study tests the concurrent validity of Google Trends MVT estimates at three levels of aggregation: state and year, Designated Market Area (DMA) and year, and DMA and month. Methods include generalized least squares (GLS), fixed effects models, a natural experiment with interrupted time series, and instrumental variables.

The findings confirm a linear relationship between MVT estimates from Google Trends and other official crime statistics, with the exception of the NCVS. MVT estimates from Google Trends also show strong concurrent validity, mirroring trends and reacting similarly to crime covariates as other official statistics, except for temperature and precipitation. The dissertation discusses further applications of Google Trends data, how population size influences crime data estimates, and the potentials and limitations of digital trace data. To the best of the author's knowledge, it is the first study to derive multiple-level crime statistics from Google Trends using multiple GT samples and rigorous validation methods, examining both the reliability of digital trace data and its ability to illuminate the dark figure of crime.

## Repository contents

| Path | Description |
|---|---|
| `yu_hsuan_liu_phd_dissertation_final_letter.Rmd` | Full dissertation manuscript source (R Markdown, CUNY letter format). Contains all statistical analyses, tables, and figures. |
| `Liu_Dissertation_GT_Code/` | Python notebooks and scripts used to collect and preprocess the data, together with the processed CSV files they produce. |

### Data collection and processing code (`Liu_Dissertation_GT_Code/`)

| File | Purpose |
|---|---|
| `Process_GT_RAW_data_of_MVT_at_DMA_state_annual_month.ipynb` | Combines raw Google Trends MVT samples into state-annual, DMA-annual, and DMA-monthly panels. |
| `Process_ACS_County_and_MSA_Level_Data.ipynb` | Processes American Community Survey (2011 to 2022) county, MSA, and state estimates; crosswalks MSA to DMA; computes covariates (including Cronbach's alpha checks). |
| `aggregate_daily_city_911_to_month_mvt.ipynb` | Aggregates daily 911 calls for service from city open-data portals into monthly MVT counts per city. |
| `get_san_antonio_police_911_data.ipynb` | Scrapes San Antonio police 911 records with Selenium. |
| `process_large_nypd_911_data.py` | Deduplicates and aggregates the large NYPD 911 calls-for-service file. |
| `Get_Weather_Data_test.ipynb` | Processes NOAA GHCN daily weather records (average temperature, precipitation) and assigns county FIPS codes through the FCC area API. |
| `Get_CPI_new_and_used_cars.ipynb` | Reads BLS series reports on new and used car CPI and semiconductor PPI, reshaping them into a long-format monthly series. |
| `*_911_by_month.csv` | Monthly MVT-related 911 call counts for 22 U.S. cities (Atlanta through Washington DC). |
| `gt_state_mvt_2011_2022.csv`, `gt_dma_mvt_annually_2011_2022.csv`, `gt_dma_mvt_monthly_2017_2022.csv`, `gt_dma_mvt_2011_2022.csv` | Processed Google Trends MVT panels. |
| `acs_state_2011_2022.csv`, `acs_county_2011_2022.csv`, `acs_msa_2011_2022.csv` | Processed ACS covariate panels. |
| `new_and_used_car_cpi_semiconductor_ppi.csv`, `cpi_new_and_used_cars/` | Car price and semiconductor price series with the original BLS source workbooks. |
| `DMA_region_walk.csv`, `station_id_with_fips.csv` | Crosswalk files (DMA to region, weather station to FIPS). |

## Data sources

All data were collected from publicly available sources, as reflected in the code:

- Google Trends search interest for motor vehicle theft terms (multiple independent samples)
- FBI Uniform Crime Reports and NIBRS
- National Crime Victimization Survey (MSA-level estimates)
- National Insurance Crime Bureau theft reports
- 911 calls for service from municipal open-data portals in 22 cities
- American Community Survey one-year estimates, 2011 to 2022
- NOAA GHCN daily weather observations
- Bureau of Labor Statistics CPI series and FRED semiconductor PPI

## Requirements

**R** (manuscript and statistical models): tidyverse, dplyr, ggplot2, nlme, plm, lmtest, MASS, MatchIt, multiwayvcov, stargazer, kableExtra, xtable, broom, haven, readxl, janitor, psych, sf, maps, corrplot, cowplot, gridExtra, viridis, among others listed in the first chunk of the Rmd.

**Python 3** (data collection and preprocessing): pandas, numpy, matplotlib, requests, beautifulsoup4, selenium, webdriver-manager, pingouin, scikit-learn, openpyxl.

The Selenium scraping notebook (`get_san_antonio_police_911_data.ipynb`) expects `msedgedriver.exe` in its working directory. Download the driver matching your Edge version from https://developer.microsoft.com/en-us/microsoft-edge/tools/webdriver/; the binary is not tracked in this repository.

## Reproducibility notes

The processed CSV files needed by the manuscript are included in `Liu_Dissertation_GT_Code/`. Some file paths in the Rmd and the notebooks point to local drives (for example `D:/0dissertation_code_data/`) where the raw source downloads were stored; those paths must be adjusted to rerun the collection steps from scratch. Rendering the Rmd to PDF additionally requires the CUNY dissertation LaTeX class, a preamble file, and the BibTeX bibliography, which are not part of this repository.

## Citation

Liu, Yu-Hsuan (2024). "Exploring the Correlation Between Google Trends and Crime Statistics: A Focus on Motor Vehicle Theft in the United States." PhD dissertation, CUNY Graduate Center. https://academicworks.cuny.edu/gc_etds/5949

## License

MIT License. See `LICENSE`.
