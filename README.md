# Environmental Data Management

Environmental data (NARR and NSIDC) are downloaded and stored on our internal network. Data products are loaded into the DB using the code stored in this repository. Code numbered 0+ are intended to be run sequentially as the data are available for processing. Code numbered 99 are stored for longetivity, but are intended to only be run once to address a specific issue or run as needed, depending on the intent of the code.

The data management processing code is as follows:
* **Environ_01_LoadNARR_acpcp.R** - code to import NARR precipitation data into the DB
* **Environ_01_LoadNARR_air2m.R** - code to import NARR air temperature (at 2 m) data into the DB
* **Environ_01_LoadNARR_airsfc.R** - code to import NARR air temperature (at the surface) data into the DB
* **Environ_01_LoadNARR_prmsl.R** - code to import NARR pressure (at MSL) data into the DB
* **Environ_01_LoadNARR_uwnd.R** - code to import NARR U wind vector data into the DB
* **Environ_01_LoadNARR_vwnd.R** - code to import NARR V wind vector data into the DB
* **Environ_01_LoadNSIDC_sic_cdr.R** - code to import NSIDC sea ice data into the DB
* **Environ_02_Covariates2Grid.R** - code to extract environmental covariate data to each of the cells within the grid (from March to June each year for which there are environmental covariate data
* **Environ_99_SeaIceExtentFunction4JML.txt** - code for creating a function to extract sea ice data; currently under development (and may be antiquated based on other processing that is developed)