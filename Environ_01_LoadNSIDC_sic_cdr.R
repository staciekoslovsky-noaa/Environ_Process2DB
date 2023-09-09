# Load NARR vwnd data into pep DB
# S. Hardy, 1SEPT2017

# Create functions -----------------------------------------------
# Function to install packages needed
install_pkg <- function(x)
{
  if (!require(x,character.only = TRUE))
  {
    install.packages(x,dep=TRUE)
    if(!require(x,character.only = TRUE)) stop("Package not found")
  }
}

# Install libraries ----------------------------------------------
install_pkg("RPostgreSQL")
install_pkg("reshape2")
install_pkg("raster")
install_pkg("ncdf4")
install_pkg("sp")
install_pkg("rpostgis")
install_pkg("stringr")

# Run code -------------------------------------------------------
# Import data into DB -------------------------------------------------------------------
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              password = Sys.getenv("admin_pw"))

dbGetQuery(con, "CREATE TABLE IF NOT EXISTS environ.tbl_sic_cdr_conc (id serial NOT NULL PRIMARY KEY, rid int NOT NULL, fdate date, rast raster);")
dbGetQuery(con, "CREATE TABLE IF NOT EXISTS environ.tbl_sic_cdr_stdev (id serial NOT NULL PRIMARY KEY, rid int NOT NULL, fdate date, rast raster);")
dbGetQuery(con, "CREATE TABLE IF NOT EXISTS environ.tbl_sic_cdr_melt (id serial NOT NULL PRIMARY KEY, rid int NOT NULL, fdate date, rast raster);")
dbGetQuery(con, "CREATE TABLE IF NOT EXISTS environ.tbl_sic_cdr_qa (id serial NOT NULL PRIMARY KEY, rid int NOT NULL, fdate date, rast raster);")

# Merge individual netcdfs for each year into single table for import into the DB -------
# wd <- "O://Data/GIS_External/SeaIce/Data_NSIDC_CDR/Version3_Revision1/"
wd <- "O://Data/GIS_External/SeaIce/Data_NSIDC_CDR/Version4/"
#yrs <- c(2004:2021)
yrs <- 2022

# var_names <- c("seaice_conc_cdr", "stdev_of_seaice_conc_cdr", "melt_onset_day_seaice_conc_cdr", "qa_of_seaice_conc_cdr")
var_names <- c("cdr_seaice_conc", "stdev_of_cdr_seaice_conc", "melt_onset_day_cdr_seaice_conc", "qa_of_cdr_seaice_conc")
tbl_names <- c("tbl_sic_cdr_conc", "tbl_sic_cdr_stdev", "tbl_sic_cdr_melt", "tbl_sic_cdr_qa")

# Set variables for processing each year, create our clipped raster, set projection to epsg:3338
clip_extent <- extent(-5000000, 1900000, 300000, 4000000)
r2 <- raster(clip_extent, resolution = c(25067.53, 25067.53), crs=CRS("+init=epsg:3338"))

# Process all files and variables for each year -----------------------------------------
for (i in 1:length(yrs)){
  yr <- yrs[i]
  setwd(paste(wd, yr, "/", sep = ""))
  files <- dir(pattern = '*.nc')
  
  dbGetQuery(con, paste("DROP TABLE IF EXISTS environ.tbl_sic_cdr_conc_", yr, sep = ""))
  dbSendQuery(con, paste("CREATE TABLE environ.tbl_sic_cdr_conc_", yr, "() INHERITS (environ.tbl_sic_cdr_conc);", sep = ""))
  
  dbGetQuery(con, paste("DROP TABLE IF EXISTS environ.tbl_sic_cdr_stdev_", yr, sep = ""))
  dbSendQuery(con, paste("CREATE TABLE environ.tbl_sic_cdr_stdev_", yr, " () INHERITS (environ.tbl_sic_cdr_stdev);", sep = ""))
  
  dbGetQuery(con, paste("DROP TABLE IF EXISTS environ.tbl_sic_cdr_melt_", yr, sep = ""))
  dbSendQuery(con, paste("CREATE TABLE environ.tbl_sic_cdr_melt_", yr, " () INHERITS (environ.tbl_sic_cdr_melt);", sep = ""))
  
  dbGetQuery(con, paste("DROP TABLE IF EXISTS environ.tbl_sic_cdr_qa_", yr, sep = ""))
  dbSendQuery(con, paste("CREATE TABLE environ.tbl_sic_cdr_qa_", yr, " () INHERITS (environ.tbl_sic_cdr_qa);", sep = ""))
  
  # Process all files for each year
  for (j in 1:length(files)){
    print(j)
    # Connect to j-th file
    file <- files[j]
    nc_tmp <- ncdf4::nc_open(file)
    nc_date <- as.Date(str_extract(file, "[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]"), format = "%Y%m%d", tz = "UTC")
    # nc_lat <- ncdf4::ncvar_get(nc_tmp, attributes(nc_tmp$dim)$names[1])
    # nc_lon <- ncdf4::ncvar_get(nc_tmp, attributes(nc_tmp$dim)$names[2])
    nc_lat <- ncdf4::ncvar_get(nc_tmp, "xgrid")
    nc_lon <- ncdf4::ncvar_get(nc_tmp, "ygrid")
    
    # Process variables for each file
    for (k in 1:length(var_names)){
      var <- var_names[k]
      nc_var <- ncdf4::ncvar_get(nc_tmp, var)
      
      # Create raster of data from netcdf
      dimnames(nc_var) <- list(lat = nc_lat, lon = nc_lon)
      tmp_nc_var <- reshape2::melt(nc_var, value.name = var)
      coordinates(tmp_nc_var) =~ lat+lon
      proj4string(tmp_nc_var) = CRS("+init=epsg:3413")
      gridded(tmp_nc_var) = TRUE
      tmp_nc_var <- raster(tmp_nc_var)
      
      # Clip and re-project raster
      tmp_nc_var <- raster::projectRaster(tmp_nc_var, r2)
      
      # Import raster data into temp table in DB
      rpostgis::pgWriteRast(con, c("environ", "temp"), raster = tmp_nc_var, overwrite = TRUE)
      dbGetQuery(con, "SELECT UpdateRasterSRID(\'environ\', \'temp\',\'rast\', 3338)")
      
      # Finish processing raster data in DB
      #rid <- as.numeric(format(nc_date, "%j"))
      dbGetQuery(con, paste("INSERT INTO environ.", tbl_names[k], "_", yr, " (rid, fdate, rast) SELECT ", i, ", \'", nc_date, "\', st_union(rast, 1) from environ.temp", sep = ""))
      dbSendQuery(con, "DROP TABLE IF EXISTS environ.temp")
    }
    
    # Close connection to netcdf file
    ncdf4::nc_close(nc_tmp)
  }
  
  # Create indexes for data
  dbGetQuery(con, paste("CREATE INDEX idx_sic_cdr_conc_", yr, " ON environ.tbl_sic_cdr_conc_", yr, " (id, fdate);", sep = ""))
  dbGetQuery(con, paste("CREATE INDEX idx_sic_cdr_stdev_", yr, " ON environ.tbl_sic_cdr_stdev_", yr, " (id, fdate);", sep = ""))
  dbGetQuery(con, paste("CREATE INDEX idx_sic_cdr_melt_", yr, " ON environ.tbl_sic_cdr_melt_", yr, " (id, fdate);", sep = ""))
  dbGetQuery(con, paste("CREATE INDEX idx_sic_cdr_qa_", yr, " ON environ.tbl_sic_cdr_qa_", yr, " (id, fdate);", sep = ""))
}

# Clean up memory, DB and files on server
dbDisconnect(con)
rm(list=ls())