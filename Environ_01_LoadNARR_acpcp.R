# Load NARR acpcp data into pep DB
# Created by J. London
# Modified by S. Hardy, 5APR2017

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
install_pkg("lubridate")
install_pkg("raster")
install_pkg("ncdf4")
install_pkg("RNetCDF")
install_pkg("rpostgis")

# Run code -------------------------------------------------------
# Import data into DB -------------------------------------------------------------------
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              password = Sys.getenv("admin_pw"))

sql <- "CREATE TABLE IF NOT EXISTS environ.tbl_narr_acpcp (id serial NOT NULL PRIMARY KEY, rid int NOT NULL, fdatetime_range tstzrange, rast raster);"
dbGetQuery(con, sql)
rm(sql)

wd <- "O://Data/GIS_External/Climate/NARR/acpcp/"
setwd(wd)
fls <- dir(pattern = '*.nc')
fls <- subset(fls, nchar(fls) == 13)
#yrs <- substring(fls, 7, 10)[17]
yrs <- 2021

# create our clipped raster, set projection to epsg:3338
clip_extent <- extent(-5000000, 1900000, 300000, 4000000)
r2 <- raster(clip_extent, resolution = c(25067.53, 25067.53), crs=CRS("+init=epsg:3338"))

for(i in unique(yrs)){
  dbGetQuery(con, paste("DROP TABLE IF EXISTS environ.tbl_narr_acpcp_", i, sep = ""))
  sql <- paste("CREATE TABLE environ.tbl_narr_acpcp_", i, "(CHECK (lower(fdatetime_range) >= TIMESTAMP WITH TIME ZONE '", i, 
               "-01-01 00:00:00-00' AND upper(fdatetime_range) <= TIMESTAMP WITH TIME ZONE'", i, 
               "-12-31 23:59:59-00' )) INHERITS (environ.tbl_narr_acpcp);", sep = "")
  dbGetQuery(con, sql)
  rm(sql)
  f <- subset(fls, substring(fls, 7, 10) == i)
  x <- ifelse(i == "2020" | i == "2016" | i == "2012" | i ==  "2008" | i == "2004", 2928, 2920)
  
  for (b in c(1:x)){
    print(b)
    # Create raster
    ras.tmp <- raster(paste(wd, f, sep=""), band = b)
    
    # Clip and re-project raster
    ras.tmp <- raster::projectRaster(ras.tmp, r2)

    # Import raster into DB
    rpostgis::pgWriteRast(con, c("environ", "temp_acpcp"), raster = ras.tmp, overwrite = TRUE)
    dbGetQuery(con, "SELECT UpdateRasterSRID(\'environ\', \'temp_acpcp\',\'rast\', 3338)")
    
    # Finish processing raster data in DB
    dt_char <- paste(i, "-01-01 00:00:00", sep = "")
    band_dt <- lubridate::ymd_hms(dt_char) + lubridate::hours(b*3 - 3)
    band_dt_end <- band_dt + lubridate::seconds(10799)
    sql <- paste("ALTER TABLE environ.temp_acpcp ADD COLUMN fdatetime_range tstzrange; UPDATE environ.temp_acpcp SET fdatetime_range = tstzrange('", format(band_dt, "%Y-%m-%d %H:%M:%S"), "-00','", format(band_dt_end, "%Y-%m-%d %H:%M:%S"), "-00','[]');", sep="")
    dbGetQuery(con, sql)
    sql <- paste("INSERT INTO environ.tbl_narr_acpcp_", i, " (rid, fdatetime_range, rast) SELECT rid, fdatetime_range, rast from environ.temp_acpcp;", sep = "")
    dbGetQuery(con, sql)
  }
  sql <- paste("CREATE INDEX idx_narr_acpcp_fdatetime_range_", i, " ON environ.tbl_narr_acpcp_", i, " USING GIST (fdatetime_range);", sep = "")
  dbGetQuery(con, sql)
}

# Delete temp tables from DB
dbGetQuery(con, "DROP TABLE IF EXISTS environ.temp_acpcp")
 
# Create index on layer (if it does not exist)
# dbSendQuery(con, "CREATE INDEX tbl_narr_acpcp_rast_idx ON environ.tbl_narr_acpcp USING gist(ST_ConvexHull(rast))")

# Clean up memory, DB and files on server
dbDisconnect(con)
rm(list=ls())
