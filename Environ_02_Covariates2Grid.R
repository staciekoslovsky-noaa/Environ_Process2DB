# Process environmental covariates for grid centroids
# Created by S. Koslovsky, 7MAY2018

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

# Run code -------------------------------------------------------
# Import data into DB 
con <- RPostgreSQL::dbConnect(PostgreSQL(), 
                              dbname = Sys.getenv("pep_db"), 
                              host = Sys.getenv("pep_ip"), 
                              user = Sys.getenv("pep_admin"), 
                              password = Sys.getenv("admin_pw"))

# Get centroid layer from DB for looping
centroid <- dbGetQuery(con, "SELECT cell FROM base.geo_analysis_grid_centroid")

# Process environmental covariates for each centroid
# Variable acpcp
dbSendQuery(con, "DROP TABLE IF EXISTS base.tbl_analysis_grid_cov_acpcp;")

sql <- paste("CREATE TABLE base.tbl_analysis_grid_cov_acpcp AS
                SELECT cell, lower(fdatetime_range) as fdatetime_range_start, ST_Value(rast, centroid) as rast_acpcp
                FROM base.geo_analysis_grid_centroid
                LEFT JOIN environ.tbl_narr_acpcp
                ON ST_Intersects(rast, centroid)
                WHERE date_part('month', lower(fdatetime_range)::date) >= 3 AND
                date_part('month', lower(fdatetime_range)::date) <= 6 AND
                cell = ", centroid$cell[1], sep = "" )
dbSendQuery(con, sql)

for (i in 2:nrow(centroid)){
  cell <- centroid$cell[i]
  sql <- paste("INSERT INTO base.tbl_analysis_grid_cov_acpcp
                SELECT cell, lower(fdatetime_range) as fdatetime_range_start, ST_Value(rast, centroid) as rast_acpcp
                FROM base.geo_analysis_grid_centroid
                LEFT JOIN environ.tbl_narr_acpcp
                ON ST_Intersects(rast, centroid)
                WHERE date_part('month', lower(fdatetime_range)::date) >= 3 AND
                date_part('month', lower(fdatetime_range)::date) <= 6 AND
                cell = ", cell, sep = "" )
  dbSendQuery(con, sql)
}

# Variable air2m
dbSendQuery(con, "DROP TABLE IF EXISTS base.tbl_analysis_grid_cov_air2m;")

sql <- paste("CREATE TABLE base.tbl_analysis_grid_cov_air2m AS
                SELECT cell, lower(fdatetime_range) as fdatetime_range_start, ST_Value(rast, centroid) as rast_air2m
                FROM base.geo_analysis_grid_centroid
                LEFT JOIN environ.tbl_narr_air2m
                ON ST_Intersects(rast, centroid)
                WHERE date_part('month', lower(fdatetime_range)::date) >= 3 AND
                date_part('month', lower(fdatetime_range)::date) <= 6 AND
                cell = ", centroid$cell[1], sep = "" )
dbSendQuery(con, sql)

for (i in 2:nrow(centroid)){
  cell <- centroid$cell[i]
  sql <- paste("INSERT INTO base.tbl_analysis_grid_cov_air2m
                SELECT cell, lower(fdatetime_range) as fdatetime_range_start, ST_Value(rast, centroid) as rast_air2m
                FROM base.geo_analysis_grid_centroid
                LEFT JOIN environ.tbl_narr_air2m
                ON ST_Intersects(rast, centroid)
                WHERE date_part('month', lower(fdatetime_range)::date) >= 3 AND
                date_part('month', lower(fdatetime_range)::date) <= 6 AND
                cell = ", cell, sep = "" )
  dbSendQuery(con, sql)
}

# Variable airsfc
dbSendQuery(con, "DROP TABLE IF EXISTS base.tbl_analysis_grid_cov_airsfc;")

sql <- paste("CREATE TABLE base.tbl_analysis_grid_cov_airsfc AS
                SELECT cell, lower(fdatetime_range) as fdatetime_range_start, ST_Value(rast, centroid) as rast_airsfc
                FROM base.geo_analysis_grid_centroid
                LEFT JOIN environ.tbl_narr_airsfc
                ON ST_Intersects(rast, centroid)
                WHERE date_part('month', lower(fdatetime_range)::date) >= 3 AND
                date_part('month', lower(fdatetime_range)::date) <= 6 AND
                cell = ", centroid$cell[1], sep = "" )
dbSendQuery(con, sql)

for (i in 2:nrow(centroid)){
  cell <- centroid$cell[i]
  sql <- paste("INSERT INTO base.tbl_analysis_grid_cov_airsfc
                SELECT cell, lower(fdatetime_range) as fdatetime_range_start, ST_Value(rast, centroid) as rast_airsfc
                FROM base.geo_analysis_grid_centroid
                LEFT JOIN environ.tbl_narr_airsfc
                ON ST_Intersects(rast, centroid)
                WHERE date_part('month', lower(fdatetime_range)::date) >= 3 AND
                date_part('month', lower(fdatetime_range)::date) <= 6 AND
                cell = ", cell, sep = "" )
  dbSendQuery(con, sql)
}

# Variable prmsl
dbSendQuery(con, "DROP TABLE IF EXISTS base.tbl_analysis_grid_cov_prmsl;")

sql <- paste("CREATE TABLE base.tbl_analysis_grid_cov_prmsl AS
               SELECT cell, lower(fdatetime_range) as fdatetime_range_start, ST_Value(rast, centroid) as rast_prmsl
               FROM base.geo_analysis_grid_centroid
               LEFT JOIN environ.tbl_narr_prmsl
               ON ST_Intersects(rast, centroid)
               WHERE date_part('month', lower(fdatetime_range)::date) >= 3 AND
               date_part('month', lower(fdatetime_range)::date) <= 6 AND
               cell = ", centroid$cell[1], sep = "" )
dbSendQuery(con, sql)

for (i in 2:nrow(centroid)){
  cell <- centroid$cell[i]
  sql <- paste("INSERT INTO base.tbl_analysis_grid_cov_prmsl
               SELECT cell, lower(fdatetime_range) as fdatetime_range_start, ST_Value(rast, centroid) as rast_prmsl
               FROM base.geo_analysis_grid_centroid
               LEFT JOIN environ.tbl_narr_prmsl
               ON ST_Intersects(rast, centroid)
               WHERE date_part('month', lower(fdatetime_range)::date) >= 3 AND
               date_part('month', lower(fdatetime_range)::date) <= 6 AND
               cell = ", cell, sep = "" )
  dbSendQuery(con, sql)
}

# Variable uwnd
dbSendQuery(con, "DROP TABLE IF EXISTS base.tbl_analysis_grid_cov_uwnd;")

sql <- paste("CREATE TABLE base.tbl_analysis_grid_cov_uwnd AS
               SELECT cell, lower(fdatetime_range) as fdatetime_range_start, ST_Value(rast, centroid) as rast_uwnd
               FROM base.geo_analysis_grid_centroid
               LEFT JOIN environ.tbl_narr_uwnd
               ON ST_Intersects(rast, centroid)
               WHERE date_part('month', lower(fdatetime_range)::date) >= 3 AND
               date_part('month', lower(fdatetime_range)::date) <= 6 AND
               cell = ", centroid$cell[1], sep = "" )
dbSendQuery(con, sql)

for (i in 2:nrow(centroid)){
  cell <- centroid$cell[i]
  sql <- paste("INSERT INTO base.tbl_analysis_grid_cov_uwnd
               SELECT cell, lower(fdatetime_range) as fdatetime_range_start, ST_Value(rast, centroid) as rast_uwnd
               FROM base.geo_analysis_grid_centroid
               LEFT JOIN environ.tbl_narr_uwnd
               ON ST_Intersects(rast, centroid)
               WHERE date_part('month', lower(fdatetime_range)::date) >= 3 AND
               date_part('month', lower(fdatetime_range)::date) <= 6 AND
               cell = ", cell, sep = "" )
  dbSendQuery(con, sql)
}

# Variable vwnd
dbSendQuery(con, "DROP TABLE IF EXISTS base.tbl_analysis_grid_cov_vwnd;")

sql <- paste("CREATE TABLE base.tbl_analysis_grid_cov_vwnd AS
                SELECT cell, lower(fdatetime_range) as fdatetime_range_start, ST_Value(rast, centroid) as rast_vwnd
                FROM base.geo_analysis_grid_centroid
                LEFT JOIN environ.tbl_narr_vwnd
                ON ST_Intersects(rast, centroid)
                WHERE date_part('month', lower(fdatetime_range)::date) >= 3 AND
                date_part('month', lower(fdatetime_range)::date) <= 6 AND
               cell = ", centroid$cell[1], sep = "" )
dbSendQuery(con, sql)

for (i in 2:nrow(centroid)){
  cell <- centroid$cell[i]
  sql <- paste("INSERT INTO base.tbl_analysis_grid_cov_vwnd
                SELECT cell, lower(fdatetime_range) as fdatetime_range_start, ST_Value(rast, centroid) as rast_vwnd
                FROM base.geo_analysis_grid_centroid
                LEFT JOIN environ.tbl_narr_vwnd
                ON ST_Intersects(rast, centroid)
                WHERE date_part('month', lower(fdatetime_range)::date) >= 3 AND
                date_part('month', lower(fdatetime_range)::date) <= 6 AND
                cell = ", cell, sep = "" )
  dbSendQuery(con, sql)
}

# Variable seaice
### date_part('month', fdate) >= 3 AND date_part('month', fdate) <= 6 AND
dbSendQuery(con, "DROP TABLE IF EXISTS base.tbl_analysis_grid_cov_seaice;")

sql <- paste("CREATE TABLE base.tbl_analysis_grid_cov_seaice AS
                SELECT cell, fdate, ST_Value(rast, centroid) as rast_seaice
                FROM base.geo_analysis_grid_centroid
                LEFT JOIN environ.tbl_sic_cdr_conc
                ON ST_Intersects(rast, centroid)
                WHERE cell = ", centroid$cell[1], sep = "" )
dbSendQuery(con, sql)

for (i in 2:nrow(centroid)){
  cell <- centroid$cell[i]
  sql <- paste("INSERT INTO base.tbl_analysis_grid_cov_seaice
                SELECT cell, fdate, ST_Value(rast, centroid) as rast_seaice
                FROM base.geo_analysis_grid_centroid
                LEFT JOIN environ.tbl_sic_cdr_conc
                ON ST_Intersects(rast, centroid)
                WHERE cell = ", cell, sep = "" )
  dbSendQuery(con, sql)
}




# Create merged table
dbSendQuery(con, "CREATE INDEX idx_cov_acpcp ON base.tbl_analysis_grid_cov_acpcp (cell, fdatetime_range_start);")
dbSendQuery(con, "CREATE INDEX idx_cov_air2m ON base.tbl_analysis_grid_cov_air2m (cell, fdatetime_range_start);")
dbSendQuery(con, "CREATE INDEX idx_cov_airsfc ON base.tbl_analysis_grid_cov_airsfc (cell, fdatetime_range_start);")
dbSendQuery(con, "CREATE INDEX idx_cov_prmsl ON base.tbl_analysis_grid_cov_prmsl (cell, fdatetime_range_start);")
dbSendQuery(con, "CREATE INDEX idx_cov_uwnd ON base.tbl_analysis_grid_cov_uwnd (cell, fdatetime_range_start);")
dbSendQuery(con, "CREATE INDEX idx_cov_vwnd ON base.tbl_analysis_grid_cov_vwnd (cell, fdatetime_range_start);")

dbSendQuery(con, "DROP TABLE IF EXISTS base.tbl_analysis_grid_cov_wx;")
dbSendQuery(con, "CREATE TABLE base.tbl_analysis_grid_cov_wx AS
                  SELECT cell, fdatetime_range_start, rast_acpcp, rast_air2m, rast_airsfc, rast_prmsl, rast_uwnd, rast_vwnd
                  FROM base.tbl_analysis_grid_cov_acpcp
                  LEFT JOIN base.tbl_analysis_grid_cov_air2m
                  USING (cell, fdatetime_range_start)
                  LEFT JOIN base.tbl_analysis_grid_cov_airsfc
                  USING (cell, fdatetime_range_start)
                  LEFT JOIN base.tbl_analysis_grid_cov_prmsl
                  USING (cell, fdatetime_range_start)
                  LEFT JOIN base.tbl_analysis_grid_cov_uwnd
                  USING (cell, fdatetime_range_start)
                  LEFT JOIN base.tbl_analysis_grid_cov_vwnd
                  USING (cell, fdatetime_range_start);")

# Create indexes
dbSendQuery(con, "CREATE INDEX idx_cov_wx ON base.tbl_analysis_grid_cov_wx (cell, fdatetime_range_start);")
# dbSendQuery(con, "CREATE INDEX idx_cov_seaice ON base.tbl_analysis_grid_cov_seaice (cell, fdate);")

# Delete duplicate tables
# dbSendQuery(con, "DROP TABLE IF EXISTS base.tbl_analysis_grid_cov_acpcp;")
# dbSendQuery(con, "DROP TABLE IF EXISTS base.tbl_analysis_grid_cov_air2m;")
# dbSendQuery(con, "DROP TABLE IF EXISTS base.tbl_analysis_grid_cov_airsfc;")
# dbSendQuery(con, "DROP TABLE IF EXISTS base.tbl_analysis_grid_cov_prmsl;")
# dbSendQuery(con, "DROP TABLE IF EXISTS base.tbl_analysis_grid_cov_uwnd;")
# dbSendQuery(con, "DROP TABLE IF EXISTS base.tbl_analysis_grid_cov_vwnd;")