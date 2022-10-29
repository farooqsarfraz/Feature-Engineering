## Clear All Variables and Console ##
rm(list=ls())
cat("\014")  

## Libraries ##
library(data.table) # wrapper around data frame
library(RMySQL) # interaction with mysql
library(rpart) # building callgroup trees
library(stringr) # library for working with strings
library(ggplot2) # plotting
library(doParallel) # parallel computation
library(lubridate) # working with dates
library(rattle)
library(dplyr)
# library(rpart.plot)
# library(RColorBrewer)
setDTthreads(threads = parallel::detectCores()/4) # threads to use for data.table


## Source Files ##
source("./feature_engineering_functions.R")
source("./config.R")


############################################################################################################
## DATASET FOR TREE BUILDING ##

## Query for Data Fetch ##
data.config <- list()

# PORT DATA #
data.config$select.query <- paste0("SELECT
                                   `AGENTID`, 
                                   `Calltime`,
                                   `UCID`,    
                                   `vdn`,
                                   `IsSale`,
                                   `On_Off`,
                                   `TotalFee`,
                                   `Calldate_PD`,
                                   `Days_Since_Active_PD`,
                                   `DialCode_Transformed_PD`,
                                   `DialCode_PD`,
                                   `Dial_code_type_PD`,
                                   `Country_code_PD`,
                                   `PortDateTime_PD`,
                                   `Action_PD`,
                                   `CompanyCode_PD`,
                                   `CompanyCode_Combined`,
                                   `companycode_Carrier`,
                                   `TypeOfService_PD`,
                                   `RoutingCode_PD`,
                                   `iso_abbreviation_PD`,
                                   `modernized_provider_PD`,
                                   `TOS_Combined_PD`,
                                   `TOSdesc_combined_GRF`,
                                   `Provider_Combined_PD`,
                                   `startdate_PD`,
                                   `enddate_PD`,
                                   `Block_GRF`,
                                   `TOS_GRF`,
                                   `TOSdesc_GRF`,
                                   `NDC_GRF`,
                                   `Locality_GRF`,
                                   `Provider_GRF`,
                                   `UTC_GRF`,
                                   `Supplement_GRF`,
                                   `BillingID_GRF`,
                                   `AdminDiv_GRF`,
                                   `Effective_Date_GRF`,
                                   `LEMin_GRF`,
                                   `LEMax_GRF`,
                                   `POS_GRF`,
                                   `OCN_GRF`
                                   FROM `smexplorerdata`.`libertymutual.smetable_red_custom`")


## Configs ##
data.config$db.credentials <- data.table(
  conn_name = c("ai_red", "arch"), 
  user = c(ai.red.db.user, arch.db.user), 
  password = c(ai.red.db.password, arch.db.password), 
  db_name = c(ai.red.db.name, arch.db.name), 
  db_host = c(ai.red.db.host, arch.db.host), 
  db_port = c(ai.red.db.port, arch.db.port))


## Dates For Data Fetch ##
data.config$start.date <- as.Date('2020-05-01')
data.config$end.date <- as.Date('2020-10-01')


## Parallel Queries for Data Fetch for Each Date ##
data.config$queries <- list()

i <- 1
jump <- 1
s.date <- data.config$start.date

while(s.date < data.config$end.date) {
  e.date <- ifelse(as_date(s.date + jump) > data.config$end.date, data.config$end.date, s.date + jump)
  print(paste(i, s.date, as_date(e.date)))
  
  data.config$queries[i] <- with(data.config, paste0(select.query, " where calltime >= '", 
                                                     as_date(s.date), "' and calltime < '", as_date(e.date),"';"))
  i <- i + 1
  s.date <- as_date(s.date + jump)
}


data.dictionary.query <- "select column_name,
case 
when column_name = 'Days_Since_Active_PD' then 'numeric'
when data_type in ('varchar', 'text', 'longtext', 'mediumtext') then 'character' 
when data_type in ('int', 'bigint', 'smallint') then 'integer' 
when data_type in ('float', 'double', 'decimal') then 'numeric' 
when data_type = 'datetime' then 'POSIXct' 
when data_type = 'date' then 'date' else 'other' end class,
case when column_name in ('Days_Since_Active_PD','DialCode_Transformed_PD','Dial_code_type_PD','Country_code_PD','Action_PD','CompanyCode_Combined',
'companycode_Carrier','TOS_Combined_PD','Provider_Combined_PD','TOSdesc_combined_GRF','RoutingCode_PD','iso_abbreviation_PD','Locality_GRF','Supplement_GRF',
'BillingID_GRF','AdminDiv_GRF','LEMin_GRF','LEMax_GRF','POS_GRF','OCN_GRF')
then 'TRUE' else 'FALSE' end is_callgroup,
'FALSE' is_preprocessed
from information_schema.columns
where table_name = 'libertymutual.smetable_red_custom';"


data.config$dictionary <- with(data.config, ExtractDataFromMySQLDatabase(db.credentials, conn.name = "ai_red", queries = data.dictionary.query, 
                                                                         data.description = "data dictionary", cores = 1L))
data.config$dictionary[, column_name := tolower(column_name)]
data.config$dictionary[, column_transformation := ifelse(is_callgroup == T & class == 'character', 'index', 'actual')]
data.config$dictionary[, column_name_modeling := ifelse(column_transformation == 'index', paste0(column_name, "_ix"), column_name)]
data.config$dictionary[, smalls_threshold := ifelse(column_transformation == 'index', 0.05, 0)]
data.config$dictionary[is_callgroup == T]

## Data Fetch ##
complete.data <- with(data.config, ExtractDataFromMySQLDatabase(db.credentials, conn.name = "ai_red", queries, data.description = "raw data", cores = 19L))
complete.data <- UpdateDataTypes(complete.data, data.config$dictionary)


## Indexing ##
data.config$mapping <- IndexData(complete.data, data.config$dictionary)
PrintSummary(complete.data, "SMEData")


## Number of Agents and Calls ##
data.config$calls.count <- complete.data[, .N]
data.config$agents.count <- complete.data[, length(unique(agentid))]


## Callgroup Tree ##
rm(list = setdiff(ls(), c("complete.data", "data.config", lsf.str())))

callgroup.target.metric <- "totalfee"
# data.config$dictionary[column_name_modeling == 'ndc_grf_ix', is_callgroup:=FALSE]
callgroup.data.variables <- c(data.config$dictionary[is_callgroup == TRUE, column_name_modeling], callgroup.target.metric)
print(callgroup.data.variables)
callgroup.training.data <- complete.data[, ..callgroup.data.variables]

callgroup.tree <- rpart(formula = paste(callgroup.target.metric, " ~ ."), data = callgroup.training.data, 
                        control = rpart.control(minsplit = callgroup.training.data[, as.integer(.N * 0.01)],
                                                cp = 0, maxdepth = 10, xval = 5, maxsurrogate = 0, maxcompete = 0), method = "anova")
rsq.rpart(callgroup.tree)


## Variable Importance before Pruning ##
print(callgroup.tree$variable.importance)


## Pruning ##
min.cp <- callgroup.tree$cptable[which.min(callgroup.tree$cptable[,"xerror"]), "CP"]
callgroup.tree <- prune(callgroup.tree, cp = min.cp)
rsq.rpart(callgroup.tree)

## Variable Importance after Pruning ##
print(callgroup.tree$variable.importance)


############################################################################################################
## ORIGINAL DATASET  FOR SCORING ##

## Loading all Port Data Important Variables ##
data.config$select.phquery <- paste0("SELECT `Calldate_PD`,
    `Days_Since_Active_PD`,
                                     `DialCode_Transformed_PD`,
                                     `DialCode_PD`,
                                     `Dial_code_type_PD`,
                                     `Country_code_PD`,
                                     `PortDateTime_PD`,
                                     `Action_PD`,
                                     `CompanyCode_PD`,
                                     `CompanyCode_Combined`,
                                     `companycode_Carrier`,
                                     `TypeOfService_PD`,
                                     `TOS_Combined_PD`,
                                     `TOSdesc_combined_GRF`,
                                     `Provider_Combined_PD`,
                                     `RoutingCode_PD`,
                                     `iso_abbreviation_PD`,
                                     `modernized_provider_PD`,
                                     `startdate_PD`,
                                     `enddate_PD`,
                                     `Block_GRF`,
                                     `TOS_GRF`,
                                     `TOSdesc_GRF`,
                                     `NDC_GRF`,
                                     `Locality_GRF`,
                                     `Provider_GRF`,
                                     `UTC_GRF`,
                                     `Supplement_GRF`,
                                     `BillingID_GRF`,
                                     `AdminDiv_GRF`,
                                     `Effective_Date_GRF`,
                                     `LEMin_GRF`,
                                     `LEMax_GRF`,
                                     `POS_GRF`,
                                     `OCN_GRF`
                                     FROM `libertymutualsatmap`.`portdata_grf`
                                     ")

## Dates For Data Fetch ##
data.config$start.dt <- as.Date('2020-09-15')
data.config$end.dt <- as.Date('2020-10-01')

## Parallel Queries for Data Fetch for date range ##
data.config$phqueries <- list()

i <- 1
jump <- 1
ph.s.date <- data.config$start.dt

while(ph.s.date < data.config$end.dt) {
  ph.e.date <- ifelse(as_date(ph.s.date + jump) > data.config$end.dt, data.config$end.dt, ph.s.date + jump)
  print(paste(i, ph.s.date, as_date(ph.e.date)))
  
  data.config$phqueries[i] <- with(data.config, paste0(select.phquery, " where calldate_pd >= '", 
                                                     as_date(ph.s.date), "' and calldate_pd < '", as_date(ph.e.date),"';"))
  i <- i + 1
  ph.s.date <- as_date(ph.s.date + jump)
}


## Creating the Data Dictionary for Complete Dataset ##
data.phdictionary.query <- "select column_name,
case 
when column_name = 'Days_Since_Active_PD' then 'numeric'
when data_type in ('varchar', 'text', 'longtext', 'mediumtext') then 'character' 
when data_type in ('int', 'bigint', 'smallint') then 'integer' 
when data_type in ('float', 'double', 'decimal') then 'numeric' 
when data_type = 'datetime' then 'POSIXct' 
when data_type = 'date' then 'date' else 'other' end class,
case when column_name in ('Days_Since_Active_PD','DialCode_Transformed_PD','Dial_code_type_PD','Country_code_PD','Action_PD','CompanyCode_Combined',
                          'companycode_Carrier','TOS_Combined_PD','Provider_Combined_PD','TOSdesc_combined_GRF','RoutingCode_PD','iso_abbreviation_PD','Locality_GRF','Supplement_GRF',
                          'BillingID_GRF','AdminDiv_GRF','LEMin_GRF','LEMax_GRF','POS_GRF','OCN_GRF')
then 'TRUE' else 'FALSE' end is_callgroup,
'FALSE' is_preprocessed
from information_schema.columns
where table_name = 'portdata_grf';"


data.config$phdictionary <- with(data.config, ExtractDataFromMySQLDatabase(db.credentials, conn.name = "arch", queries = data.phdictionary.query, 
                                                                         data.description = "data dictionary", cores = 1L))
data.config$phdictionary[, column_name := tolower(column_name)]
data.config$phdictionary[, column_transformation := ifelse(is_callgroup == T & class == 'character', 'index', 'actual')]
data.config$phdictionary[, column_name_modeling := ifelse(column_transformation == 'index', paste0(column_name, "_ix"), column_name)]


## Data Fetch ##
ph.data <- with(data.config, ExtractDataFromMySQLDatabase(db.credentials, conn.name = "arch", phqueries, data.description = "raw data", cores = 19L))

str(ph.data) #View the data types

ph.data <- UpdateDataTypes(ph.data,data.config$dictionary)
str(ph.data) #View the updated data types


## Indexing in relation to the old mapping ##
ph.data <- IndexData(ph.data, data.config$phdictionary, data.config$mapping)
PrintSummary(ph.data, "PortData")


## Prediction ##
rpart.predict(callgroup.tree, ph.data)

# Add the predicted columns to the dataset #
ph.data[,predicted_revenue_ph := rpart.predict(callgroup.tree, ph.data)]

# Check whether NA is present in the predicted revenue column #
ph.data[is.na(predicted_revenue_ph), .N] #If smalls is set for original dataset, then we might expect a count for NA

# Normalized value #
ph.data[,predicted_revenue_ph_norm := (predicted_revenue_ph - mean(predicted_revenue_ph))/sd(predicted_revenue_ph)]

# Zscore #
ph.data[,predicted_revenue_ph_01 := (predicted_revenue_ph - min(predicted_revenue_ph))/(max(predicted_revenue_ph)-min(predicted_revenue_ph))]

# Score rounded off to an integer #
ph.data[,predicted_revenue_ph_100 := as.integer(round(predicted_revenue_ph_01 * 100))]
ph.data

# Final view of the score grouped by #
View(ph.data[,.N,predicted_revenue_ph_100])


############################################################################################################
## Store the Dataset into a Table ##

# Query for the new table with additional ranked columns #
create_table_query <- paste0("CREATE TABLE `portdata_grf_score` (
                             `Calldate_PD` varchar(10) NOT NULL DEFAULT '',
                             `Days_Since_Active_PD` varchar(7) NOT NULL DEFAULT '',
                             `DialCode_Transformed_PD` varchar(15) NOT NULL DEFAULT '',
                             `DialCode_PD` varchar(15) NOT NULL DEFAULT '',
                             `Dial_code_type_PD` varchar(25) NOT NULL DEFAULT '',
                             `Country_code_PD` varchar(30) NOT NULL DEFAULT '',
                             `PortDateTime_PD` varchar(19) NOT NULL DEFAULT '',
                             `Action_PD` varchar(3) NOT NULL DEFAULT '',
                             `CompanyCode_PD` varchar(7) NOT NULL DEFAULT '',
                             `CompanyCode_Combined` varchar(10) NOT NULL DEFAULT '',
                             `companycode_Carrier` varchar(18) NOT NULL DEFAULT '',
                             `TypeOfService_PD` varchar(3) NOT NULL DEFAULT '',
                             `TOS_Combined_PD` varchar(3) NOT NULL DEFAULT '',
                             `TOSdesc_combined_GRF` varchar(100) NOT NULL DEFAULT '',
                             `Provider_Combined_PD` varchar(300) NOT NULL DEFAULT '',
                             `RoutingCode_PD` varchar(15) NOT NULL DEFAULT '',
                             `iso_abbreviation_PD` varchar(10) NOT NULL DEFAULT '',
                             `modernized_provider_PD` varchar(20) NOT NULL DEFAULT '',
                             `startdate_PD` varchar(10) NOT NULL DEFAULT '',
                             `enddate_PD` varchar(10) NOT NULL DEFAULT '',
                             `Block_GRF` varchar(10) NOT NULL DEFAULT '',
                             `TOS_GRF` varchar(2) NOT NULL DEFAULT '',
                             `TOSdesc_GRF` varchar(100) NOT NULL DEFAULT '',
                             `NDC_GRF` varchar(10) NOT NULL DEFAULT '',
                             `Locality_GRF` varchar(300) NOT NULL DEFAULT '',
                             `Provider_GRF` varchar(300) NOT NULL DEFAULT '',
                             `UTC_GRF` varchar(10) NOT NULL DEFAULT '',
                             `Supplement_GRF` varchar(100) NOT NULL DEFAULT '',
                             `BillingID_GRF` varchar(10) NOT NULL DEFAULT '',
                             `AdminDiv_GRF` varchar(2) NOT NULL DEFAULT '',
                             `Effective_Date_GRF` varchar(10) NOT NULL DEFAULT '',
                             `LEMin_GRF` varchar(10) NOT NULL DEFAULT '',
                             `LEMax_GRF` varchar(10) NOT NULL DEFAULT '',
                             `POS_GRF` varchar(50) NOT NULL DEFAULT '',
                             `OCN_GRF` varchar(4) NOT NULL DEFAULT '',
                             `predicted_revenue_ph` float Default 0.0,
                             `predicted_revenue_ph_norm` float Default 0.0,
                             `predicted_revenue_ph_01` int Default 0,
                             `predicted_revenue_ph_100` int Default 0,
                              PRIMARY KEY (`DialCode_PD`,`Calldate_PD`),
                              KEY `idx_block_grf` (`Block_GRF`),
                              KEY `idx_calldate` (`Calldate_PD`)
                              )")

# DB Connection #
db.conn <- dbConnect(MySQL(), 
                  user =  data.config$db.credentials[conn_name == 'arch']$user, 
                  password =  data.config$db.credentials[conn_name == 'arch']$password, 
                  host =  data.config$db.credentials[conn_name == 'arch']$db_host, 
                  port =  data.config$db.credentials[conn_name == 'arch']$db_port,
                  dbname = data.config$db.credentials[conn_name == 'arch']$db_name)

## Table Creation ##
dbSendQuery(db.conn, create_table_query)
dbDisconnect(db.conn)


## Fetch all the non-indexed columns from the final dataframe ##
data.final <- subset(ph.data, select = !(colnames(ph.data) %like% "_ix")) #select all columns except _ix


## Put in the MySQL Table name and chunk size for insertion ##
mysql.tablename <- "portdata_grf_score"
chunk <- 100000


## Chunking ##
df.list <- list()
index <- 1
start <- 0
data.count <- nrow(data.final)
while(index < ((data.count/chunk)+1) ){
  end <- ifelse( (data.count - start) >= chunk, start + chunk, (data.count) )
  df.list[[index]] = data.frame(data.final[(start+1):end,])
  index <- index + 1
  start <- start + chunk
}
message(paste0("Total number of Data frames: ", length(df.list)))


## Function for Parallel Insertion ##
InsertDataToMySQLDatabase <- function(db.credentials, conn.name, list, tablename, cores = 10L){
  registerDoParallel(cores = cores)
  data <- foreach(i = 1:length(list), .inorder = F, .multicombine = T, .verbose = T) %dopar% {
                    db.conn <- dbConnect(MySQL(), user = db.credentials[conn_name == conn.name, user], password = db.credentials[conn_name == conn.name, password],
                                         dbname = db.credentials[conn_name == conn.name, db_name], host = db.credentials[conn_name == conn.name, db_host],
                                         port = db.credentials[conn_name == conn.name, db_port])
                    dbWriteTable(db.conn, name = tablename, value = df.list[[i]], overwrite = FALSE, row.names = FALSE, append = TRUE)
                    dbDisconnect(db.conn)
                  }
}


## Insertion into MySql Table ##
start_time = Sys.time()
with(data.config, InsertDataToMySQLDatabase(db.credentials, conn.name = "arch", df.list, mysql.tablename, cores = 19L))
end_time = Sys.time()
end_time - start_time
