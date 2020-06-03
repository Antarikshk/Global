library(xlsx)
library(mailR)
#library (syuzhet)
library(plyr)
library(dplyr)
library(tidyr)
#library(lazyeval)
library(DBI)
library(RPostgreSQL)
library(xtable)
library(reshape2)
library(rJava)
library(formattable)
library(htmlTable)
library(RSQLite)
library(googlesheets)
library(lubridate)
library('data.table')
drv<-dbDriver("PostgreSQL")
library("RPresto")
library(cellranger)
library(botor)

con <- dbConnect(  RPresto::Presto(),  host='presto.oyorooms.io',  port=8889, user='revenue.analytics@oyorooms.com',schema='task_service',catalog='hive') #create connection

som <- function(x) {
  as.Date(format(x, "%Y-%m-01"))}

dm2=Sys.Date()-2
dm1="2020-05-01"

dm1
dm2


Restriction <- dbGetQuery(con,paste0("SELECT h.oyo_id,h.d as date,restriction FROM aggregatedb.hotel_date_summary h left join aggregatedb.hotels_summary h2 on h2.hotel_id = h.hid
                                     WHERE  h.agreement_type in (0,1,2,5,6,7) and (h.agreement_business_model not in (2,6) or h.agreement_business_model is null)
                                     and h.country_id in (36) and h.name not LIKE '%test%' and h.name not LIKE '%Test%' and h.name not LIKE '%training%' and h.name not LIKE '%Training%' AND d between date '",dm1,"' and date '",dm2,"' "))

write.csv(Restriction,"RestBrazil.csv")

s3_upload_file("RestBrazil.csv", "s3://prod-datapl-r-scheduler/team/global_ops/Brazil/RestBrazil.csv")



paste0("Updated at ", Sys.time() + 5.5*60*60)

library(reticulate)

repl_python()

import pandas as pd
import pygsheets

a = pd.read_csv("s3://prod-datapl-r-scheduler/team/global_ops/Brazil/RestBrazil.csv",index_col=0)
gc_cred_file= downloadFileFromS3("prod-datapl-r-scheduler", "team/global_ops/credentials/antariksh.khanna/client_secret.json")
gc = pygsheets.authorize(service_account_file=gc_cred_file)
sh = gc.open_by_key('1yN1WEfhWlo-FOxSL1Q2W1RDBniPsOc2hLPilwJMqlOc')
wks1 = sh.worksheet_by_title("New")
wks1.clear()
wks1.set_dataframe(a, (1, 1), copy_head = True)

exit
