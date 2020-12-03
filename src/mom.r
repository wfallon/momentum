library(timeDate)
library(timereg)
library(tidyr)
library(RPostgres)
library(DataCombine)
library(lubridate)
library(RPostgres)
library(RcppRoll)
library(dplyr)

#get monthly returns for S&P 500 constituents 
res <- dbSendQuery(wrds, "select a.permno, a.date, a.ret,
                   b.start, b.ending
                   from crsp.msf as a
                   inner join crsp.msp500list as b
                   on a.permno = b.permno
                   and a.date >= b.start
                   and a.date <= b.ending
                   where a.date between '1997-12-31' and '2019-12-31'
                   order by a.permno")

data <- dbFetch(res)
dbClearResult(res)

#set missing return values to 0
data$ret[is.na(data$ret)] <- 0

#set dates to end of month
data$date <- ymd(data$date)
data$date <- ceiling_date(data$date, unit = "month") - days(1)

data$residual <- data$ret

file_name <- "data/intermediate_data//mom_data.csv"
write.csv(data, file_name)

source("src/momentum_calc.R")
momentum_calc(file_name, 11, 1, 1)
