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

stock_ret_data <- dbFetch(res)
dbClearResult(res)

#set missing return values to 0
stock_ret_data$ret[is.na(stock_ret_data$ret)] <- 0

#set dates to end of month
stock_ret_data$date <- ymd(stock_ret_data$date)
stock_ret_data$date <- 
  ceiling_date(stock_ret_data$date, unit = "month") - days(1)


#VWRETD - "Total Return Value-Weighted Index"
res2 <- dbSendQuery(wrds, "select date, vwretd 
                           from msi
                           where date between '1997-12-31' and '2019-12-31'")
mkt_ret_data <- dbFetch(res2)
dbClearResult(res2)

#set missing return values to 0
mkt_ret_data$vwretd[is.na(mkt_ret_data$vwretd)] <- 0

#set dates to end of month
mkt_ret_data$date <- ymd(mkt_ret_data$date)
mkt_ret_data$date <- 
  ceiling_date(mkt_ret_data$date, unit = "month") - days(1)

#get risk free rates
res3 <- dbSendQuery(wrds, "select dateff, rf 
                           from ff.factors_monthly
                           where date between '1997-12-31' and '2019-12-31'")
risk_free_data <- dbFetch(res3)
dbClearResult(res3)

#set dates to end of month
risk_free_data$dateff <- ymd(risk_free_data$dateff)
risk_free_data$dateff <- 
  ceiling_date(risk_free_data$dateff, unit = "month") - days(1)

risk_free_data <- risk_free_data %>%
  rename(date = dateff)

#Calculate excess returns for market and stock data by subtracting
#risk free rate from return data
mkt_ret_data <- mkt_ret_data %>% 
  inner_join(risk_free_data, by = "date")

mkt_ret_data$excess_vwretd <- mkt_ret_data$vwretd - mkt_ret_data$rf

stock_ret_data <- stock_ret_data %>%
  inner_join(risk_free_data, by = "date")

stock_ret_data$excess_ret <- stock_ret_data$ret - stock_ret_data$rf


#counter used to track progress during running of regressions
counter <- 0
find_residuals_by_permno <- function(window_df) {
  counter <<- counter + 1
  print(counter)
  
  #min date of running of regressions
  #need at least 12 months of data so start from 1 year from min date
  min_date <<- min(window_df$date) %m+% years(1)
  max_date <<- max(window_df$date)
  
  #skip if fewer than 12 months of data
  if (min_date > max_date) {
    return(window_df)
  }
  
  total_months <<- interval(min_date, max_date)
  total_months <<- total_months %/% months(1)
  for(i in 0:total_months) {
    #curr_date is date we are estimating at
    curr_date <<- min_date %m+% months(i)
    
    #starting_date is start date of regressions window
    starting_date <<- max(min_date %m-% years(1), curr_date %m-% years(3))
    
    #filter stock data and market data
    window_stock_data <<- window_df %>%
      filter(date >= starting_date) %>%
      filter(date < curr_date)
    window_mkt_data <<- mkt_ret_data %>%
      filter(date >= starting_date) %>%
      filter(date < curr_date)
    
    #skip if gap in data 
    #(only applicable for entire universe, data for S&P is continous)
    if(nrow(window_stock_data) != nrow(window_mkt_data)) {
      next
    }
    
    #run regression on market and stock data
    model <<- lm(window_stock_data$excess_ret ~ 
                   window_mkt_data$excess_vwretd)
    
    #alpha is intercept of regression, beta is slope
    alpha <- model$coefficients[[1]]
    beta <- model$coefficients[[2]]
    
    
    excess_stock_ret <<- window_df$excess_ret[window_df$date == curr_date]
    excess_mkt_ret <<- mkt_ret_data$excess_vwretd[mkt_ret_data$date == curr_date]
    
    #calculate epsilon as e = r_i - r_f - a - B(r_m - r_f)
    epsilon <<- excess_stock_ret - alpha - (beta * excess_mkt_ret)
    
    #save epsilon value
    window_df$residual[window_df$date == curr_date] <- epsilon
  }
  return(window_df)
}


#initialize column for residuals
stock_ret_data$residual <- NA

#run above function on each stock
stock_ret_data <- stock_ret_data %>%
  group_by(permno) %>%
  group_modify(~ find_residuals_by_permno(.x))


file_name <- "data/intermediate_data//imom_data.csv"
write.csv(stock_ret_data, file_name)

source("src/momentum_calc.R")
momentum_calc(file_name, 11, 1, 1)

