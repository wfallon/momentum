library(timeDate)
library(timereg)
library(tidyr)
library(RPostgres)
library(DataCombine)
library(lubridate)
library(RPostgres)
library(RcppRoll)
library(dplyr)


#file_name: path to input csv
#J: historial holding period length
#L: lag
#K: holding period length
momentum_calc <- function (file_name, J, L, K) {
  stock_ret_data <- read.csv(file_name)
  stock_ret_data$date <- ymd(stock_ret_data$date)
  
  #calculate product of IMOM value from t-(j+1) to t-2
  stock_ret_data$log_resid <- log(1 + stock_ret_data$residual)
  stock_ret_data <- stock_ret_data %>%
    arrange(permno, date) %>%
    group_by(permno) %>%
    mutate(roll_sum = roll_sum(log_resid, J, align = "right", fill = NA)) %>%
    mutate(roll_sum = lag(roll_sum, L + 1))
  
  stock_ret_data$cumret <- exp(stock_ret_data$roll_sum) - 1
  
  #drop early months
  stock_ret_data <- DropNA(stock_ret_data, Var = "cumret", message = FALSE)
  
  #Form quintiles
  stock_ret_data <- stock_ret_data %>%
    group_by(date) %>%
    mutate(rank = ntile(cumret, 5))
  
  #calculate interval of holding dates
  ranks <- stock_ret_data %>%
    select(permno, date, rank) %>%
    rename (form_date = date)
  ranks$hdate1 <- ceiling_date(ranks$form_date, unit = "month")
  ranks$hdate2 <- 
    ceiling_date(ranks$form_date, unit = "month") %m+% months(K) - days(1)
  
  #form momentum portfolios
  tmp_data <- stock_ret_data %>%
    select(permno, date, ret)
  
  tmp_data <- inner_join(tmp_data, ranks, by = 'permno')
  tmp_data <- filter(tmp_data, date >= hdate1 & date <= hdate2)
  
  returns <- tmp_data %>%
    group_by(date, rank, form_date) %>%
    summarise_at(vars(ret), list(mean = mean))
  
  ewret <- returns %>%
    group_by(date, rank) %>%
    summarise_at(vars(mean), list(mean = mean))
  
  
  
  #summary
  ewret <- pivot_wider(ewret, names_from = rank, values_from = mean)
  print(summary(ewret))
  
  ewret$ls <- ewret$'5' - ewret$'1'
  
  write.csv(ewret, "data/intermediate_data/ewret_ffmom.csv")
  
  #Long-Short Returns
  ewret <- ewret %>%
    select(date, '1', '5') %>%
    rename(winners = '5', losers = '1') %>%
    mutate(long_short = (winners - losers))
  
  #write.csv(ewret, sprintf("Documents/Github/momentum3//J_%d_K_%d_chaves_test_ewret.csv", J, K))
  
  
  #Compute Cumaltive Returns
  ewret2 <- ewret %>%
    transmute('1 + losers' = losers + 1,
              '1 + winners' = winners + 1,
              '1 + ls' = long_short + 1)
  
  
  ewret2$cumret_losers <- cumprod(ewret2$'1 + losers') - 1
  ewret2$cumret_winners <- cumprod(ewret2$'1 + winners') - 1
  ewret2$cumret_ls <- cumprod(ewret2$'1 + ls') - 1
  
  ewret2 <- ewret2 %>%
    select(date, cumret_losers, cumret_winners, cumret_ls)
  
  #write.csv(ewret2, sprintf("Documents/Github/momentum3//J_%d_K_%d_chaves_test_ewret2.csv", J, K))
  
  
  #Portfolio Summary Statistics
  
  mom_mean <- data.frame(winners = mean(ewret$winners), 
                         losers = mean(ewret$losers),
                         long_short = mean(ewret$long_short))
  mom_mean <- pivot_longer(mom_mean, 0:3, names_to = "momr", 
                           values_to = "mean")
  
  winner_test <- t.test(ewret$winners, mu = 0, alternative = "two.sided")
  loser_test <- t.test(ewret$losers, mu = 0, alternative = "two.sided")
  ls_test <- t.test(ewret$long_short, mu = 0, alternative = "two.sided")
  
  mom_mean$t_stat <- c(winner_test$statistic, 
                       loser_test$statistic, 
                       ls_test$statistic)
  mom_mean$p_value <- c(winner_test$p.value, 
                        loser_test$p.value, 
                        ls_test$p.value)
  
  mom_mean
}