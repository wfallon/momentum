library(timeDate)
library(timereg)
library(tidyr)
library(RPostgres)
library(DataCombine)
library(lubridate)
library(RPostgres)
library(RcppRoll)
library(dplyr)


ff_data <- read.csv("data/input_data/F-F_Research_Data_5_Factors_2x3.csv")

#set dates to end of month
ff_data$date <- ym(ff_data$date)
ff_data$date <- 
  ceiling_date(ff_data$date, unit = "month") - days(1)

ff_data <- ff_data %>%
  mutate_at(vars(-date), function(x) x / 100)

#exhibit 1 output 
for (strat in c("mom", "imom", "ffmom")) {
  ret_data <- read.csv(sprintf("data/intermediate_data/ewret_%s.csv", strat))
  ret_data$date <- ymd(ret_data$date)
  tmp <- ret_data[,-(1:2)]
  
  #exhibit 1
  output <- NULL
  output$variables <- c("mean", "std dev", "tstat", "ann sr")
  for (i in names(tmp)) {
    col_mean <- mean(tmp[[i]])
    col_sd <- sd(tmp[[i]])
    col_ttest <- t.test(tmp[[i]], mu = 0, alternative = "two.sided")
    col_tstat <- col_ttest$statistic
    col_sr <- 0
    output[[i]] <- c(col_mean, col_sd, col_tstat, col_sr)
  }
  
  write.csv(output, sprintf("data/output_data/exhibit1_%s.csv", strat))
  
  
  
  #Exhibit 2
  min_date <- min(ret_data$date)
  max_date <- max(ret_data$date)
  
  window_ff_data <- ff_data %>%
    filter(date >= min_date) %>%
    filter(date <= max_date)
  
  model <- lm(ret_data$ls ~ 
                 window_ff_data$Mkt.RF + window_ff_data$SMB +
                 window_ff_data$HML + window_ff_data$RMW + 
                 window_ff_data$CMA)
  
  out <- summary(model)
  output2<- NULL
  output2$variables <- c("estimate", "std err", "t-stat")
  
  col_names <- c("alpha", "MKT", "HML", "SMB", "RMW", "CMA")
  for (i in 1:length(col_names)) {
    estimate <- out$coefficients[i, 1]
    err <- out$coefficients[i, 2]
    tstat <- out$coefficients[i, 3]
    output2[[col_names[i]]] <- c(estimate, err, tstat)
  }
  output2[["adj R2"]] <- c(out$adj.r.squared, "", "")
  
  write.csv(output2, sprintf("data/output_data/exhibit2_%s.csv", strat))
}

#Exhibit 3
sp_data <- read.csv("data/intermediate_data/sp_monthly_avg.csv")
sp_data <- sp_data[, -1]
sp_data$date <- ymd(sp_data$date)


ff_factors_data <- read.csv("data/input_data/F-F_Research_Data_5_Factors_2x3.csv")
ff_factors_data$date <- ym(ff_factors_data$date)
ff_factors_data$date <- 
  ceiling_date(ff_factors_data$date, unit = "month") - days(1)

ff_factors_data <- ff_factors_data %>%
  mutate_at(vars(-date), function(x) x / 100)


ffmom_data <- read.csv("data/intermediate_data/ewret_ffmom.csv")
ffmom_data <- ffmom_data %>%
  select(date, ls)

ffmom_data$date <- ymd(ffmom_data$date)

min_date <- min(ffmom_data$date)
max_date <- max(ffmom_data$date)

sp_data <- sp_data %>%
  filter(date >= min_date & date <= max_date)

ff_factors_data <- ff_factors_data %>%
  filter(date >= min_date & date <= max_date)



model1 <- lm(sp_data$mean ~ ff_factors_data$Mkt.RF +
               ff_factors_data$SMB + ff_factors_data$HML +
               ff_factors_data$RMW + ff_factors_data$CMA)

output3 <- NULL
output3$variables <- c("estimate", "stderr", "tstat")

out <- summary(model1)
col_names <- c("alpha", "MKT", "HML", "SMB", "RMW", "CMA", "F5MOM")
for (i in 1:(length(col_names)-1)) {
  estimate <- out$coefficients[i, 1]
  err <- out$coefficients[i, 2]
  tstat <- out$coefficients[i, 3]
  output3[[col_names[i]]] <- c(estimate, err, tstat)
}
output3[["F5MOM"]] <- c("","", "")
output3[["ADJ R2"]] <- c(out$adj.r.squared, "", "")

write.csv(output3, "data/output_data/exhibit3_model1.csv")



model2 <- lm(sp_data$mean ~
              ff_factors_data$SMB + ff_factors_data$HML +
              ff_factors_data$RMW + ff_factors_data$CMA)

output3 <- NULL
output3$variables <- c("estimate", "stderr", "tstat")

out <- summary(model2)
col_names <- c("alpha", "MKT", "HML", "SMB", "RMW", "CMA", "F5MOM")
for (i in 1:(length(col_names)-1)) {
  if (i == 2) {
    output3[[col_names[i]]] <- c("", "", "")
    next()
  }
  index <- if(i < 2) i else i-1
  estimate <- out$coefficients[index, 1]
  err <- out$coefficients[index, 2]
  tstat <- out$coefficients[index, 3]
  output3[[col_names[i]]] <- c(estimate, err, tstat)
}
output3[["F5MOM"]] <- c("","", "")
output3[["ADJ R2"]] <- c(out$adj.r.squared, "", "")

write.csv(output3, "data/output_data/exhibit3_model2.csv")



model3 <- lm(sp_data$mean ~ ff_factors_data$Mkt.RF +
               ff_factors_data$SMB + ff_factors_data$HML +
               ff_factors_data$RMW + ff_factors_data$CMA + 
               ffmom_data$ls)

output3 <- NULL
output3$variables <- c("estimate", "stderr", "tstat")

out <- summary(model3)
col_names <- c("alpha", "MKT", "HML", "SMB", "RMW", "CMA", "F5MOM")
for (i in 1:(length(col_names))) {
  estimate <- out$coefficients[i, 1]
  err <- out$coefficients[i, 2]
  tstat <- out$coefficients[i, 3]
  output3[[col_names[i]]] <- c(estimate, err, tstat)
}
output3[["ADJ R2"]] <- c(out$adj.r.squared, "", "")

write.csv(output3, "data/output_data/exhibit3_model3.csv")


model4 <- lm(sp_data$mean ~ 
              ff_factors_data$SMB + ff_factors_data$HML +
              ff_factors_data$RMW + ff_factors_data$CMA + 
              ffmom_data$ls)

output3 <- NULL
output3$variables <- c("estimate", "stderr", "tstat")

out <- summary(model4)
col_names <- c("alpha", "MKT", "HML", "SMB", "RMW", "CMA", "F5MOM")
for (i in 1:(length(col_names))) {
  if (i == 2) {
    output3[[col_names[i]]] <- c("", "", "")
    next()
  }
  index <- if(i < 2) i else i-1
  estimate <- out$coefficients[index, 1]
  err <- out$coefficients[index, 2]
  tstat <- out$coefficients[index, 3]
  output3[[col_names[i]]] <- c(estimate, err, tstat)
}
output3[["ADJ R2"]] <- c(out$adj.r.squared, "", "")

write.csv(output3, "data/output_data/exhibit3_model4.csv")





