


# consolidating the data from aws s3 
# targeted time frame Jan '23 to Mar '24

library(readxl)
library(tidyverse)
library(arrow)


# claims_details ----------------------------------------------------------

# found 16 files from the last upload of '23 i.e., Dec 28 '23 to recent upload April 4 '24

# first importing 15 weekly uploads. got to check the excel sheets manually 
# to check if there are more than 1 sheets

# checked


# even in weekly uploads there is no uniformity 
# apart from JPAL_Claims_Details_2024-03-28_06-00-00.xlsx all the 14 weekly 
# files are alike.

# for these 14 files the best way to import the excel data is to import 
# all columns as characters 



folder_path <- "D:/Work/AWS_S3_Data/raw_data/claims_details/weekly_dd_mm_yy"

excel_files <- list.files(path = folder_path, pattern = "\\.xlsx$", full.names = TRUE)

dfs <- list()

for (file in excel_files) {
  df <- read_excel(file, col_types = "text")
  dfs[[file]] <- df
}

check_unique_var1 <- function(df) {
  unique_values <- unique(df$CASE_ID)
  nrows <- nrow(df)
  
  identical(length(unique_values), nrows)
}

# Apply the function to each dataframe in the list
result <- lapply(dfs, check_unique_var1)

# Output the result
print(result)

# implies each of the 14 dfs have unique case ids, i.e one case id per row 

weekly_df_1 <- do.call(rbind, dfs)

weekly_df_1$date_of_s3_upload <- rownames(weekly_df_1)

# getting the date of s3 upload for each row
weekly_df_1$date_of_s3_upload <- gsub("^.*_Details_(.{10}).*", "\\1", 
                                      weekly_df_1$date_of_s3_upload)


# changing the date vars to date class 

weekly_df_1$CLAIM_INITIATED_DATE <- as.Date(weekly_df_1$CLAIM_INITIATED_DATE, 
                                     format = "%d-%m-%y")

weekly_df_1$CEO_APPROVED_DATE <- as.Date(weekly_df_1$CEO_APPROVED_DATE, 
                                  format = "%d-%m-%y")

weekly_df_1$date_of_s3_upload <- as.Date(weekly_df_1$date_of_s3_upload)


# converting the amount variables to numeric 

weekly_df_1 <- weekly_df_1 |> 
  mutate_at(vars(ends_with("_AMOUNT")), as.numeric)


# this following single weekly upload file has time stamp date variable 
# the 14 files above had date as character in the dd-mm-yy format


weekly_df_2 <- read_excel("D:/Work/AWS_S3_Data/raw_data/claims_details/JPAL_Claims_Details_2024-03-28_06-00-00.xlsx", 
                          col_types = c("text", "text", "text", 
                                        "text", "text", "date", "text", "text", 
                                        "text", "text", "date"))
length(unique(weekly_df_2$CASE_ID))
# so many case IDs so many rows 

weekly_df_2$date_of_s3_upload <- as.Date("2024-04-01")
# manually checked by looking at the aws console last modified date of the file uploaded file 

weekly_df_2$CLAIM_INITIATED_DATE <- as.Date(weekly_df_2$CLAIM_INITIATED_DATE, 
                                            format = "%d-%m-%y")

weekly_df_2$CEO_APPROVED_DATE <- as.Date(weekly_df_2$CEO_APPROVED_DATE, 
                                         format = "%d-%m-%y")

weekly_df_2 <- weekly_df_2 |> 
  mutate_at(vars(ends_with("_AMOUNT")), as.numeric)


# annual file 
# has data in both sheet 1 and 2 

annual_sheet1 <- read_excel("D:/Work/AWS_S3_Data/raw_data/claims_details/annual/Claims_details_01-01-2023_31-12-2023.xlsx", 
                            sheet = "Sheet1",
                            col_types = c("text", "text", "text", 
                                          "text", "text", "date", "text", "text", 
                                          "text", "text", "date")
                            )

annual_sheet2 <- read_excel("D:/Work/AWS_S3_Data/raw_data/claims_details/annual/Claims_details_01-01-2023_31-12-2023.xlsx", 
                            sheet = "Sheet2",
                            col_types = c("text", "text", "text", 
                                          "text", "text", "date", "text", "text", 
                                          "text", "text", "date")
                            )
annual <- rbind(annual_sheet1, annual_sheet2)

length(unique(annual$CASE_ID))
# phew. as many case ids as rows.

annual_sheet1 <- rm
annual_sheet2 <- rm

annual$CLAIM_INITIATED_DATE <- as.Date(annual$CLAIM_INITIATED_DATE, 
                                            format = "%d-%m-%y")

annual$CEO_APPROVED_DATE <- as.Date(annual$CEO_APPROVED_DATE, 
                                         format = "%d-%m-%y")

annual$date_of_s3_upload <- as.Date("2024-03-11")
# manually checked by looking at the aws console last modified date of the file uploaded file 

annual <- annual |> 
  mutate_at(vars(ends_with("_AMOUNT")), as.numeric)


# merging all files to make the master claims details 

master_claims_details <- rbind (annual, weekly_df_1, weekly_df_2)

# removing the duplicates based on entire rows 

master_claims_details_noduplicates <- distinct(master_claims_details)

# obviously nothing gets removed. as in all the files uploaded on s3,
# each case id occurred once and once only


length(unique(master_claims_details_noduplicates$CASE_ID))
# the rows should be unique on case ids 

# keeping the case ids based on the latest upload in the s3 bucket 
# eg: if the same case id is uploaded on 4th Jan and 5th Jan, we keep the 5th Jan entry
# and remove the 4th Jan entry 

master_claims_details_noduplicates <- master_claims_details_noduplicates |> 
  arrange(CASE_ID, desc(date_of_s3_upload))

# Removing duplicates, keeping only the most recent upload_date for each case_id
master_claims_details_noduplicates <- master_claims_details_noduplicates |> 
  distinct(CASE_ID, .keep_all = TRUE)


# the objective of getting a consolidated df from Jan '23 to Mar '24
# that is unique on case id is achieved.

#writing a parquet file 

# write_parquet(master_claims_details_noduplicates[,1:11], 
#              "E:/datasets/cleaned_data/consolidated_parquet/consolidated_claims_details_jan23_mar24.parquet")

# the above file is uploaded in the cryptovault.
# the above file can be used 
    
#### test begin ####
# testing whether we correcly kept the latest uploaded data of the case ids that 
# got repeated 


origina_consol_df <- master_claims_details |>
  group_by(CASE_ID) |>
  mutate(case_id_count = n()) 

origina_consol_df <- origina_consol_df |>
  filter(case_id_count > 1) |>
  select(CASE_ID, case_id_count, date_of_s3_upload) |>
  arrange(desc(case_id_count), CASE_ID, desc(date_of_s3_upload))

test <- left_join(origina_consol_df, master_claims_details_noduplicates,
                  by  = c("CASE_ID", "date_of_s3_upload"))

# checked that we kept the most recent upload of the case ID 


#### test end ####


# checking the quality of the data 
colMeans(is.na(master_claims_details_noduplicates)) * 100

# we have 100% data of CASE_ID, PATIENT_ID, CLAIM_NO, CLAIM_STATUS,
# CLAIM_INITIATED_DATE, CLAIM_HEAD_DEDUCTED_AMOUNT

# highest missing values are found in CEO_APPROVED_AMOUNT (59%),
# CEO_APPROVED_DATE (25%), CLAIM_HEAD_APPROVED_AMOUNT (24%) &
# CPD_APPROVED_AMOUNT (16%)


# all the other columns are as expected.
# things like presence of unexpected characters in numerical values 
# would have been shown as warnings. no bother there.




# treatments details ------------------------------------------------------

# including the annual file there are 16 uploads 
# even among the weekly files, the file uploaded on 1st Apr differs from the rest 
# in the format of the variables.

# I inspected each file to check the format of the data columns, whether there is data 
# in more than 1 sheet. Based on that I devised the following import pattern. 

# following a similar procedure adopted for importing the claims details data.
# hence not commenting extensively.

folder_path <- "D:/Work/AWS_S3_Data/raw_data/treatments_details/weekly_dd_mm_yy"

excel_files <- list.files(path = folder_path, pattern = "\\.xlsx$", full.names = TRUE)

dfs <- list()

for (file in excel_files) {
  df <- read_excel(file, col_types = "text")
  dfs[[file]] <- df
}

#### testing begin ####

JPAL_Treatment_Details_2024_01_18_06_00_28 <- 
  read_excel("D:/Work/AWS_S3_Data/raw_data/treatments_details/weekly_dd_mm_yy/JPAL_Treatment_Details_2024-01-18_06-00-28.xlsx")

# deduplicating #

test <- distinct(JPAL_Treatment_Details_2024_01_18_06_00_28)

# no drop of duplicates 

test <- test |>
  group_by(CASE_ID) |>
  mutate(case_id_count = n(),
         prcdr_code_uniq_count = n_distinct(PROCEDURE_CODE),
         prcdr_name_uniq_count = n_distinct(PROCEDURE_NAME)) |>
  arrange(desc(case_id_count), CASE_ID)

table(test$case_id_count)
table(test$prcdr_code_uniq_count)
table(test$prcdr_name_uniq_count)

# the above 3 lines tell me that the data is unique on case_id and procedures 

#### testing end ####

# checking whether each of the dfs are unique on case id and procedure combination 

check_uniqueness <- function(df) {
  unique_df <- distinct(df, CASE_ID, PROCEDURE_CODE, PROCEDURE_NAME)
  is_unique <- nrow(df) == nrow(unique_df)
  return(is_unique)
}

# Apply the function to each data frame in the list
result <- sapply(dfs, check_uniqueness)

# Output the result
print(result)

# it was TRUE for all 14 files

# implies each of the 14 dfs are unique on case id and procedure_name and/or
# procedure_code 

treatments_weekly_df_1 <- do.call(rbind, dfs)

treatments_weekly_df_1$date_of_s3_upload <- rownames(treatments_weekly_df_1)

# getting the date of s3 upload for each row
treatments_weekly_df_1$date_of_s3_upload <- gsub("^.*_Details_(.{10}).*", "\\1", 
                                                 treatments_weekly_df_1$date_of_s3_upload)


# changing the 6 date vars including the mutated upload date var to date class 

treatment_date_vars <- c("ADMISSION_DATE", "DISCHARGE_DATE", "CS_DEATH_DT",
                         "PREAUTH_APRV_REJ_DATE", "CLAIM_SUBMITTED_DATE")

treatments_weekly_df_1 <- treatments_weekly_df_1 %>%
  mutate_at(vars(treatment_date_vars), ~as.Date(., format = "%d-%m-%y"))


treatments_weekly_df_1$date_of_s3_upload <- as.Date(treatments_weekly_df_1$date_of_s3_upload)


# converting the lone amount variables to numeric 
treatments_weekly_df_1$PROCEDURE_AMOUNT <- as.numeric(treatments_weekly_df_1$PROCEDURE_AMOUNT)

sum(is.na(treatments_weekly_df_1$ADMISSION_DATE))

sum(is.na(treatments_weekly_df_1$PROCEDURE_AMOUNT))

# the above two lines gives a quick QC and assurance of the data quality 


# this following single weekly upload file has time stamp date variable 
# the 14 files above had date as character in the dd-mm-yy format


treatments_weekly_df_2 <- read_excel("D:/Work/AWS_S3_Data/raw_data/treatments_details/JPAL_Treatment_Details_2024-03-28_06-00-55.xlsx")


# Check for duplicates based on the combination of CASE_ID, PROCEDURE_CODE, PROCEDURE_NAME
is_unique <- !duplicated(treatments_weekly_df_2[c("CASE_ID", "PROCEDURE_CODE", "PROCEDURE_NAME")]) & !duplicated(treatments_weekly_df_2[c("CASE_ID", "PROCEDURE_CODE", "PROCEDURE_NAME")], fromLast = TRUE)

# Check if dataframe is unique on the combination of CASE_ID, PROCEDURE_CODE, PROCEDURE_NAME
if(all(is_unique)) {
  print("Dataframe is unique on CASE_ID, PROCEDURE_CODE, PROCEDURE_NAME")
} else {
  print("Dataframe contains duplicates on CASE_ID, PROCEDURE_CODE, PROCEDURE_NAME")
}


treatments_weekly_df_2$date_of_s3_upload <- as.Date("2024-04-01")
# manually checked by looking at the aws console last modified date of the file uploaded file 

treatments_weekly_df_2 <- treatments_weekly_df_2 %>%
  mutate_at(vars(treatment_date_vars), ~as.Date(., format = "%d-%m-%y"))

# converting the lone amount variables to numeric 
treatments_weekly_df_2$PROCEDURE_AMOUNT <- as.numeric(treatments_weekly_df_2$PROCEDURE_AMOUNT)

sum(is.na(treatments_weekly_df_2$ADMISSION_DATE))
sum(is.na(treatments_weekly_df_2$PROCEDURE_AMOUNT))


# annual file 
# has data in both sheet 1 and 2 

treatments_annual_sheet1 <- read_excel("D:/Work/AWS_S3_Data/raw_data/treatments_details/annual/Treatment_Details_01-01-2023_31-12-2023.xlsx", 
                                       sheet = "Sheet 1")

treatments_annual_sheet2 <- read_excel("D:/Work/AWS_S3_Data/raw_data/treatments_details/annual/Treatment_Details_01-01-2023_31-12-2023.xlsx", 
                                       sheet = "Sheet2")


treatments_annual <- rbind(treatments_annual_sheet1, treatments_annual_sheet2)

# Check for duplicates based on the combination of CASE_ID, PROCEDURE_CODE, PROCEDURE_NAME
is_unique <- !duplicated(treatments_annual[c("CASE_ID", "PROCEDURE_CODE", "PROCEDURE_NAME")]) & !duplicated(treatments_annual[c("CASE_ID", "PROCEDURE_CODE", "PROCEDURE_NAME")], fromLast = TRUE)

# Check if dataframe is unique on the combination of CASE_ID, PROCEDURE_CODE, PROCEDURE_NAME
if(all(is_unique)) {
  print("Dataframe is unique on CASE_ID, PROCEDURE_CODE, PROCEDURE_NAME")
} else {
  print("Dataframe contains duplicates on CASE_ID, PROCEDURE_CODE, PROCEDURE_NAME")
}

# tells that duplication is not a concern in the annual file 

treatments_annual_sheet1 <- rm
treatments_annual_sheet2 <- rm

treatments_annual$date_of_s3_upload <- as.Date("2024-03-11")
# manually checked by looking at the aws console last modified date of the file uploaded file 

treatments_annual <- treatments_annual %>%
  mutate_at(vars(treatment_date_vars), ~as.Date(., format = "%d-%m-%y"))


sum(is.na(treatments_annual$ADMISSION_DATE))
sum(is.na(treatments_annual$PROCEDURE_AMOUNT))


# merging all files to make the master treatments details 

master_treatments_details <- rbind (treatments_annual, 
                                    treatments_weekly_df_1, 
                                    treatments_weekly_df_2
                                    )

# removing the duplicates based on entire rows 

master_treatments_details_noduplicates <- distinct(master_treatments_details)

# obviously nothing gets removed.

# finding the no of unique case and procedure combinations in the data 
# doesn't matter whether we unique on case id and procedure code/name or all the 3
# checked this 

master_treatments_details_noduplicates$uniq_comb <- 
  paste(
    master_treatments_details_noduplicates$CASE_ID,
    master_treatments_details_noduplicates$PROCEDURE_CODE,
    master_treatments_details_noduplicates$PROCEDURE_NAME,
    sep = ""
  )

length(unique(master_treatments_details_noduplicates$uniq_comb))
# 2163288
# the deduplicated df should have these many rows  

# keeping the case ids and procedure combination based on the latest date of upload in the s3 bucket 


master_treatments_details_noduplicates <- master_treatments_details_noduplicates |> 
  arrange(uniq_comb, desc(date_of_s3_upload))

# Removing duplicates, keeping only the most recent upload_date for each uniq_comb
master_treatments_details_noduplicates <- master_treatments_details_noduplicates |> 
  distinct(uniq_comb, .keep_all = TRUE)


#### testing begin ####

test <- master_treatments_details_noduplicates |>
  group_by(CASE_ID) |>
  mutate(case_id_count = n(),
         prcdr_code_uniq_count = n_distinct(PROCEDURE_CODE),
         prcdr_name_uniq_count = n_distinct(PROCEDURE_NAME)) |>
  arrange(desc(case_id_count), CASE_ID)

table(test$case_id_count)
table(test$prcdr_code_uniq_count)
table(test$prcdr_name_uniq_count)

# the 3 lines tells that df is as expected.
# it is unique on case id and procedure code or name

#### testing end ####


length(unique(master_treatments_details_noduplicates$CLAIM_SUBMITTED_DATE))


# the objective of getting a consolidated treatments df from Jan '23 to Mar '24
# that is unique on case and procedure code or name id is achieved.
# the dates are claims submitted date 


#writing a parquet file 

# write_parquet(master_treatments_details_noduplicates[,1:22], 
#              "E:/datasets/cleaned_data/consolidated_parquet/consolidated_treatments_details_jan23_mar24.parquet")

# the above file is uploaded in the cryptovault.



# patient details  --------------------------------------------------------

folder_path <- "D:/Work/AWS_S3_Data/raw_data/patient_details"

excel_files <- list.files(path = folder_path, pattern = "\\.xlsx$", full.names = TRUE)

dfs <- list()

for (file in excel_files) {
  df <- read_excel(file, col_types = "text")
  dfs[[file]] <- df
}

#### testing begin ####

JPAL_Patient_Details_2024_01_18_06_02_08 <- 
  read_excel("D:/Work/AWS_S3_Data/raw_data/patient_details/JPAL_Patient_Details_2024-01-18_06-02-08.xlsx")

# deduplicating #

test <- distinct(JPAL_Patient_Details_2024_01_18_06_02_08)

# no drop of duplicates 

length(unique(test$CASE_NUMBER))

# this matched with nrow of df, looks like this df is unique on case number.

test <- test |>
  group_by(CASE_NUMBER) |>
  mutate(case_id_count = n())

table(test$case_id_count)

# this confirms that this df is unique on case number  

#### testing end ####

# checking whether each of the dfs are unique on case number  

# Function to check uniqueness based on case_number
check_uniqueness <- function(df) {
  n_rows <- nrow(df)
  n_unique_var <- length(unique(df$CASE_NUMBER))
  is_unique <- n_rows == n_unique_var
  return(is_unique)
}

result <- sapply(dfs, check_uniqueness)

print(result)

# it was TRUE for all 15 files

# implies each of the 15 dfs are unique on case_number 

patients_weekly_df <- do.call(rbind, dfs)

patients_weekly_df$date_of_s3_upload <- rownames(patients_weekly_df)

# getting the date of s3 upload for each row
patients_weekly_df$date_of_s3_upload <- gsub("^.*_Details_(.{10}).*", "\\1", 
                                             patients_weekly_df$date_of_s3_upload)


# changing the date vars to date class 

patients_weekly_df$CLAIM_SUBMITED_DATE <- as.Date(patients_weekly_df$CLAIM_SUBMITED_DATE, 
                                                  format = "%d-%m-%y")


patients_weekly_df$date_of_s3_upload <- as.Date(patients_weekly_df$date_of_s3_upload)


# converting the age variable to numeric 
patients_weekly_df$PATIENT_AGE <- as.numeric(patients_weekly_df$PATIENT_AGE)

sum(is.na(patients_weekly_df$CLAIM_SUBMITED_DATE))

sum(is.na(patients_weekly_df$PATIENT_AGE))

# the above two lines gives a quick QC and assurance of the data quality 


length(unique(patients_weekly_df$RESIDENT_ID))
length(unique(patients_weekly_df$CASE_NUMBER))

# gives a sense of re admissions. just checking. 


# annual file 
# has data in both sheet 1 and 2 

patients_annual_sheet1 <- 
  read_excel("D:/Work/AWS_S3_Data/raw_data/patient_details/annual/Patient_Details_01-01-2023_31-12-2023.xlsx", 
              sheet = "Sheet 1 ")

patients_annual_sheet2 <- 
  read_excel("D:/Work/AWS_S3_Data/raw_data/patient_details/annual/Patient_Details_01-01-2023_31-12-2023.xlsx", 
             sheet = "Sheet 2")

patients_annual <- rbind(patients_annual_sheet1, patients_annual_sheet2)

# Check for duplicates based on CASE_ID

length(unique(patients_annual$CASE_NUMBER))

# the annual file is unique on case_number 

patients_annual_sheet1 <- rm
patients_annual_sheet2 <- rm

patients_annual$date_of_s3_upload <- as.Date("2024-03-11")
# manually checked by looking at the aws console last modified date of the file uploaded file 

#converting calim)submission_date to date class 

patients_annual$CLAIM_SUBMITED_DATE <- as.Date(patients_annual$CLAIM_SUBMITED_DATE, 
                                               format = "%d-%m-%y")

patients_annual$PATIENT_MOBILE_NUMBER <- as.character(patients_annual$PATIENT_MOBILE_NUMBER)

# converting the mobile number to character to match with weekly df. in there the var is coded as chr.
# just keeping it uniform

sum(is.na(patients_annual$CLAIM_SUBMITED_DATE))


# merging all files to make the master patients details 

master_patients_details <- rbind (patients_annual, 
                                    patients_weekly_df
                                    )

# removing the duplicates based on entire rows 

master_patients_details_noduplicates <- distinct(master_patients_details)

# obviously nothing gets removed.

# finding the no of unique case_number 

length(unique(master_patients_details$CASE_NUMBER))
# 2015773
# the deduplicated df should have these many rows  

# keeping the case_numbers based on the latest date of upload in the s3 bucket 


master_patients_details_noduplicates <- master_patients_details_noduplicates |> 
  arrange(CASE_NUMBER, desc(date_of_s3_upload))

# Removing duplicates, keeping only the most recent upload_date for each uniq_comb
master_patients_details_noduplicates <- master_patients_details_noduplicates |> 
  distinct(CASE_NUMBER, .keep_all = TRUE)


length(unique(master_patients_details_noduplicates$CASE_NUMBER))


# the objective of getting a consolidated patients df from Jan '23 to Mar '24
# that is unique on case_number
# the dates are claims submitted date 


#writing a parquet file 

#write_parquet(master_patients_details_noduplicates[,1:25], 
#             "E:/datasets/cleaned_data/consolidated_parquet/consolidated_patients_details_jan23_mar24.parquet")





