
library(cleaningtools)
library(tidyverse)
library(readxl)
library(addindicators)
library(analysistools)
library(impactR4PHU)

rm(list = ls())

# specify file paths
path_folder <- "data/"

path_raw_file <- paste0(path_folder, "04_preclean/msna25_preclean.xlsx")

# Lire la base de données principale
col_types_vec <- rep("guess", 801)

## On specifie les numeros des variables qui doivent etre en text pour
## pour eviter que R confond avec le type logical lors de l'importation
col_nums <- c(
  487, 352, 679, 789, 470, 744, 526, 92, 394, 144, 466, 468, 135,
  787, 539, 472, 151, 112, 142, 147
)
col_types_vec[col_nums] <- "text"

# Importation des bases
raw_dataset<-read_excel(path_raw_file, col_types = col_types_vec)

# Load Kobo tool
survey <- read_excel("data/01_docs/kobo/mli_msna_2025.xlsx", 2)

choices <- read_excel("data/01_docs/kobo/mli_msna_2025.xlsx", 3)

# Loading check_list
list_checks_logical <- read_excel("data/01_docs/check_list/mli_check_list_2025.xlsx")


## Numerisation des variables numérique
var_numerique_revenu <- grep(
  "^revenu_source_(?!autre$)",
  names(raw_dataset),
  value = TRUE,
  perl = TRUE
)

raw_dataset <- raw_dataset %>%
  mutate(across(all_of(var_numerique_revenu), as.numeric))

var_numerique_depense_1mois <- grep(
  "^depense_1mois_(?!autre$)",
  names(raw_dataset),
  value = TRUE,
  perl = TRUE
)

raw_dataset <- raw_dataset %>%
  mutate(across(all_of(var_numerique_depense_1mois), as.numeric))

var_numerique_depense_6mois <- grep(
  "^depense_6mois_(?!autre$)",
  names(raw_dataset),
  value = TRUE,
  perl = TRUE
)

raw_dataset <- raw_dataset %>%
  mutate(across(all_of(var_numerique_depense_6mois), as.numeric))

# Creating fields for comments
raw_dataset <- raw_dataset %>%
  mutate("commentaire_charge_terrain" = "")

################## Checks To Perform ##################

other_columns_to_check <- survey %>%
  filter(type == "text") %>%
  filter(name %in% names(raw_dataset)) %>%
  pull(name)

all_check <- raw_dataset %>%
  check_duration(column_to_check = "duration", uuid_column = "uuid") %>%
  check_outliers(uuid_column = "uuid") %>%
  check_duplicate(uuid_column = "uuid") %>%
  check_value(uuid_column = "uuid") %>%
  check_soft_duplicates(
    uuid_column = "uuid",
    kobo_survey = survey,
    sm_separator = "."
  ) %>%
  check_logical_with_list(
    uuid_column = "uuid",
    list_of_check = list_checks_logical,
    check_id_column = "check_id",
    check_to_perform_column = "check_to_perform",
    columns_to_clean_column = "columns_to_clean",
    description_column = "description"
  ) %>%
  check_others(uuid_column = "uuid", columns_to_check = other_columns_to_check) %>%
  check_pii(element_name = "checked_dataset", uuid_column = "uuid")

### Create combining log
combined_log <- create_combined_log(all_check)

### add_info_to_cleaning_log
combined_log <- combined_log %>%
  add_info_to_cleaning_log(
    dataset_uuid_column = "uuid",
    information_to_add = c(
      "enum_id",
      "enum_base",
      "enum_org",
      "today",
      "admin1",
      "admin2",
      "admin3",
      "commentaire_charge_terrain"
    )
  )

# Join label of questions
survey_label <- survey %>%
  select(name, `label::french`)


combined_log$cleaning_log <- combined_log$cleaning_log %>%
  left_join(survey_label, by = c("question" = "name"))

## Reorganizing columns
combined_log$cleaning_log <- combined_log$cleaning_log %>% select(
  uuid,
  enum_id,
  enum_base,
  enum_org,
  today,
  admin1,
  admin2,
  admin3,
  `label::french`,
  question,
  issue,
  check_id,
  check_binding,
  old_value,
  change_type,
  new_value,
  commentaire_charge_terrain
)

## Remove NA in old value
combined_log$cleaning_log <- combined_log$cleaning_log %>%
  filter(old_value != "NA")

### create_xlsx_cleaning_log all (Du 28-06-2024 a aujourd'hui)

create_xlsx_cleaning_log(
  combined_log,
  kobo_survey = survey,
  kobo_choices = choices,
  use_dropdown = T,
  sm_dropdown_type = "logical",
  output_path =  "data/03_log_files/02_cleaning_log/all/msna_cleaning_log.xlsx"
)

cleaning_log_all <- combined_log$cleaning_log

writexl::write_xlsx(cleaning_log_all, "data/03_log_files/02_cleaning_log/all/msna_cleaning_log.xlsx")

################## 
rm(list = ls())
cat("\014")

## Lire les bases du roster individu et mettre ensemble -----------
raw_loop_individu<-read_excel("data/04_pre_clean/pre_clean_loop/loop_roster.xlsx")

## Lire les bases de eha_stockage et mettre ensemble -----------

raw_loop_eha<-read_excel("data/04_pre_clean/pre_clean_loop/loop_eha_stockage.xlsx")

raw_loop_eha <-raw_loop_eha %>%
  rename(parent_uuid=uuid)%>%
  mutate(eha_uuid=paste(parent_uuid,eha_stockage_nb,sep = "-")) 

## Lire les bases du loop sante et mettre ensemble ------------

### lire les bases sante et mettre ensemble
col_types_vec <- rep("guess", 54)
col_types_vec[c(25,45,48,50)] <- "text"

raw_loop_sante<-read_excel("data/04_pre_clean/pre_clean_loop/loop_sante.xlsx",col_types = col_types_vec)

## Lire les bases du loop education et mettre ensemble ------------

### Charger les données preclean de la loop education et les cleaning logs edu
col_types_vec <- rep("guess", 28)
col_types_vec[c(8,10,22)] <- "text"

raw_loop_education<-read_excel("data/04_pre_clean/pre_clean_loop/loop_education.xlsx",col_types = col_types_vec)


## Load check_lists
list_checks_logical_edu<- read_excel("data/01_docs/check_list/mli_check_list_loop_2025.xlsx","lc_edu")
list_checks_logical_sante<- read_excel("data/01_docs/check_list/mli_check_list_loop_2025.xlsx","lc_sante")
list_checks_logical_roster<- read_excel("data/01_docs/check_list/mli_check_list_loop_2025.xlsx","lc_roster")


### Faire le check à partir du checklist education
check_educ<-raw_loop_education%>%
  check_logical_with_list(
    uuid_column = "edu_person_id",
    list_of_check = list_checks_logical_edu,
    check_id_column = "check_id",
    check_to_perform_column = "check_to_perform",
    columns_to_clean_column = "columns_to_clean",
    description_column = "description"
  )

### Generer le cleaning log
combined_log <- create_combined_log(check_educ)

### Enlever les NA dans old_value
combined_log$cleaning_log <- combined_log$cleaning_log %>%
  filter(old_value != "NA")

cleaning_output_path<-paste0("data/03_log_files/04_cleaning_log_loop/educ/msna_educ_cl_",date_verification,".xlsx")

create_xlsx_cleaning_log(combined_log,
                         kobo_survey = survey,
                         kobo_choices = choices,
                         use_dropdown = T,
                         sm_dropdown_type = "logical",
                         output_path = cleaning_output_path)


### Faire le check à partir du checklist sante
check_sante<-raw_loop_sante%>%
  check_logical_with_list(
    uuid_column = "health_person_id",
    list_of_check = list_checks_logical_sante,
    check_id_column = "check_id",
    check_to_perform_column = "check_to_perform",
    columns_to_clean_column = "columns_to_clean",
    description_column = "description"
  )

### Generer le cleaning log
combined_log <- create_combined_log(check_sante)

### Enlever les NA dans old_value
combined_log$cleaning_log <- combined_log$cleaning_log %>%
  filter(old_value != "NA")

### Sortir le cleaning_log de sante pour la date de verification
cleaning_output_path<-paste0("data/03_log_files/04_cleaning_log_loop/sante/msna_sante_cl_",date_verification,".xlsx")

create_xlsx_cleaning_log(combined_log,
                         kobo_survey = survey,
                         kobo_choices = choices,
                         use_dropdown = T,
                         sm_dropdown_type = "logical",
                         output_path = cleaning_output_path)

### Faire le check à partir du checklist roster
check_roster<-raw_loop_individu%>%
  check_logical_with_list(
    uuid_column = "person_id",
    list_of_check = list_checks_logical_roster,
    check_id_column = "check_id",
    check_to_perform_column = "check_to_perform",
    columns_to_clean_column = "columns_to_clean",
    description_column = "description"
  )

### Generer le cleaning log
combined_log <- create_combined_log(check_roster)

### Enlever les NA dans old_value
combined_log$cleaning_log <- combined_log$cleaning_log %>%
  filter(old_value != "NA")

### Sortir le cleaning_log de Roster pour la date de verification
cleaning_output_path<-paste0("data/03_log_files/04_cleaning_log_loop/roster/msna_roster_cl_",date_verification,".xlsx")

create_xlsx_cleaning_log(combined_log,
                         kobo_survey = survey,
                         kobo_choices = choices,
                         use_dropdown = T,
                         sm_dropdown_type = "logical",
                         output_path = cleaning_output_path)


### Faire le check à partir du checklist eha
check_eha<-raw_loop_eha%>%
  check_outliers(uuid_column = "eha_uuid",columns_not_to_check = "eha_stockage_nb")

### Generer le cleaning log
combined_log <- create_combined_log(check_eha)

### Enlever les NA dans old_value
combined_log$cleaning_log <- combined_log$cleaning_log %>%
  filter(old_value != "NA")

### Extraire les uuids vérifiés par ce cleaning log
checked_uuid_eha<-combined_log$checked_dataset%>%select(parent_uuid)%>%unique()

### Sortir le cleaning_log de sante pour la date de verification
cleaning_output_path<-paste0("data/03_log_files/04_cleaning_log_loop/eha/msna_eha_cl_",date_verification,".xlsx")

create_xlsx_cleaning_log(combined_log,
                         kobo_survey = survey,
                         kobo_choices = choices,
                         use_dropdown = T,
                         sm_dropdown_type = "logical",
                         output_path = cleaning_output_path)

