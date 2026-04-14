
library(tidyverse)
library(lubridate)
library(readxl)

rm(list = ls())

# Chemin des dossiers de travail ------------------------
path_folder <- "data/"

path_raw_file <- paste0(path_folder, "02_raw_data/msna25_raw.xlsx")

path_deletion_logfile <- paste0(path_folder,
                                "data/03_log_files/01_journal_suppression/deletion_log.csv")

## Lecture de la base principale brute ----------------------------------
data_raw <- read_excel(path_raw_file)

## Add missing_count variable to check for incomplete surveys

data_raw <- data_raw %>%
  mutate(missing_count = rowSums(is.na(data_raw)))


## Creation de deletion log ---------------------------------

data_deletion <- data_raw %>%
  mutate(
    start = ymd_hms(start),
    end = ymd_hms(end),
    today = ymd(today),
    duration = difftime(end, start, units = 'mins'),
    missing_count = rowSums(is.na(.))
  ) %>%
  filter(
    consent == "non" |
      duration < 30 |
      start < ymd("2025-07-21") | stratification == "pdi_hors_site" &
      dis_forced == "non" | missing_count >= 500
  ) %>%
  mutate(
    reason_deletion = case_when(
      consent == "non" ~ "Pas de consentement",
      duration < 30 ~ "La durée de l'enquête est inférieure à 30 minutes",
      start < ymd("2025-07-21") ~ "L'enquête est antérieure au 21 juillet 2025",
      stratification == "pdi_hors_site" &
        dis_forced == "non" ~ "Le menage se situe sur un site pdi mais déclare n'avoir jamais été déplacé",
      missing_count >= 500 ~ "Cette enquête est incomplète",
    )
  ) %>%
  select(
    uuid,
    today,
    enum_id,
    admin1,
    admin2,
    admin3,
    enum_org,
    enum_base,
    cluster_id,
    cluster_id_backup,
    admin4,
    admin4_text,
    today,
    start,
    end,
    reason_deletion
  )

### Export csv des deletions logs
if (file.exists("data/03_log_files/01_journal_suppression/deletion_log.csv") == TRUE) {
  write_excel_csv(
    data_deletion,
    "data/03_log_files/01_journal_suppression/deletion_log.csv",
    append = TRUE
  )
  
} else{
  write_excel_csv(
    data_deletion,
    "data/03_log_files/01_journal_suppression/deletion_log.csv",
    append = FALSE
  )
}

# Export Excel des deletions logs
writexl::write_xlsx(data_deletion,
                    "data/03_log_files/01_journal_suppression/deletion_log.xlsx")



# Créer les donnees pre-cleaned ------------------------------------------------

## Récuperer les enquetes à supprimer
log_deletion_ids <- read_xlsx("data/03_log_files/01_journal_suppression/deletion_log.xlsx") %>%
  filter(action_to_perform == "remove_survey") %>%
  pull(`_uuid`)

## Creer la base pre clean en enlevant les enquetes à supprimer
data_pre_clean <- data_raw %>%
  filter(!uuid %in% log_deletion_ids) %>%
  mutate(
    start = ymd_hms(start),
    end = ymd_hms(end),
    duration = as.numeric(difftime(end, start, units = 'mins')),
    date_ymd = ymd(today),
    date_dmy = format(ymd(date_ymd), "%d.%m.%Y")
  )

### check time
quantile(data_pre_clean$duration)

### check age criteria
quantile(data_pre_clean$enum_age, na.rm = T)

### check consent
data_pre_clean %>%
  count(consent)

## Exporter la base preclean destinée pour le nettoyage
writexl::write_xlsx(data_pre_clean, "data/04_preclean/msna25_preclean.xlsx")


## Creer la base pre clean de la loop Roster ------------------------

### Lecture de la loop roster brute
raw_loop_individu<-read_excel(path_raw_file,"roster")

### Recuperer et enregistrer les uuid des individus supprimés
deletion<-raw_loop_individu%>%
  filter(uuid %in% log_deletion_ids)%>%
  select(person_id)

write_xlsx(deletion, "data/03_log_files/01_journal_suppression/deletion_log_loop_roster_step1.xlsx")

### Generer le preclean du roster et exporter en excel
preclean_loop_individu<-raw_loop_individu%>%filter(!uuid %in% log_deletion_ids)

writexl::write_xlsx(preclean_loop_individu, "data/04_preclean/msna25_preclean_roster.xlsx")

## Creer la base pre clean de la loop EHA ------------------------
### Meme processus pour tous les loop sauf que pour EHA on a cree un identifiant unique pour les 
## recipient

raw_loop_eha<-read_excel(path_raw_file,"eha_stockage")

loop_eha_stockage<-raw_loop_eha%>%mutate(eha_uuid=paste0(uuid,"-",eha_stockage_nb))

deletion<-loop_eha_stockage%>%
  filter(uuid %in% log_deletion_ids)%>%
  select(eha_uuid)

write_xlsx(deletion, "data/03_log_files/01_journal_suppression/deletion_log_loop_eha_step1.xlsx")

preclean_loop_eha<-raw_loop_eha%>%filter(!uuid %in% log_deletion_ids)

writexl::write_xlsx(preclean_loop_eha, "data/04_preclean/msna25_preclean_eha.xlsx")

## Creer la base pre clean de la loop Sante ------------------------

raw_loop_sante<-read_excel(path_raw_file,"health_ind")

deletion<-raw_loop_sante%>%
  filter(uuid %in% log_deletion_ids)%>%
  select(health_person_id)

write_xlsx(deletion, "data/03_log_files/01_journal_suppression/deletion_log_loop_sante_step1.xlsx")

preclean_loop_sante<-raw_loop_sante%>%filter(!uuid %in% log_deletion_ids)

writexl::write_xlsx(preclean_loop_sante, "data/04_preclean/msna25_preclean_sante.xlsx")

## Creer la base pre clean de la loop Education ------------------------

raw_loop_edu<-read_excel(path_raw_file,"edu_ind")

deletion<-raw_loop_edu%>%
  filter(uuid %in% log_deletion_ids)%>%
  select(edu_person_id)

write_xlsx(deletion, "data/03_log_files/01_journal_suppression/deletion_log_loop_edu_step1.xlsx")


preclean_loop_edu<-raw_loop_edu%>%filter(!uuid %in% log_deletion_ids)

writexl::write_xlsx(preclean_loop_edu, "data/04_preclean/msna25_preclean_education.xlsx")

## Creer la base pre clean de la loop Nutrition ------------------------

raw_loop_nut<-read_excel(path_raw_file,"nut_ind")

deletion<-raw_loop_nut%>%
  filter(uuid %in% log_deletion_ids)%>%
  select(nut_person_id)

write_xlsx(deletion, "data/03_log_files/01_journal_suppression/deletion_log_loop_nut_step1.xlsx")

preclean_loop_nut<-raw_loop_nut%>%filter(!uuid %in% log_deletion_ids)

writexl::write_xlsx(preclean_loop_nut, "data/04_preclean/msna25_preclean_nut.xlsx")
