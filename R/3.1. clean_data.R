

library(tidyverse)
library(readxl)
library(tidyr)
library(dplyr)
library(lubridate)
library(impactR4PHU)
library(cleaningtools)
library(fs)
library(stringr)
library(writexl)

rm(list = ls())

####################################################

# Fonction pour lire les données et nettoyer les noms des colonnes
read_and_clean <- function(file_path, sheet = 1) {
  data <- read_excel(file_path, sheet = sheet)
  # Nettoyage des noms de colonnes
  names(data) <- gsub(pattern = "^_", replacement = "", names(data))
  names(data) <- gsub(pattern = "/", replacement = ".", names(data))
  return(data)
}

####################################################

# Charger les données principales à nettoyer
data_to_clean <- read_and_clean("data/04_preclean/msna25_preclean.xlsx")


####################################################
# Charger l'outil Kobo
survey <- read_excel("data/01_docs/kobo/mli_msna_2025.xlsx", 2)

choices <- read_excel("data/01_docs/kobo/mli_msna_2025.xlsx", 3)

all_combined_log <- read_excel("data/03_log_files/03_retour_ct/all/all_combined/msna25_cleaning_log_all.xlsx")


####################################################
# Executer les checks

check_log_results <- review_cleaning_log(
  raw_dataset = data_to_clean,
  raw_data_uuid_column = "uuid",
  cleaning_log = all_combined_log,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column = "question",
  cleaning_log_new_value_column = "new_value",
  cleaning_log_change_type_column = "change_type",
  change_response_value = "change_response"
)

my_clean_data <- create_clean_data(
  raw_dataset = data_to_clean,
  raw_data_uuid_column = "uuid",
  cleaning_log = all_combined_log,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column = "question",
  cleaning_log_new_value_column = "new_value",
  cleaning_log_change_type_column = "change_type"
)

my_clean_data2 <- recreate_parent_column(
  dataset = my_clean_data,
  uuid_column = "uuid",
  kobo_survey = survey,
  kobo_choices = choices,
  sm_separator = ".",
  cleaning_log_to_append = all_combined_log
)


clean_data <- my_clean_data2[["data_with_fix_concat"]]

##############################################
# Executer les checks supplementaires

supplement_check <- read_excel("data/03_log_files/05_cleaning_log_supplementaire/cleaning_log.xlsx")

uuid_a_supprimer_dans_les_cl <- setdiff(supplement_check$uuid, clean_data$uuid)
uuid_a_supprimer_dans_les_cl

supplement_check <- supplement_check %>%
  mutate(new_value = ifelse(change_type == "no_action", old_value, new_value))

write_xlsx(
  supplement_check,
  "data/03_log_files/05_cleaning_log_supplementaire/cleaning_log_supplementaire.xlsx"
)

check_log_results <- review_cleaning_log(
  raw_dataset = clean_data,
  raw_data_uuid_column = "uuid",
  cleaning_log = supplement_check,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column = "question",
  cleaning_log_new_value_column = "new_value",
  cleaning_log_change_type_column = "change_type",
  change_response_value = "change_response"
)

my_clean_data <- create_clean_data(
  raw_dataset = clean_data,
  raw_data_uuid_column = "uuid",
  cleaning_log = supplement_check,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column = "question",
  cleaning_log_new_value_column = "new_value",
  cleaning_log_change_type_column = "change_type"
)

my_clean_data2 <- recreate_parent_column(
  dataset = my_clean_data,
  uuid_column = "uuid",
  kobo_survey = survey,
  kobo_choices = choices,
  sm_separator = ".",
  cleaning_log_to_append = supplement_check
)

clean_data <- my_clean_data2[["data_with_fix_concat"]]


##############################################
# Exporter la base principale MSNA 2025 nettoyées

writexl::write_xlsx(clean_data, "data/05_clean_data/msna25_clean.xlsx")

##############################################
rm(list = ls())

## Recalculer les variables calculées et ajouter d'autres variables

# Fonction pour lire les données et nettoyer les noms des colonnes
read_and_clean <- function(file_path, sheet = 1) {
  data <- read_excel(file_path, sheet = sheet)
  # Nettoyage des noms de colonnes
  names(data) <- gsub(pattern = "^_", replacement = "", names(data))
  names(data) <- gsub(pattern = "/", replacement = ".", names(data))
  return(data)
}

# Charger les données principales
clean_data <- read_and_clean("data/05_clean_data/msna25_clean.xlsx")

# Recalculate Data and adding variables

# recalculate "income_sum" variable
clean_data <- clean_data %>%
  mutate(income_sum = rowSums(
    select
    (
      .,
      cm_income_source_salaried_n,
      cm_income_source_casual_n,
      cm_income_source_own_business_n,
      cm_income_source_own_production_n,
      cm_income_source_social_benefits_n,
      cm_income_source_rent_n,
      cm_income_source_remittances_n,
      cm_income_source_assistance_n,
      cm_income_source_support_friends_n,
      cm_income_source_donation_n,
      cm_income_source_other_n
    ),
    na.rm = T
  ))

##############################################

# recalculate "expenditure_frequent_sum" variable

clean_data <- clean_data %>%
  mutate(across(
    c(
      cm_expenditure_frequent_food,
      cm_expenditure_frequent_rent,
      cm_expenditure_frequent_water,
      cm_expenditure_frequent_nfi,
      cm_expenditure_frequent_utilities,
      cm_expenditure_frequent_fuel,
      cm_expenditure_frequent_transportation,
      cm_expenditure_frequent_communication,
      cm_expenditure_frequent_other
    ),
    ~ as.numeric(.)
  ))

clean_data <- clean_data %>%
  mutate(expenditure_frequent_sum = rowSums(
    select(
      .,
      cm_expenditure_frequent_food,
      cm_expenditure_frequent_rent,
      cm_expenditure_frequent_water,
      cm_expenditure_frequent_nfi,
      cm_expenditure_frequent_utilities,
      cm_expenditure_frequent_fuel,
      cm_expenditure_frequent_transportation,
      cm_expenditure_frequent_communication,
      cm_expenditure_frequent_other
    ),
    na.rm = T
  ))


################################################
# Reformat hhs entries

clean_data <- clean_data %>%
  mutate(
    fsl_hhs_alldaynight = ifelse(
      fsl_hhs_alldaynight == "nsp" |
        fsl_hhs_alldaynight == "pnpr",
      "non",
      fsl_hhs_alldaynight
    ),
    fsl_hhs_nofoodhh = ifelse(
      fsl_hhs_nofoodhh == "nsp" |
        fsl_hhs_nofoodhh == "pnpr",
      "non",
      fsl_hhs_nofoodhh
    ),
    fsl_hhs_sleephungry = ifelse(
      fsl_hhs_sleephungry == "nsp" |
        fsl_hhs_sleephungry == "pnpr",
      "non",
      fsl_hhs_sleephungry
    )
  )



## Adding "fcs, hhs, rcsi, lcsi" indicators
clean_data <- clean_data %>%
  impactR4PHU::add_fcs(
    cutoffs = "normal",
    fsl_fcs_cereal = "fsl_fcs_cereal",
    fsl_fcs_legumes = "fsl_fcs_legumes",
    fsl_fcs_veg = "fsl_fcs_veg",
    fsl_fcs_fruit = "fsl_fcs_fruit",
    fsl_fcs_meat = "fsl_fcs_meat",
    fsl_fcs_dairy = "fsl_fcs_dairy",
    fsl_fcs_sugar = "fsl_fcs_sugar",
    fsl_fcs_oil = "fsl_fcs_oil"
  ) %>%
  impactR4PHU::add_hhs(
    fsl_hhs_nofoodhh = "fsl_hhs_nofoodhh",
    fsl_hhs_nofoodhh_freq = "fsl_hhs_nofoodhh_freq",
    fsl_hhs_sleephungry = "fsl_hhs_sleephungry",
    fsl_hhs_sleephungry_freq = "fsl_hhs_sleephungry_freq",
    fsl_hhs_alldaynight = "fsl_hhs_alldaynight",
    fsl_hhs_alldaynight_freq = "fsl_hhs_alldaynight_freq",
    yes_answer = "oui",
    no_answer = "non",
    rarely_answer = "rarement",
    sometimes_answer = "parfois",
    often_answer = "souvent"
  ) %>%
  impactR4PHU::add_rcsi(
    fsl_rcsi_lessquality = "fsl_rcsi_lessquality",
    fsl_rcsi_borrow = "fsl_rcsi_borrow",
    fsl_rcsi_mealsize = "fsl_rcsi_mealsize",
    fsl_rcsi_mealadult = "fsl_rcsi_mealadult",
    fsl_rcsi_mealnb = "fsl_rcsi_mealnb"
  ) %>%
  impactR4PHU::add_lcsi(
    fsl_lcsi_stress1 = "fsl_lcsi_stress1",
    fsl_lcsi_stress2 = "fsl_lcsi_stress2",
    fsl_lcsi_stress3 = "fsl_lcsi_stress3",
    fsl_lcsi_stress4 = "fsl_lcsi_stress4",
    fsl_lcsi_crisis1 = "fsl_lcsi_crisis1",
    fsl_lcsi_crisis2 = "fsl_lcsi_crisis2",
    fsl_lcsi_crisis3 = "fsl_lcsi_crisis3",
    fsl_lcsi_emergency1 = "fsl_lcsi_emergency1",
    fsl_lcsi_emergency2 = "fsl_lcsi_emergency2",
    fsl_lcsi_emergency3 = "fsl_lcsi_emergency3",
    yes_val = "oui",
    no_val = "non_pas_besoin",
    exhausted_val = "non_deja_epuise",
    not_applicable_val = "non_pertinent"
  )

fsl_fcs_indicator <- c(
  "fcs_weight_cereal1",
  "fcs_weight_legume2",
  "fcs_weight_dairy3",
  "fcs_weight_meat4",
  "fcs_weight_veg5",
  "fcs_weight_fruit6",
  "fcs_weight_oil7",
  "fcs_weight_sugar8",
  "fsl_fcs_score",
  "fsl_fcs_cat"
)

fsl_hhs_indicator <- c(
  "fsl_hhs_nofoodhh_recoded",
  "fsl_hhs_nofoodhh_freq_recoded",
  "fsl_hhs_sleephungry_recoded",
  "fsl_hhs_sleephungry_freq_recoded",
  "fsl_hhs_alldaynight_recoded",
  "fsl_hhs_alldaynight_freq_recoded",
  "fsl_hhs_comp1",
  "fsl_hhs_comp2",
  "fsl_hhs_comp3",
  "fsl_hhs_score",
  "fsl_hhs_cat_ipc",
  "fsl_hhs_cat"
)

fsl_rcsi_indicator <- c("fsl_rcsi_score", "fsl_rcsi_cat")

fsl_lcsi_indicator <- c(
  "fsl_lcsi_stress1",
  "fsl_lcsi_stress2",
  "fsl_lcsi_stress3",
  "fsl_lcsi_stress4",
  "fsl_lcsi_crisis1",
  "fsl_lcsi_crisis2",
  "fsl_lcsi_crisis3",
  "fsl_lcsi_emergency1",
  "fsl_lcsi_emergency2",
  "fsl_lcsi_emergency3",
  "fsl_lcsi_stress_yes",
  "fsl_lcsi_stress_exhaust",
  "fsl_lcsi_stress",
  "fsl_lcsi_crisis_yes",
  "fsl_lcsi_crisis_exhaust",
  "fsl_lcsi_crisis",
  "fsl_lcsi_emergency_yes",
  "fsl_lcsi_emergency_exhaust",
  "fsl_lcsi_emergency",
  "fsl_lcsi_cat_yes",
  "fsl_lcsi_cat_exhaust",
  "fsl_lcsi_cat"
)

clean_data <- clean_data %>%
  relocate(fsl_fcs_indicator, .after = fcs_prelim) %>%
  relocate(fsl_hhs_indicator, .after = fsl_hhs_alldaynight_freq) %>%
  relocate(fsl_rcsi_indicator, .after = fsl_rcsi_mealnb) %>%
  relocate(fsl_lcsi_indicator, .after = fsl_lcsi_emergency3) %>%
  select(-fcs_prelim)

#################################################################

## Ajout de la variable admin1_name

clean_data <- clean_data %>%
  mutate(
    admin1_name = case_when(
      admin1 == "ML01" ~ "kayes",
      admin1 == "ML02" ~ "koulikoro",
      admin1 == "ML03" ~ "sikasso",
      admin1 == "ML04" ~ "segou",
      admin1 == "ML05" ~ "mopti",
      admin1  == "ML06" ~ "tombouctou",
      admin1 == "ML07" ~ "gao",
      admin1 == "ML08" ~ "kidal",
      admin1 == "ML09" ~ "bamako",
      admin1 == "ML10" ~ "menaka"
    )
  ) %>%
  relocate(admin1_name, .after = admin1)

## Ajout la variable admin1 du nouveau decoupage

clean_data <- clean_data %>%
  mutate(
    admin1_new = case_when(
      admin2 == "ML0204" | admin2 == "ML0901" ~ "ML00",
      admin2 == "ML0101" |
        admin2 == "ML0103" | admin2 == "ML0104" |
        admin2 == "ML0107" ~ "ML01",
      admin2 == "ML0201" |
        admin2 == "ML0203" |
        admin2 == "ML0204" | admin2 == "ML0205" |
        admin2 == "ML0206" ~ "ML02",
      admin2 == "ML0302" |
        admin2 == "ML0305" ~ "ML03",
      admin2 == "ML0401" |
        admin2 == "ML0402" |
        admin2 == "ML0403" | admin2 == "ML0404" |
        admin2 == "ML0406" ~ "ML04",
      admin2 == "ML0503" |
        admin2 == "ML0506" | admin2 == "ML0507" |
        admin2 == "ML0508" ~ "ML05",
      admin2 == "ML0601" |
        admin2 == "ML0602" |
        admin2 == "ML0603" | admin2 == "ML0604" |
        admin2 == "ML0605" ~ "ML06",
      admin2 == "ML0701" |
        admin2 == "ML0702" | admin2 == "ML0703" ~ "ML07",
      admin2 == "ML0801" |
        admin2 == "ML0802" | admin2 == "ML0803" |
        admin2 == "ML0804" ~ "ML08",
      admin2 == "ML0605" &
        admin3 == "ML060505" ~ "ML09",
      admin2 == "ML1001" |
        admin2 == "ML1002" | admin2 == "ML1003" |
        admin2 == "ML1004" ~ "ML10",
      admin2 == "ML0102" |
        admin2 == "ML0106" ~ "ML11",
      admin2 == "ML0105" ~ "ML12",
      admin2 == "ML0202" ~ "ML13",
      admin2 == "ML0301" |
        admin2 == "ML0303" | admin2 == "ML0306" ~ "ML15",
      admin2 == "ML0304" |
        admin2 == "ML0307" ~ "ML16",
      admin2 == "ML0405" |
        admin2 == "ML0407" ~ "ML17",
      admin2 == "ML0504" ~ "ML18",
      admin2 == "ML0501" |
        admin2 == "ML0502" | admin2 == "ML0505" ~ "ML19"
    ),
    admin1_new_name = case_when(
      admin1_new == "ML00" ~ "Bamako",
      admin1_new == "ML01" ~ "Kayes",
      admin1_new == "ML02" ~ "Koulikoro",
      admin1_new == "ML03" ~ "Sikasso",
      admin1_new == "ML04" ~ "Segou",
      admin1_new == "ML05" ~ "Mopti",
      admin1_new == "ML06" ~ "Tombouctou",
      admin1_new == "ML07" ~ "Gao",
      admin1_new == "ML08" ~ "Kidal",
      admin1_new == "ML09" ~ "Taoudenie",
      admin1_new == "ML10" ~ "Menaka",
      admin1_new == "ML11" ~ "Nioro",
      admin1_new == "ML12" ~ "Kita",
      admin1_new == "ML13" ~ "Dioila",
      admin1_new == "ML15" ~ "Bougouni",
      admin1_new == "ML16" ~ "Koutiala",
      admin1_new == "ML17" ~ "San",
      admin1_new == "ML18" ~ "Douentza",
      admin1_new == "ML19" ~ "Bandiagara"
    )
  ) %>%
  relocate(admin1_new, .after = admin1) %>%
  relocate(admin1_new_name, .after = admin1_new)


## Ajout de la variable admin2_name

clean_data <- clean_data %>%
  mutate(
    admin2_name = case_when(
      admin2 == "ML0101" ~ "bafoulabe",
      admin2 == "ML0102" ~ "diema",
      admin2 == "ML0103" ~ "kayes",
      admin2 == "ML0104" ~ "kenieba",
      admin2 == "ML0105" ~ "kita",
      admin2 == "ML0106" ~ "nioro",
      admin2 == "ML0107" ~ "yelimane",
      admin2 == "ML0201" ~ "banamba",
      admin2 == "ML0202" ~ "dioila",
      admin2 == "ML0203" ~ "kangaba",
      admin2 == "ML0204"  ~ "kati",
      admin2 == "ML0205" ~ "kolokani",
      admin2 == "ML0206" ~ "koulikoro",
      admin2 == "ML0301" ~ "bougouni",
      admin2 == "ML0302" ~ "kadiolo",
      admin2 == "ML0303" ~ "kolondieba",
      admin2 == "ML0304" ~ "koutiala",
      admin2 == "ML0305" ~ "sikasso",
      admin2 == "ML0306" ~ "yanfolila",
      admin2 == "ML0307" ~ "yorosso",
      admin2 == "ML0401" ~ "baroueli",
      admin2 == "ML0402" ~ "bla",
      admin2 == "ML0403" ~ "macina",
      admin2 == "ML0404" ~ "niono",
      admin2 == "ML0405" ~ "san",
      admin2 == "ML0406" ~ "segou",
      admin2 == "ML0407" ~ "tominian",
      admin2 == "ML0501" ~ "bandiagara",
      admin2 == "ML0502" ~ "bankass",
      admin2 == "ML0503" ~ "djenne",
      admin2 == "ML0504" ~ "douentza",
      admin2 == "ML0505" ~ "koro",
      admin2 == "ML0506" ~ "mopti",
      admin2 == "ML0507" ~ "tenenkou",
      admin2 == "ML0508" ~ "youwarou",
      admin2 == "ML0601" ~ "dire",
      admin2 == "ML0602" ~ "goundam",
      admin2 == "ML0603" ~ "gourma_rharous",
      admin2 == "ML0604" ~ "niafunke",
      admin2 == "ML0605"  ~ "tombouctou",
      admin2 == "ML0701" ~ "ansongo",
      admin2 == "ML0702" ~ "bourem",
      admin2 == "ML0703" ~ "gao",
      admin2 == "ML0801" ~ "abeibara",
      admin2 == "ML0802" ~ "kidal",
      admin2 == "ML0803" ~ "tessalit",
      admin2 == "ML0804" ~ "tin_essako",
      admin2 == "ML0901" ~ "bamako",
      admin2 == "ML1001" ~ "menaka"
    )
  ) %>%
  relocate(admin2_name, .after = admin2)

##########################################################################
## statut_deplacement

clean_data <- clean_data %>%
  mutate(
    statut_deplacement = case_when(
      dis_forced %in% c("non", "nsp", "pnpr") ~ "PND",
      dis_forced == "oui_deplace_retourne" &
        dis_area_origin == "qlq_part_mali" &
        dis_area_origin_border_yn == "oui" ~ "RAPATRIE",
      dis_forced == "oui_deplace_retourne" &
        dis_area_origin == "qlq_part_mali" ~ "RETOURNE",
      dis_forced == "oui_deplace" &
        dis_area_origin == "un_autre_pays" ~ "REFUGIE",
      dis_forced == "oui_deplace" &
        dis_area_origin == "qlq_part_mali" ~ "PDI"
    )
  )


# creation de la variable strate_name

clean_data <- clean_data %>%
  mutate(strate_name = admin2_name)


## Creation de la variable population groupe et strate_id

clean_data <- clean_data %>%
  mutate(
    pop_group = case_when(
      dis_forced == "non" ~ "PND",
      dis_forced == "oui_deplace"  &
        dis_area_origin == "qlq_part_mali" ~ "PDI",
      dis_forced == "oui_deplace"  &
        dis_area_origin == "un_autre_pays" ~ "REF",
      dis_forced == "nsp" |
        dis_forced == "pnpr" | is.na(dis_forced)  |
        dis_area_origin == "nsp" |
        dis_area_origin == "pnpr" |
        is.na(dis_area_origin) ~ "dont_know",
      TRUE ~ "PND"
    ),
    strate_id = paste0(strate_name, "_", pop_group)
  ) %>%
  relocate(strate_id, .after = strate_name)


## Recategorisation des ménages sans statut

clean_data <- clean_data %>%
  mutate(
    pop_group = ifelse(
      pop_group == "dont_know" &
        (dis_forced == "nsp" |
           dis_forced == "pnpr") &
        stratification == "com_hote",
      "PND",
      pop_group
    )
  )


clean_data <- clean_data %>%
  mutate(pop_group = ifelse(
    pop_group == "dont_know" &
      (dis_forced == "nsp" |
         dis_forced == "pnpr") &
      stratification == "refugie",
    "REF",
    pop_group
  ))


clean_data <- clean_data %>%
  mutate(pop_group = ifelse(
    pop_group == "dont_know" &
      (dis_forced == "oui_deplace") &
      (
        stratification == "com_hote" |
          stratification == "pdi_site" |
          stratification == "pdi_hors_site"
      ),
    "PDI",
    pop_group
  ))


clean_data <- clean_data %>%
  mutate(
    pop_group = ifelse(
      pop_group == "dont_know" &
        dis_forced == "oui_deplace_retourne" &
        stratification == "com_hote",
      "PND",
      pop_group
    )
  )


clean_data <- clean_data %>%
  mutate(
    pop_group = ifelse(
      pop_group == "dont_know" &
        dis_forced == "oui_deplace" &
        stratification == "refugie",
      "REF",
      pop_group
    )
  )

## Recalcule de la variable strate_id

clean_data <- clean_data %>%
  mutate(strate_id = paste0(strate_name, "_", pop_group))

# Importer les strates de l'echantillonnage
strate <- read_excel("data/sample_msna25.xlsx") %>% pull(strate_id)

# Sortir les strates de la base de données data_clean
strate_id <- clean_data %>% pull(strate_id)

# Chercher les strates qui ne respectent pas l'echantillonnage
strate_id_incorect <- setdiff(strate_id, strate)
strate_id_incorect

data_deletion <- clean_data %>%
  filter(strate_id %in% strate_id_incorect) %>%
  mutate(
    reason_deletion = "L'enquêteur n'a pas respecté la stratification PND, PDI ou REF",
    today = as.Date(today),
    start = as.Date(start),
    end = as.Date(end)
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


write_xlsx(
  data_deletion,
  "data/03_log_files/01_journal_suppression/deletion_log_step2.xlsx"
)

log_deletion_ids <- data_deletion %>%
  pull(uuid)

clean_data <- clean_data %>%
  filter(!uuid %in% log_deletion_ids)

write_excel_csv(clean_data, "data/05_clean_data/msna25_clean_final.csv")
writexl::write_xlsx(clean_data, "data/05_clean_data/msna25_clean_final.xlsx")
