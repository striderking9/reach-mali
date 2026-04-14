

library(cleaningtools)
library(tidyverse)
library(readxl)
library(addindicators)
library(analysistools)

rm(list = ls())
gc()
cat("\014")

clean_data <- read_excel("data/05_clean_data/msna25_clean_final.xlsx")

## creation de la variable tranche d'age chef de ménage et tranche d'age du repondant

clean_data <- clean_data %>%
  mutate(
    tranche_age_chef_menage = case_when(
      hoh_age_final >= 18 & hoh_age_final <= 59 ~ "tranche_18_59",
      hoh_age_final >= 60  ~ "tranche_60_et_plus"
    ),
    tranche_age_repondant = case_when(
      resp_age >= 18 & resp_age <= 59 ~ "tranche_18_59",
      resp_age >= 60  ~ "tranche_60_et_plus"
    )
  )


### Chargement des loop

roster <- read_excel("data/05_clean_data/loop_data/loop_roster_clean.xlsx")

education <- read_excel("data/05_clean_data/loop_data/loop_education_clean.xlsx")

sante <- read_excel("data/05_clean_data/loop_data/loop_sante_clean.xlsx")

nutrition <- read_excel("data/05_clean_data/loop_data/loop_nutrition_clean.xlsx")

eha <- read_excel("data/05_clean_data/loop_data/loop_eha_clean.xlsx")


### listes des variables de la data_clean à ajouter aux loop

liste_var <- clean_data %>%
  select(
    uuid,
    admin1,
    admin1_new_name,
    admin2,
    strate_id,
    hoh_gender_final,
    tranche_age_chef_menage,
    tranche_age_repondant,
    resp_gender,
    pop_group,
    weights
  )

## Ajout des variables dans chaque loop

#roster

roster <- roster %>%
  left_join(liste_var, by = "uuid")

#education
education <- education %>%
  left_join(liste_var, by = "uuid")

#sante

sante <- sante %>%
  left_join(liste_var, by = "uuid")

#nutrition

nutrition <- nutrition %>%
  left_join(liste_var, by = "uuid")

# eha

eha <- eha %>%
  left_join(liste_var, by = "uuid")


## stockage des loop

writexl::write_xlsx(roster,
                    "data/05_clean_data/loop_data/all/loop_roster_clean_all.xlsx")
writexl::write_xlsx(education,
                    "data/05_clean_data/loop_data/all/loop_education_clean_all.xlsx")
writexl::write_xlsx(sante,
                    "data/05_clean_data/loop_data/all/loop_sante_clean_all.xlsx")
writexl::write_xlsx(nutrition,
                    "data/05_clean_data/loop_data/all/loop_nutrition_clean_all.xlsx")
writexl::write_xlsx(eha,
                    "data/05_clean_data/loop_data/all/loop_eha_clean_all.xlsx")
