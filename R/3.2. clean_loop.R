

library(dplyr)
library(cleaningtools)
library(readxl)
library(writexl)
library(analysistools)

rm(list = ls())
gc()
cat("\014")

# Chemin pour les cleaning logs des loop
cl_educ_path <- "data/03_log_files/04_cleaning_log_loop/educ/"
cl_sante_path <- "data/03_log_files/04_cleaning_log_loop/sante/"
cl_roster_path <- "data/03_log_files/04_cleaning_log_loop/roster/"
cl_eha_path <- "data/03_log_files/04_cleaning_log_loop/eha/"

## Load Kobo tool
survey <- read_excel("data/01_docs/kobo/mli_msna_2025.xlsx", 2)
choices <- read_excel("data/01_docs/kobo/mli_msna_2025.xlsx", 3)

dl_id <- read_xlsx("data/03_log_files/01_journal_suppression/deletion_log_step2.xlsx") %>%
  pull(uuid)


# Nettoyer la loop education -----------------------------------------

loop_education <- read_excel("data/04_preclean/msna25_preclean_education.xlsx")

deletion <- loop_education %>%
  filter(uuid %in% dl_id) %>%
  select(edu_person_id)

deletion_step1 <- read_excel(
  "data/03_log_files/01_journal_suppression/deletion_log_loop_education_step1.xlsx"
)

deletion <- deletion %>% bind_rows(deletion_step1)

write_xlsx(
  deletion,
  "data/03_log_files/01_journal_suppression/deletion_log_loop_education.xlsx"
)


loop_education <- loop_education %>%
  filter(!uuid %in% dl_id)

### Importer les cleaning logs remplits
cleaning_files <- list.files(path = cl_educ_path,
                             pattern = "^msna_educ_cl.*\\.xlsx$",
                             full.names = TRUE)

cleaning_edu_logs <- lapply(cleaning_files, read_excel, sheet = "cleaning_log") %>%
  bind_rows()

uuid_a_supprimer_dans_les_cl <- setdiff(cleaning_edu_logs$uuid, loop_education$edu_person_id)

cleaning_edu_logs <- cleaning_edu_logs %>% filter(!uuid %in% uuid_a_supprimer_dans_les_cl)

write_xlsx(
  cleaning_edu_logs,
  "data/03_log_files/04_cleaning_log_loop/cleaning_logs_loop_edu.xlsx"
)

### verifier les cleaning et generer les donnees nettoyees
check_log_results <- review_cleaning_log(
  raw_dataset = loop_education,
  raw_data_uuid_column = "edu_person_id",
  cleaning_log = cleaning_edu_logs,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column = "question",
  cleaning_log_new_value_column = "new_value",
  cleaning_log_change_type_column = "change_type",
  change_response_value = "change_response"
)

education_clean <- create_clean_data(
  raw_dataset = loop_education,
  raw_data_uuid_column = "edu_person_id",
  cleaning_log = cleaning_edu_logs,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column = "question",
  cleaning_log_new_value_column = "new_value",
  cleaning_log_change_type_column = "change_type"
)

education_clean2 <- recreate_parent_column(
  dataset = education_clean,
  uuid_column = "edu_person_id",
  kobo_survey = survey,
  kobo_choices = choices,
  sm_separator = ".",
  cleaning_log_to_append = cleaning_edu_logs
)

education_clean <- education_clean2[["data_with_fix_concat"]]



# Nettoyer la loop sante -----------------------------------------

loop_sante <- read_excel("data/04_preclean/msna25_preclean_sante.xlsx")

deletion <- loop_sante %>%
  filter(uuid %in% dl_id) %>%
  select(health_person_id)

deletion_step1 <- read_excel("data/03_log_files/01_journal_suppression/deletion_log_loop_sante_step1.xlsx")

deletion <- deletion %>% bind_rows(deletion_step1)


write_xlsx(
  deletion,
  "data/03_log_files/01_journal_suppression/deletion_log_loop_sante.xlsx"
)

loop_sante <- loop_sante %>%
  filter(!uuid %in% dl_id)

### Importer les cleaning logs remplits
cleaning_files <- list.files(path = cl_sante_path,
                             pattern = "^msna_sante_cl.*\\.xlsx$",
                             full.names = TRUE)

cleaning_sante_logs <- lapply(cleaning_files, read_excel, sheet = "cleaning_log") %>%
  bind_rows()

write_xlsx(
  cleaning_sante_logs,
  "data/03_log_files/04_cleaning_log_loop/cleaning_logs_loop_sante.xlsx"
)

### verifier les cleaning et generer les donnees nettoyees
check_log_results <- review_cleaning_log(
  raw_dataset = loop_sante,
  raw_data_uuid_column = "health_person_id",
  cleaning_log = cleaning_sante_logs,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column = "question",
  cleaning_log_new_value_column = "new_value",
  cleaning_log_change_type_column = "change_type",
  change_response_value = "change_response"
)

sante_clean <- create_clean_data(
  raw_dataset = loop_sante,
  raw_data_uuid_column = "health_person_id",
  cleaning_log = cleaning_sante_logs,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column = "question",
  cleaning_log_new_value_column = "new_value",
  cleaning_log_change_type_column = "change_type"
)

sante_clean2 <- recreate_parent_column(
  dataset = sante_clean,
  uuid_column = "health_person_id",
  kobo_survey = survey,
  kobo_choices = choices,
  sm_separator = ".",
  cleaning_log_to_append = cleaning_sante_logs
)

sante_clean <- sante_clean2[["data_with_fix_concat"]]

# Nettoyer la loop roster individu  -----------------------------------------

loop_individu <- read_excel("data/04_preclean/msna25_preclean_roster.xlsx")

deletion <- loop_individu %>%
  filter(uuid %in% dl_id) %>%
  select(person_id)


deletion_step1 <- read_excel("data/03_log_files/01_journal_suppression/deletion_log_loop_roster_step1.xlsx")

deletion <- deletion %>% bind_rows(deletion_step1)

write_xlsx(
  deletion,
  "data/03_log_files/01_journal_suppression/deletion_log_loop_roster.xlsx"
)

loop_individu <- loop_individu %>%
  filter(!uuid %in% dl_id)

### Importer les cleaning logs remplits
cleaning_files <- list.files(path = cl_roster_path,
                             pattern = "^msna_roster_cl.*\\.xlsx$",
                             full.names = TRUE)

cleaning_roster_logs <- lapply(cleaning_files, read_excel, sheet = "cleaning_log") %>%
  bind_rows()

uuid_a_supprimer_dans_les_cl <- setdiff(cleaning_roster_logs$uuid, loop_individu$person_id)

cleaning_roster_logs <- cleaning_roster_logs %>% filter(!uuid %in% uuid_a_supprimer_dans_les_cl)

write_xlsx(
  cleaning_roster_logs,
  "data/03_log_files/04_cleaning_log_loop/cleaning_logs_loop_roster.xlsx"
)

### verifier les cleaning et generer les donnees nettoyees
check_log_results <- review_cleaning_log(
  raw_dataset = loop_individu,
  raw_data_uuid_column = "person_id",
  cleaning_log = cleaning_roster_logs,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column = "question",
  cleaning_log_new_value_column = "new_value",
  cleaning_log_change_type_column = "change_type",
  change_response_value = "change_response"
)

roster_clean <- create_clean_data(
  raw_dataset = loop_individu,
  raw_data_uuid_column = "person_id",
  cleaning_log = cleaning_roster_logs,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column = "question",
  cleaning_log_new_value_column = "new_value",
  cleaning_log_change_type_column = "change_type"
)

roster_clean2 <- recreate_parent_column(
  dataset = roster_clean,
  uuid_column = "person_id",
  kobo_survey = survey,
  kobo_choices = choices,
  sm_separator = ".",
  cleaning_log_to_append = cleaning_sante_logs
)

roster_clean <- roster_clean2[["data_with_fix_concat"]]

# Nettoyer la loop eha_stockage -----------------------------------------

loop_eha_stockage <- read_excel("data/04_preclean/msna25_preclean_eha.xlsx")

### Importer les cleaning logs remplits
cleaning_files <- list.files(path = cl_eha_path,
                             pattern = "^msna_eha_cl.*\\.xlsx$",
                             full.names = TRUE)

cleaning_eha_logs <- lapply(cleaning_files, read_excel, sheet = "cleaning_log") %>%
  bind_rows()

cleaning_eha_logs_deletion <- cleaning_eha_logs %>%
  filter(change_type == "remove_survey") %>%
  pull(uuid)

loop_eha_stockage <- loop_eha_stockage %>% mutate(eha_uuid = paste0(uuid, "-", eha_stockage_nb))

deletion_1 <- loop_eha_stockage %>%
  filter(uuid %in% dl_id) %>%
  select(eha_uuid)

deletion_2 <- loop_eha_stockage %>%
  filter(eha_uuid %in% cleaning_eha_logs_deletion) %>%
  select(eha_uuid)

deletion_step1 <- read_excel("data/03_log_files/01_journal_suppression/deletion_log_loop_eha_step1.xlsx")

deletion <- deletion_1 %>% bind_rows(deletion_2) %>% bind_rows(deletion_step1)

write_xlsx(deletion,
           "data/03_log_files/01_journal_suppression/deletion_log_loop_eha.xlsx")

cleaning_eha_logs_deletion <- deletion %>% pull(eha_uuid)

loop_eha_stockage <- loop_eha_stockage %>%
  filter(!eha_uuid %in% cleaning_eha_logs_deletion)

uuid_a_supprimer_dans_les_cl <- setdiff(cleaning_eha_logs$uuid, loop_eha_stockage$eha_uuid)
#les cleaning_logs ont été construit avec les bases raw donc il faut enlever les uuid
#des cleaning_logs qui ne figurent plus dans les donnees preclean

cleaning_eha_logs <- cleaning_eha_logs %>% filter(!uuid %in% uuid_a_supprimer_dans_les_cl)

write_xlsx(
  cleaning_eha_logs,
  "data/03_log_files/04_cleaning_log_loop/cleaning_logs_loop_eha.xlsx"
)

### verifier les cleaning et generer les donnees nettoyees
check_log_results <- review_cleaning_log(
  raw_dataset = loop_eha_stockage,
  raw_data_uuid_column = "eha_uuid",
  cleaning_log = cleaning_eha_logs,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column = "question",
  cleaning_log_new_value_column = "new_value",
  cleaning_log_change_type_column = "change_type",
  change_response_value = "change_response"
)

eha_clean <- create_clean_data(
  raw_dataset = loop_eha_stockage,
  raw_data_uuid_column = "eha_uuid",
  cleaning_log = cleaning_eha_logs,
  cleaning_log_uuid_column = "uuid",
  cleaning_log_question_column = "question",
  cleaning_log_new_value_column = "new_value",
  cleaning_log_change_type_column = "change_type"
)


# Nettoyer les variables EHA de la base principale ----------------

stat_eha <- eha_clean %>%
  group_by(uuid) %>%
  summarise(
    bn_recipient = max(eha_stockage_nb),
    capacite_stockage = sum(eha_capacite_stockage)
  ) %>%
  arrange((capacite_stockage))

clean_data <- read_excel("data/05_clean_data/msna25_clean_final.xlsx")

clean_data <- clean_data %>% left_join(stat_eha, join_by("uuid" == "uuid"))


clean_data <- clean_data %>%
  mutate(eha_stockage_volume = capacite_stockage,
         eha_eau_recipient_nb_stockage = bn_recipient) %>%
  select(-capacite_stockage, -bn_recipient)

# Ajouter des variables supplementaires ------------

roster_clean <- roster_clean %>%
  mutate(
    serious_ind_difficulty_sight = ifelse(
      ind_difficulty_sight == "beaucoup" |
        ind_difficulty_sight == "vois_rien_du_tout",
      1,
      0
    ),
    serious_ind_difficulty_hearing = ifelse(
      ind_difficulty_hearing == "beaucoup" |
        ind_difficulty_hearing == "entend_rien_du_tout",
      1,
      0
    ),
    serious_ind_difficulty_walking = ifelse(
      ind_difficulty_walking == "beaucoup" |
        ind_difficulty_walking == "incapable",
      1,
      0
    ),
    serious_ind_difficulty_remembering = ifelse(
      ind_difficulty_remembering == "beaucoup" |
        ind_difficulty_remembering == "incapable",
      1,
      0
    ),
    serious_ind_difficulty_take_care = ifelse(
      ind_difficulty_take_care == "beaucoup" |
        ind_difficulty_take_care == "incapable",
      1,
      0
    ),
    serious_ind_difficulty_communicating = ifelse(
      ind_difficulty_communicating == "beaucoup" |
        ind_difficulty_communicating == "incapable",
      1,
      0
    ),
    situation_handicap = ifelse(
      serious_ind_difficulty_sight == 1 |
        serious_ind_difficulty_hearing == 1 |
        serious_ind_difficulty_walking == 1 |
        serious_ind_difficulty_remembering == 1 |
        serious_ind_difficulty_take_care == 1 |
        serious_ind_difficulty_communicating == 1,
      1,
      0
    )
  )

nb_personne_situation_handicap <- roster_clean %>%
  group_by(uuid) %>%
  summarize(situation_handicap_n = sum(situation_handicap))

## Ajout de la variable situation_handicap_n dans la base clean_data

clean_data <- clean_data %>%
  left_join(nb_personne_situation_handicap, by = "uuid") %>%
  mutate(personne_avec_handicap_dans_menage = ifelse(situation_handicap_n > 0, "oui", "non"))


# Ajout des variables dans la loop Education

education_clean <- education_clean %>%
  filter(edu_ind_age_schooling == 1) %>%
  mutate(
    edu_ind_age = as.numeric(edu_ind_age),
    edu_ind_age_5_18 = ifelse(edu_ind_age >= 5 &
                                edu_ind_age <= 18, 1, 0),
    edu_ind_f_age_5_18 = ifelse(
      edu_ind_age >= 5 &
        edu_ind_age <= 18 & edu_ind_gender == "feminin",
      1,
      0
    ),
    edu_ind_m_age_5_18 = ifelse(
      edu_ind_age >= 5 &
        edu_ind_age <= 18 & edu_ind_gender == "masculin",
      1,
      0
    ),
    edu_ind_age_5_18_access = case_when(
      edu_ind_age_5_18 == 1 & edu_access == "oui" ~ 1,
      edu_ind_age_5_18 == 1 &
        edu_access == "non" ~ 0
    ),
    edu_ind_f_age_5_18_access = case_when(
      edu_ind_age_5_18_access == 1 & edu_ind_gender == "feminin" ~ 1,
      edu_ind_age_5_18_access == 1 &
        edu_ind_gender != "feminin" ~ 0
    ),
    edu_ind_m_age_5_18_access = case_when(
      edu_ind_age_5_18_access == 1 & edu_ind_gender == "masculin" ~ 1,
      edu_ind_age_5_18_access == 1 &
        edu_ind_gender != "masculin" ~ 0
    ),
    edu_disrupted_hazards_5_18 = case_when(
      edu_ind_age_5_18_access == 1 & edu_disrupted_hazards == "oui" ~ 1,
      edu_ind_age_5_18_access ==
        1 & edu_disrupted_hazards == "non" ~ 0
    ),
    edu_non_formel = ifelse(edu_other_yn == "non", 1, 0),
    edu_level = case_when(
      edu_level_grade == "premier_cycle_annee1" |
        edu_level_grade == "premier_cycle_annee2" |
        edu_level_grade == "premier_cycle_annee3" |
        edu_level_grade == "premier_cycle_annee4" |
        edu_level_grade == "premier_cycle_annee5" ~ "primaire",
      edu_level_grade == "deuxieme_cycle_annee6" |
        edu_level_grade == "deuxieme_cycle_annee7" |
        edu_level_grade == "deuxieme_cycle_annee8" |
        edu_level_grade == "deuxieme_cycle_annee9" |
        edu_level_grade == "lycee_annee10" |
        edu_level_grade == "lycee_annee11" |
        edu_level_grade == "lycee_annee12" ~ "secondaire",
      edu_level_grade == "maternelle" ~ "maternelle",
      edu_level_grade == "autre" ~ "autre"
    ),
    enfant_primaire = case_when(edu_level == "primaire" ~ 1, edu_level !=
                                  "primaire" ~ 0),
    fille_primaire = case_when(
      enfant_primaire == 1 & edu_ind_gender == "feminin" ~ 1,
      enfant_primaire == 1 &
        edu_ind_gender != "feminin" ~ 0
    ),
    garcon_primaire = case_when(
      enfant_primaire == 1 & edu_ind_gender == "masculin" ~ 1,
      enfant_primaire == 1 &
        edu_ind_gender != "masculin" ~ 0
    ),
    enfant_secondaire = case_when(edu_level == "secondaire" ~ 1, edu_level !=
                                    "secondaire" ~ 0),
    fille_secondaire = case_when(
      enfant_secondaire == 1 & edu_ind_gender == "feminin" ~ 1,
      enfant_secondaire == 1 &
        edu_ind_gender != "feminin" ~ 0
    ),
    garcon_secondaire = case_when(
      enfant_secondaire == 1 & edu_ind_gender == "masculin" ~ 1,
      enfant_secondaire == 1 &
        edu_ind_gender != "masculin" ~ 0
    )
  )

education_additionnel <- education_clean %>%
  group_by(uuid) %>%
  summarize(
    edu_ind_age_5_18_n = sum(edu_ind_age_5_18),
    edu_ind_f_age_5_18_n = sum(edu_ind_f_age_5_18),
    edu_ind_m_age_5_18_n = sum(edu_ind_m_age_5_18),
    edu_ind_age_5_18_access_n = sum(edu_ind_age_5_18_access),
    edu_ind_f_age_5_18_access_n = sum(edu_ind_f_age_5_18_access),
    edu_ind_m_age_5_18_access_n = sum(edu_ind_m_age_5_18_access),
    edu_disrupted_hazards_5_18_n = sum(edu_disrupted_hazards_5_18),
    edu_non_formel_n = sum(edu_non_formel),
    enfant_primaire_n = sum(enfant_primaire),
    fille_primaire_n = sum(fille_primaire),
    garcon_primaire_n = sum(garcon_primaire),
    enfant_secondaire_n = sum(enfant_secondaire),
    fille_secondaire_n = sum(fille_secondaire),
    garcon_secondaire_n = sum(garcon_secondaire)
  )

clean_data <- clean_data %>%
  left_join(education_additionnel, by = "uuid")


sante_clean <- sante_clean %>%
  mutate(
    besoin_soin = ifelse(health_ind_healthcare_needed == "oui", 1, 0),
    soin_recu = ifelse(health_ind_healthcare_received == "oui", 1, 0)
  )

sante_additionnel <- sante_clean %>%
  group_by(uuid) %>%
  summarize(besoin_soin_n = sum(besoin_soin),
            soin_recu_n = sum(soin_recu))

clean_data <- clean_data %>%
  left_join(sante_additionnel, by = "uuid")


# Ajout de la variable mdd (minimum dietary diversity for children)

nutrition <- read_excel("data/04_preclean/msna25_preclean_nut.xlsx")

deletion <- nutrition %>%
  filter(uuid %in% dl_id) %>%
  select(nut_person_id)

deletion_step1 <- read_excel("data/03_log_files/01_journal_suppression/deletion_log_loop_nut_step1.xlsx")

deletion <- deletion %>% bind_rows(deletion_step1)

write_xlsx(
  deletion,
  "data/03_log_files/01_journal_suppression/deletion_log_loop_nutrition.xlsx"
)

nutrition <- nutrition %>%
  mutate(
    mdd = rowSums(
      select(
        .,
        nut_ind_food.cereales,
        nut_ind_food.racines_tubercules,
        nut_ind_food.legumineuses_noix,
        nut_ind_food.produits_laitiers,
        nut_ind_food.aliment_chair,
        nut_ind_food.oeuf,
        nut_ind_food.fruits_legumes,
        nut_ind_food.autres_fuits_legumes
      )
    ),
    mdd = ifelse(nut_ind_age >= 0 & nut_ind_age < 2, mdd, NA),
    mdd_score_above5 = ifelse(mdd >= 5, "oui", "non")
  )

writexl::write_xlsx(roster_clean,
                    "data/05_clean_data/loop_data/loop_roster_clean.xlsx")
writexl::write_xlsx(education_clean,
                    "data/05_clean_data/loop_data/loop_education_clean.xlsx")
writexl::write_xlsx(sante_clean,
                    "data/05_clean_data/loop_data/loop_sante_clean.xlsx")
writexl::write_xlsx(nutrition,
                    "data/05_clean_data/loop_data/loop_nutrition_clean.xlsx")
writexl::write_xlsx(eha_clean,
                    "data/05_clean_data/loop_data/loop_eha_clean.xlsx")
writexl::write_xlsx(clean_data, "data/05_clean_data/msna25_clean_final.xlsx")

################################################################################
# Ajout d'autres indicateurs composites
################################################################################
rm(list = ls())

clean_data <- read_excel("data/05_clean_data/msna25_clean_final.xlsx")

#education

clean_data <- clean_data %>%
  mutate(
    presence_enfant_edu_5_18 = ifelse(edu_ind_age_5_18_n >= 1, "oui", "non"),
    presence_file_edu_5_18 = ifelse(edu_ind_f_age_5_18_n >= 1, "oui", "non"),
    presence_garcon_edu_5_18 = ifelse(edu_ind_m_age_5_18_n >= 1, "oui", "non"),
    presence_enfant_access_edu_5_18 = ifelse(edu_ind_age_5_18_access_n >= 1, "oui", "non"),
    presence_fille_access_edu_5_18 = ifelse(edu_ind_f_age_5_18_access_n >= 1, "oui", "non"),
    presence_garcon_access_edu_5_18 = ifelse(edu_ind_m_age_5_18_access_n >= 1, "oui", "non"),
    presence_enfant_edu_disrupted_hazards_5_18 = ifelse(edu_disrupted_hazards_5_18_n >=
                                                          1, "oui", "non"),
    presence_enfant_edu_non_formel = ifelse(edu_non_formel_n >= 1, "oui", "non"),
    presence_enfant_primaire = ifelse(enfant_primaire_n >= 1, "oui", "non"),
    presence_fille_primaire = ifelse(fille_primaire_n >= 1, "oui", "non"),
    presence_garcon_primaire = ifelse(garcon_primaire_n >= 1, "oui", "non"),
    presence_enfant_secondaire = ifelse(enfant_secondaire_n >= 1, "oui", "non"),
    presence_fille_secondaire = ifelse(fille_secondaire_n >= 1, "oui", "non"),
    presence_garcon_secondaire = ifelse(garcon_secondaire_n >= 1, "oui", "non")
  )

# sante

clean_data <- clean_data %>%
  mutate(
    presence_besoin_soin = ifelse(besoin_soin_n >= 1, "oui", "non"),
    presence_soin_recu = ifelse(soin_recu_n >= 1, "oui", "non")
  )

## Ajout de la variable weight (ponderation)

## Importation de la base populationnelle par stratification

sample <- read_excel("data/sample_msna25.xlsx")

strate_not_indataset <- setdiff(sample$strate_id, clean_data$strate_id)
strate_not_indataset


sample <- sample %>% filter(!strate_id %in% strate_not_indataset)

echantillon_final <- clean_data %>%
  group_by(strate_id) %>%
  summarise(enquete = n())

echantillon_final <- echantillon_final %>%
  left_join(sample, join_by("strate_id" == "strate_id"))

echantillon_final <- echantillon_final %>%
  mutate(
    seuil = ceiling(echantillon * 0.95),
    constat = ifelse(enquete < seuil, "sous_seuil", "representatif")
  )
echantillon_final

writexl::write_xlsx(echantillon_final, "data/msna25_echantillon_final.xlsx")

clean_data <- clean_data %>%
  add_weights(
    sample,
    strata_column_dataset = "strate_id",
    strata_column_sample = "strate_id",
    population_column = "population_menage"
  )



writexl::write_xlsx(clean_data, "data/05_clean_data/msna25_clean_final.xlsx")
