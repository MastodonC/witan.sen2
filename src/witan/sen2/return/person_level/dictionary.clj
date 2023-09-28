(ns witan.sen2.return.person-level.dictionary
  "Dictionary of lookups for SEN2 person-level return items.

  from [gov.uk](https://www.gov.uk/guidance/special-educational-needs-survey) unless stated otherwise.

  See in particular:
  - [Special educational needs survey: guide to submitting data](https://www.gov.uk/guidance/special-educational-needs-survey)
  - [Special educational needs person level survey 2023: guide](https://www.gov.uk/government/publications/special-educational-needs-person-level-survey-2023-guide)
  - [Special educational needs person level survey: technical specification](https://www.gov.uk/government/publications/special-educational-needs-person-level-survey-technical-specification)")



;;; # Utility functions
(defn- compare-mapped-keys
  [m k1 k2]
  (compare [(get m k1) k1]
           [(get m k2) k2]))



;;; # SEN setting - Establishment type <SENsetting>, per items 4.7c & 5.5c of the DfE 2023 SEN2 return.
(def sen-setting->order
  "Map SEN setting abbreviations to order"
  (let [m (zipmap ["OLA" "OPA" "EHE" "EYP" "NEET" "NIEC" "NIEO"] (iterate inc 1))]
    (into (sorted-map-by (partial compare-mapped-keys m)) m)))

(def sen-settings
  "SEN setting abbreviations"
  (apply sorted-set-by
         (partial compare-mapped-keys sen-setting->order)
         (keys sen-setting->order)))

(def sen-setting->label
  "Map SEN setting abbreviation to label for display"
  (into (sorted-map-by (partial compare-mapped-keys sen-setting->order))
        {"OLA" "Other LA Arrangements (inc. EOTAS)"
         "OPA" "Other Parent/Person Arrangements (exc. EHE)"
         "EHE" "Elective Home Education"
         "EYP" "Early Years Provider"
         "NEET" "Not in Education, Training or Employment"
         "NIEC" "Ceasing"
         "NIEO" "Other"}))

(def sen-setting->description
  "Map SEN setting abbreviation to description"
  (into (sorted-map-by (partial compare-mapped-keys sen-setting->order))
        {"OLA"  (str "Other – arrangements made by the local authority "
                     "in accordance with section 61 of the Children and Families Act 2014, "
                     "(\"education otherwise than at a school or post-16 institution etc\").")
         "OPA"  (str "Other – alternative arrangements made by parents or young person "
                     "in accordance with section 42(5) of the Children and Families Act 2014, "
                     "excluding those who are subject to elective home education.")
         "EHE"  (str "Elective home education – alternative arrangements made by parents or young person "
                     "in accordance with section 42(5) of the Children and Families Act 2014, for elective home education.")
         "EYP"  (str "Early years provider with no GIAS URN "
                     "(for example private nursery, independent early years providers and childminders).")
         "NEET" (str "Not in education, training or employment (aged 16-25).")
         "NIEC" (str "Not in education or training – Notice to cease issued.")
         "NIEO" (str "Not in education – Other – "
                     "Where this is used, the local authority will be prompted for further information in COLLECT, "
                     "for example, transferred into the local authority with an EHC plan and awaiting placement.")}))



;;; # EHCP needs <SENtype>, per item 5.6 of the DfE 2023 SEN2 return.
(def sen-type->order
  "Map SENtype (EHCP need) abbreviations to order"
  (let [m (zipmap ["SPLD" "MLD" "SLD" "PMLD" "SEMH" "SLCN" "HI" "VI" "MSI" "PD" "ASD" "OTH"] (iterate inc 1))]
    (into (sorted-map-by (partial compare-mapped-keys m)) m)))

(def sen-types
  "SENtype (EHCP need) abbreviations"
  (apply sorted-set-by
         (partial compare-mapped-keys sen-type->order)
         (keys sen-type->order)))

(def sen-type->name
  "Map SENtype (EHCP need) abbreviation to name as given in the 2023 SEN2 return guide v1.0"
  (into (sorted-map-by (partial compare-mapped-keys sen-type->order))
        {"SPLD" "Specific learning difficulty"
         "MLD"  "Moderate learning difficulty"
         "SLD"  "Severe learning difficulty"
         "PMLD" "Profound and multiple learning difficulty"
         "SEMH" "Social, emotional and mental health"
         "SLCN" "Speech, language and communication needs"
         "HI"   "Hearing impairment"
         "VI"   "Vision impairment"
         "MSI"  "Multi-sensory impairment"
         "PD"   "Physical disability"
         "ASD"  "Autistic spectrum disorder"
         "OTH"  "Other difficulty"}))

(def sen-type->label
  "Map SENtype (EHCP need) abbreviation to label for display"
  (into (sorted-map-by (partial compare-mapped-keys sen-type->order))
        {"SPLD" "Specific Learning Difficulty"
         "MLD"  "Moderate Learning Difficulty"
         "SLD"  "Severe Learning Difficulty"
         "PMLD" "Profound and Multiple Learning Difficulty"
         "SEMH" "Social, Emotional and Mental Health"
         "SLCN" "Speech, Language and Communication Needs"
         "HI"   "Hearing Impairment"
         "VI"   "Vision Impairment"
         "MSI"  "Multi-Sensory Impairment"
         "PD"   "Physical Disability"
         "ASD"  "Autistic Spectrum Disorder"
         "OTH"  "Other Difficulty"}))
