(ns witan.sen2.needs
  "EHCP needs <SENtype>, per item 5.6 of the DfE 2023 SEN2 return.")

;;; # Reference:
;; - [Special educational needs person level survey 2023: guide](https://www.gov.uk/government/publications/special-educational-needs-person-level-survey-2023-guide)

(def need->order
  "Map EHCP need abbreviations to order"
  (let [m (zipmap ["SPLD" "MLD" "SLD" "PMLD" "SEMH" "SLCN" "HI" "VI" "MSI" "PD" "ASD" "OTH"] (iterate inc 1))]
    (into (sorted-map-by (fn [k1 k2] (compare [(get m k1) k1]
                                              [(get m k2) k2]))) m)))

(def needs
  "EHCP need abbreviations"
  (apply sorted-set-by
         (fn [k1 k2] (compare [(get need->order k1) k1]
                              [(get need->order k2) k2]))
         (keys need->order)))

(def need->name
  "Map EHCP need abbreviation to name as given in the 2023 SEN2 return guide v1.0"
  (into (sorted-map-by  (fn [k1 k2] (compare [(get need->order k1) k1]
                                             [(get need->order k2) k2])))
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
(def need->label
  "Map EHCP need abbreviation to label for display"
  (into (sorted-map-by  (fn [k1 k2] (compare [(get need->order k1) k1]
                                             [(get need->order k2) k2])))
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

