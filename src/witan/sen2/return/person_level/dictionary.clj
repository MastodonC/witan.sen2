(ns witan.sen2.return.person-level.dictionary
  "Dictionary of lookups for SEN2 person-level return items.

  from [gov.uk](https://www.gov.uk/guidance/special-educational-needs-survey) unless stated otherwise.

  See in particular:
  - [Special educational needs survey: guide to submitting data](https://www.gov.uk/guidance/special-educational-needs-survey)
  - [Special educational needs person level survey 2023: guide](https://www.gov.uk/government/publications/special-educational-needs-person-level-survey-2023-guide)
  - [Special educational needs person level survey: technical specification](https://www.gov.uk/government/publications/special-educational-needs-person-level-survey-technical-specification)")



;;; # Utilities
#_(defn- compare-get
    [m k1 k2]
    (compare [(get m k1) k1]
             [(get m k2) k2]))

(defn- compare-get-in
  [m k k1 k2]
  (compare [(get-in m [k1 k]) k1]
           [(get-in m [k2 k]) k2]))

(defn- sorted-set-of-keys-by-val-order
  [m]
  (into (sorted-set-by (partial compare-get-in m :order))
        (keys m)))



;;; # SEN setting - Establishment type <SENsetting>, per items 4.7c & 5.5c of the DfE 2023 SEN2 return.
(def sen-setting
  "SEN setting - Establishment type <SENsetting>, per items 4.7c & 5.5c of the DfE 2023 SEN2 return."
  (let [m {"OLA"  {:order       1
                   :label       "Other LA Arrangements (inc. EOTAS)"
                   :description (str "Other – arrangements made by the local authority "
                                     "in accordance with section 61 of the Children and Families Act 2014, "
                                     "(\"education otherwise than at a school or post-16 institution etc\").")}
           "OPA"  {:order       2
                   :label       "Other Parent/Person Arrangements (exc. EHE)"
                   :description (str "Other – alternative arrangements made by parents or young person "
                                     "in accordance with section 42(5) of the Children and Families Act 2014, "
                                     "excluding those who are subject to elective home education.")}
           "EHE"  {:order       3
                   :label       "Elective Home Education"
                   :description (str "Elective home education – alternative arrangements made by parents or young person "
                                     "in accordance with section 42(5) of the Children and Families Act 2014, for elective home education.")}
           "EYP"  {:order       4
                   :label       "Early Years Provider"
                   :description (str "Early years provider with no GIAS URN "
                                     "(for example private nursery, independent early years providers and childminders).")}
           "NEET" {:order       5
                   :label       "Not in Education, Training or Employment"
                   :description (str "Not in education, training or employment (aged 16-25).")}
           "NIEC" {:order       6
                   :label       "Ceasing"
                   :description (str "Not in education or training – Notice to cease issued.")}
           "NIEO" {:order       7
                   :label       "Other"
                   :description (str "Not in education – Other – "
                                     "Where this is used, the local authority will be prompted for further information in COLLECT, "
                                     "for example, transferred into the local authority with an EHC plan and awaiting placement.")}}]
    (into (sorted-map-by (partial compare-get-in m :order)) m)))

(def sen-settings
  "SEN setting abbreviations"
  (sorted-set-of-keys-by-val-order sen-setting))



;;; # EHCP needs <SENtype>, per item 5.6 of the DfE 2023 SEN2 return.
(def sen-type
  (let [m {"SPLD" {:order 1
                   :name  "Specific learning difficulty"
                   :label "Specific Learning Difficulty"}
           "MLD"  {:order 2
                   :name  "Moderate learning difficulty"
                   :label "Moderate Learning Difficulty"}
           "SLD"  {:order 3
                   :name  "Severe learning difficulty"
                   :label "Severe Learning Difficulty"}
           "PMLD" {:order 4
                   :name  "Profound and multiple learning difficulty"
                   :label "Profound and Multiple Learning Difficulty"}
           "SEMH" {:order 5
                   :name  "Social, emotional and mental health"
                   :label "Social, Emotional and Mental Health"}
           "SLCN" {:order 6
                   :name  "Speech, language and communication needs"
                   :label "Speech, Language and Communication Needs"}
           "HI"   {:order 7
                   :name  "Hearing impairment"
                   :label "Hearing Impairment"}
           "VI"   {:order 8
                   :name  "Vision impairment"
                   :label "Vision Impairment"}
           "MSI"  {:order 9
                   :name  "Multi-sensory impairment"
                   :label "Multi-Sensory Impairment"}
           "PD"   {:order 10
                   :name  "Physical disability"
                   :label "Physical Disability"}
           "ASD"  {:order 11
                   :name  "Autistic spectrum disorder"
                   :label "Autistic Spectrum Disorder"}
           "OTH"  {:order 12
                   :name  "Other difficulty"
                   :label "Other Difficulty"}}]
    (into (sorted-map-by (partial compare-get-in m :order)) m)))

(def sen-types
  "SENtype (EHCP need) abbreviations"
  (sorted-set-of-keys-by-val-order sen-type))

