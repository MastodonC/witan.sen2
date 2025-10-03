(ns witan.sen2.return.person-level.dictionary
  "Dictionary of lookups for SEN2 person-level return items.

  from [gov.uk](https://www.gov.uk/guidance/special-educational-needs-survey) unless stated otherwise.

  See in particular:
  - [Special educational needs survey: guide to submitting data](https://www.gov.uk/guidance/special-educational-needs-survey)
  - [Special educational needs person level survey 2025: guide v1.4](https://assets.publishing.service.gov.uk/media/675831b5f72b1d96e06bbfdf/SEN_survey_-_guide_to_person_level_SEN2_return_2025.pdf)
  - [Special educational needs person level survey: technical specification](https://www.gov.uk/government/publications/special-educational-needs-person-level-survey-technical-specification)")



;;; # Utilities
(defn- key-comparator-fn-by
  [key->order]
  (fn [k1 k2]
    (compare [(key->order k1) k1]
             [(key->order k2) k2])))



;;; # Lookups
;;; ## Named plan cease reason - reason the EHC plan ended: <CeaseReason>, per item 4.6 of the DfE 2025 SEN2 return.
(def cease-reason
  "Named plan cease reason - reason the EHC plan ended: <CeaseReason>, per item 4.6 of the DfE 2025 SEN2 return.
   Note: Map is keyed by the numeric codes used in the SEN2 return. Value maps include MC `:abbreviation`."
  (-> {1 {:order        1
          :abbreviation "age"
          :label        "Max Age"
          :description  (str "Reached maximum age "
                             "(this is the end of the academic year during which "
                             "the young person turned 25, "
                             "or turns 19 if the young person enters the adult estate).")}
       2 {:order        2
          :abbreviation "needs-met"
          :label        "Needs Met"
          :description  (str "Ongoing educational or training needs "
                             "being met without an EHC plan "
                             "– this should only be used where there is "
                             "ongoing education and training.")}
       3 {:order        3
          :abbreviation "higher-ed"
          :label        "Higher Ed"
          :description  "Moved on to higher education."}
       4 {:order        4
          :abbreviation "employed"
          :label        "Employed"
          :description  "Moved on to paid employment, excluding apprenticeships."}
       5 {:order        5
          :abbreviation "transfer"
          :label        "Transfer Out"
          :description  "Transferred to another local authority in England."}
       6 {:order        6
          :abbreviation "no-engagement"
          :label        "No Engagement"
          :description  (str "Young person no longer wishes to engage in education or training "
                             "– this should include where the young person "
                             "has reached their 18th birthday and is no "
                             "longer under a statutory duty to participate "
                             "in education or training, and where a young "
                             "person has yet to reach their 18th birthday "
                             "but is not complying with their statutory duty "
                             "to participate in education or training.")}
       7 {:order        7
          :abbreviation "left-england"
          :label        "Left England"
          :description  "Person has moved outside England."}
       8 {:order        8
          :abbreviation "deceased"
          :label        "Deceased"
          :description  "Person deceased."}
       9 {:order        9
          :abbreviation "other"
          :label        "Other"
          :description  (str "Other - Where a large number of cases are "
                             "recorded as ‘other’, the local authority will "
                             "be prompted for further information in COLLECT.")}}
      (as-> $ (into (sorted-map-by (key-comparator-fn-by (update-vals $ :order))) $))))


;;; ## SEN setting - Establishment type <SENsetting>, per items 4.7c & 5.7c of the DfE 2025 SEN2 return.
(def sen-setting
  "SEN setting - Establishment type <SENsetting>, per items 4.7c & 5.7c of the DfE 2025 SEN2 return."
  (-> {"OLA"  {:order       1
               :label       "Other LA Arrangements (inc. EOTAS)"
               :description (str "Other – arrangements made by the local authority "
                                 "in accordance with section 61 of the 2014 Act, "
                                 "(\"Special educational provision otherwise than in schools, post-16 institutions etc\", "
                                 "commonly referred to as EOTAS) "
                                 "- for example therapy that is special educational provision for a child "
                                 "and where it would be inappropriate to provide this in any school etc.")}
       "OPA"  {:order       2
               :label       "Other Parent/Person Arrangements (exc. EHE)"
               :description (str "Other – alternative arrangements made by parents or young person "
                                 "in accordance with section 42(5) of the 2014 Act, "
                                 "excluding those who are subject to elective home education "
                                 "(for example, where parents have chosen to arrange and pay for an independent school placement).")}
       "EHE"  {:order       3
               :label       "Elective Home Education"
               :description (str "Elective home education – alternative arrangements "
                                 "made by parents or young person "
                                 "in accordance with section 42(5) of the 2014 Act "
                                 "for elective home education.")}
       "EYP"  {:order       4
               :label       "Early Years Provider"
               :description (str "Early years provider with no GIAS URN "
                                 "(for example private nursery, independent early years providers and childminders).")}
       "OTH"  {:order       5
               :label       "Other"
               :description (str "Other – Includes where a type of setting is specified in the EHC plan "
                                 "(e.g., special school) but no specific setting is named. "
                                 "Where this is used, the local authority will be prompted for further information in COLLECT.")}
       "NEET" {:order       6
               :label       "Not in education, employment or training – Aged 16-25"
               :description (str "Not in education, employment, or training (aged 16-25).")}
       "NIEC" {:order       7
               :label       "Not in education or training – Ceasing"
               :description (str "Not in education or training – Notice to cease issued.")}
       "NIEO" {:order       8
               :label       "Not in education or training – Other"
               :description (str "Not in education or training – Other – "
                                 "Where this is used, the local authority will be prompted for further information in COLLECT, "
                                 "for example, transferred into the local authority with an EHC plan and awaiting placement.")}}
      (as-> $ (into (sorted-map-by (key-comparator-fn-by (update-vals $ :order))) $))))

(def sen-settings
  "SEN setting abbreviations"
  (-> sen-setting
      (as-> $ (into (sorted-set-by (key-comparator-fn-by (update-vals $ :order))) (keys $)))))


;;; ## EHCP needs <SENtype>, per item 5.8a of the DfE 2025 SEN2 return.
(def sen-type
  (-> {"SPLD" {:order 1
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
       "DS"   {:order 12
               :name  "Down Syndrome"
               :label "Down Syndrome"}
       "OTH"  {:order 13
               :name  "Other difficulty"
               :label "Other Difficulty"}}
      (as-> $ (into (sorted-map-by (key-comparator-fn-by (update-vals $ :order))) $))))

(def sen-types
  "SENtype (EHCP need) abbreviations"
  (-> sen-type
      (as-> $ (into (sorted-set-by (key-comparator-fn-by (update-vals $ :order))) (keys $)))))
