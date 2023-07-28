(ns witan.sen2.return.person-level.sen-setting
  "SEN setting - Establishment type <SENsetting>, per items 4.7c & 5.5c of the DfE 2023 SEN2 return.")

;;; # Reference:
;; - [Special educational needs person level survey 2023: guide](https://www.gov.uk/government/publications/special-educational-needs-person-level-survey-2023-guide)

(def sen-setting->order
  "Map SEN setting abbreviations to order"
  (let [m (zipmap ["OLA" "OPA" "EHE" "EYP" "NEET" "NIEC" "NIEO"] (iterate inc 1))]
    (into (sorted-map-by (fn [k1 k2] (compare [(get m k1) k1]
                                              [(get m k2) k2]))) m)))

(def sen-settings
  "SEN setting abbreviations"
  (apply sorted-set-by
         (fn [k1 k2] (compare [(get sen-setting->order k1) k1]
                              [(get sen-setting->order k2) k2]))
         (keys sen-setting->order)))

(def sen-setting->label
  "Map SEN setting abbreviation to label for display"
  (into (sorted-map-by  (fn [k1 k2] (compare [(get sen-setting->order k1) k1]
                                             [(get sen-setting->order k2) k2])))
        {"OLA" "Other LA Arrangements (inc. EOTAS)"
         "OPA" "Other Parent/Person Arrangements (exc. EHE)"
         "EHE" "Elective Home Education"
         "EYP" "Early Years Provider"
         "NEET" "Not in Education, Training or Employment"
         "NIEC" "Ceasing"
         "NIEO" "Other"}))

(def sen-setting->description
  "Map SEN setting abbreviation to description"
  (into (sorted-map-by  (fn [k1 k2] (compare [(get sen-setting->order k1) k1]
                                             [(get sen-setting->order k2) k2])))
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
