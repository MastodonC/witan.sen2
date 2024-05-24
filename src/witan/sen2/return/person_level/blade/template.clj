(ns witan.sen2.return.person-level.blade.template
  "Read SEN2 Blade from Excel submission template."
  (:require [clojure.set :as set]
            [clojure.string]
            [tech.v3.libs.poi :as poi]
            [tech.v3.dataset.io.spreadsheet :as ss]
            [tablecloth.api :as tc])
  (:import [java.time LocalDate]
           [java.time LocalDateTime]
           [java.time.format DateTimeFormatter]))



;;; # Utility functions
(defn- parse-date
  "Function to parse date strings of the form \"2022-09-28\" to `:local-date`"
  [s]
  (LocalDate/parse s (DateTimeFormatter/ofPattern "uuuu-LL-dd" (java.util.Locale. "en_GB"))))

(defn- parse-id
  "Parse numeric IDs (read from Excel worksheet as string representations of floats `s`) back to string representations of integers."
  [s]
  (->> s parse-double (format "%.0f")))

(defn- parse-boolean
  "Function to parse \"0.0\" & \"1.0\" `s` to boolean."
  [s]
  (get {"0.0" false "1.0" true} s s))



;;; # SEN2 Person Level Return Modules
(def modules
  "SEN2 return module keys."
  [:sen2 :person :requests :assessment :named-plan :plan-detail :active-plans :placement-detail :sen-need])

(def module-order
  "Map SEN2 return module key to order."
  (zipmap modules (range)))

(def module-titles
  "Map of SEN2 return module titles"
  (into (sorted-map-by #(compare (module-order %1) (module-order %2)))
        {:sen2             "0: SEN2 metadata (`sen2`)"
         :person           "1: Person details (`person`)"
         :requests         "2: Requests (`requests`)"
         :assessment       "3: EHC needs assessments (`assessment`)"
         :named-plan       "4a: Named plan (`named-plan`)"
         :plan-detail      "4b: Plan detail records (`plan-detail`)"
         :active-plans     "5a: Placements - Active plans (`active-plans`)"
         :placement-detail "5b: Placements - Placement details (`placement-detail`)"
         :sen-need         "5c: Placements - SEN need (`sen-need`)"}))



;;; # SEN2 Person Level Census Excel Template
(def module-names-in-template
  "Map module keys to names of tables in the template."
  {:sen2             "Header Record"
   :person           "Person Record"
   :requests         "Requests Records"
   :assessment       "Assessments Records"
   :named-plan       "Named Plan Records"
   :plan-detail      "Plan Detail Records"
   :active-plans     "Active Plan Records"
   :placement-detail "Placement Detail Records"
   :sen-need         "SEN Need Records"})

(defn template-file->template-ds
  "Read \"Data Entry Template\" worksheet from Excel file at `template-filepath` into a dataset."
  [template-filepath]
  (-> template-filepath
      poi/input->workbook
      ((partial some #(when (= "Data Entry Template" (.name %)) %)))
      (ss/sheet->dataset {:header-row? false
                          :key-fn      keyword
                          :parser-fn   :string})))

(defn template-ds->module-row-numbers
  "Extracts start and end row numbers (1-indexed to match Excel) of module tables in `template-ds`."
  [template-ds]
  (-> template-ds
      (tc/select-columns [:column-1])
      (tc/add-columns {:start-row (iterate inc 1)
                       :max-row   tc/row-count})
      (tc/select-rows (comp (partial (into #{} (vals module-names-in-template))) :column-1))
      (tc/add-column :next-start-row (comp rest :start-row) {:size-strategy :na})
      (tc/map-columns :end-row [:next-start-row :max-row] #(if %1 (- %1 3) %2))
      (tc/map-columns :module-key [:column-1] (set/map-invert module-names-in-template))
      ((fn [ds] (zipmap (-> ds :module-key)
                        (-> ds (tc/select-columns [:start-row :end-row]) (tc/rows :as-maps)))))))

(defn template-ds->module-ds
  "Extract module specified in `read-cfg` from `template-ds` into a dataset and post-process with `post-fn`."
  [template-ds & {:keys  [start-row end-row key-fn parser-fn dataset-name]
                  ::keys [post-fn]
                  :or    {key-fn       identity
                          parser-fn    identity
                          post-fn      identity
                          dataset-name "_untitled"}
                  :as    read-cfg}]
  (-> template-ds
      ;; Select rows for module
      (tc/select-rows (range start-row end-row))
      ((fn [ds] ; Get column names from first row, dropping columns with no name.
         (let [col-names (-> ds
                             (tc/select-rows [0])
                             (tc/rows :as-maps)
                             first)]
           (-> ds
               (tc/drop-rows [0])
               (tc/drop-columns (keys (filter (comp nil? val) col-names)))
               (tc/rename-columns col-names)))))
      (tc/rename-columns key-fn)
      (as-> $ (tc/convert-types $ (if (map? parser-fn) (select-keys parser-fn (tc/column-names $)) parser-fn)))
      (tc/set-dataset-name dataset-name)
      post-fn))


;;; ## Module 0: SEN2 metadata (`sen2`)
(def sen2-src-col-name->col-name
  "Map SEN2 module 0 \"SEN2\" source data file column name to column name for the dataset."
  {"Record Type"                         :record-type       ; not in SEN2 Blade CSV Export
   #_                                    :sen2-table-id
   #_                                    :native-id
   #_                                    :sen2-order-seq-column
   #_                                    :source-id
   "Collection Name"                     :collection
   "Year"                                :year
   "Reference Date"                      :reference-date
   "Source Level"                        :source-level
   "Software Code"                       :software-code
   "Release"                             :release
   "Serial Number"                       :serial-no
   "Date & Time \n(ccyy-mm-ddThh:mm:ss)" :datetime
   "LA Number"                           :lea
   "Designated Medical Officer"          :dmo
   "Designated Clinical Officer"         :dco})

(def sen2-parser-fn
  "Parser function for SEN2 module 0 \"SEN2\"."
  {:record-type               :string
   #_#_:sen2-table-id         :string
   #_#_:native-id             :string
   #_#_:sen2-order-seq-column :int32
   #_#_:source-id             :string
   :collection                :string
   :year                      :int32
   :reference-date            [:local-date parse-date]
   :source-level              :string
   :software-code             :string
   :release                   :string
   :serial-no                 :int32
   :datetime                  [:date-time #(->> %
                                                parse-double
                                                (* 1e9 60 60 24)
                                                (.plusNanos (LocalDateTime/parse "1899-12-30T00:00:00.000")))]
   :lea                       :string
   :dmo                       :string
   :dco                       :string})

(def sen2-base-read-cfg
  "Base configuration map (without `:start-row` or `:end-row`) for reading SEN2 module 0 \"SEN2\" into a dataset."
  {:key-fn       #(or (sen2-src-col-name->col-name %) %)
   :parser-fn    sen2-parser-fn
   :dataset-name "sen2"
   ::post-fn     (fn [ds] (-> ds
                              (tc/add-column :native-id (partial :lea))))})

(def sen2-col-name->label
  "Map SEN2 module 0 \"SEN2\" dataset column names to display labels."
  {#_#_:sen2-table-id         "SEN2 table ID"
   :native-id                 "Native ID"
   #_#_:sen2-order-seq-column "SEN2 order seq column"
   #_#_:source-id             "Source ID"
   :collection                "Collection"
   :year                      "Year"
   :reference-date            "Reference date"
   :source-level              "Source level"
   :software-code             "Software code"
   :release                   "Release"
   :serial-no                 "Serial No"
   :datetime                  "DateTime"
   :lea                       "LEA"
   :dmo                       "DMO"
   :dco                       "DCO"})


;;; ## Module 1: Person details (`person`)
(def person-src-col-name->col-name
  "Map SEN2 module 1 \"Person\" source data file column name to column name for the dataset."
  {"Record Type"                                                    :record-type       ; not in SEN2 Blade CSV Export
   "Person ID\n\n(This must be a unique ID for each Person record)" :person-table-id
   "LA\n(From Header Record)"                                       :native-id
   #_                                                               :person-order-seq-column
   #_                                                               :source-id
   #_                                                               :sen2-table-id
   "Surname"                                                        :surname
   "Forename"                                                       :forename
   "Date of Birth \n(ccyy-mm-dd)"                                   :person-birth-date
   "Gender"                                                         :gender-current ; <v1.2
   "Sex"                                                            :sex            ; ≥v1.2
   "Ethnicity"                                                      :ethnicity
   "Post Code"                                                      :postcode
   "UPN - Unique Pupil Number"                                      :upn
   "ULN - Young Persons Unique Learner Number"                      :unique-learner-number
   "UPN and ULN unavailable – Reason"                               :upn-unknown})

(def person-parser-fn
  "Parser function for SEN2 module 1 \"Person\"."
  {:record-type                 :string
   :person-table-id             [:string parse-id]
   :native-id                   :string
   #_#_:person-order-seq-column :int32
   #_#_:source-id               :string
   #_#_:sen2-table-id           :string
   :surname                     :string
   :forename                    :string
   :person-birth-date           [:local-date parse-date]
   :gender-current              :string     ; <v1.2
   :sex                         :string     ; ≥v1.2
   :ethnicity                   :string
   :postcode                    :string
   :upn                         :string
   :unique-learner-number       :string
   :upn-unknown                 :string})

(def person-base-read-cfg
  "Base configuration map (without `:start-row` or `:end-row`) for reading SEN2 module 1 \"Person\" into a dataset."
  {:key-fn       #(or (person-src-col-name->col-name %) %)
   :parser-fn    person-parser-fn
   :dataset-name "person"
   ::post-fn     identity})

(def person-col-name->label
  "Map SEN2 module 1 \"Person\" dataset column names to display labels."
  {:record-type                 "Record type"
   :person-table-id             "Person table ID"
   :native-id                   "Native ID"
   #_#_:person-order-seq-column "Person order seq column"
   #_#_:source-id               "Source ID"
   #_#_:sen2-table-id           "SEN2 table ID"
   :surname                     "Surname"
   :forename                    "Forename"
   :person-birth-date           "Date of birth"
   :gender-current              "Gender"    ; <v1.2
   :sex                         "Sex"       ; ≥v1.2
   :ethnicity                   "Ethnicity"
   :postcode                    "Post code"
   :upn                         "UPN – Unique Pupil Number"
   :unique-learner-number       "ULN - Young person unique learner number"
   :upn-unknown                 "UPN and ULN unavailable - reason"})


;;; ## Module 2: Requests (`requests`)
(def requests-src-col-name->col-name
  "Map SEN2 module 2 \"Requests\" source data file column name to column name for the dataset."
  {"Record Type"                                                                     :record-type       ; not in SEN2 Blade CSV Export
   "Requests record ID\n\n(This must be a unique ID for each Requests record)"       :requests-table-id
   "LA\n(From Header Record)"                                                        :native-id
   #_                                                                                :requests-order-seq-column
   #_                                                                                :source-id
   "Person ID\n\n(This must match the relevant ID in the Person record table)"       :person-table-id
   "Date request was received"                                                       :received-date
   "Initial Request Whilst In RYA"                                                   :rya
   "Request Outcome Date"                                                            :request-outcome-date
   "Request Outcome"                                                                 :request-outcome
   "Request Mediation"                                                               :request-mediation
   "Request Tribunal"                                                                :request-tribunal
   "Exported - Child or young person moves out of LA before assessment is completed" :exported})

(def requests-parser-fn
  "Parser function for SEN2 module 2 \"Requests\"."
  {:record-type                   :string
   :requests-table-id             [:string parse-id]
   :native-id                     :string
   #_#_:requests-order-seq-column :int32
   #_#_:source-id                 :string
   :person-table-id               [:string parse-id]
   :received-date                 [:local-date parse-date]
   :rya                           [:boolean parse-boolean]
   :request-outcome-date          [:local-date parse-date]
   :request-outcome               :string
   :request-mediation             [:boolean parse-boolean]
   :request-tribunal              [:boolean parse-boolean]
   :exported                      :string})

(def requests-base-read-cfg
  "Base configuration map (without `:start-row` or `:end-row`) for reading SEN2 module 2 \"Requests\" into a dataset."
  {:key-fn       #(or (requests-src-col-name->col-name %) %)
   :parser-fn    requests-parser-fn
   :dataset-name "requests"
   ::post-fn     identity})

(def requests-col-name->label
  "Map SEN2 module 2 \"Requests\" dataset column names to display labels."
  {:record-type                   "Record type"
   :requests-table-id             "Requests table ID"
   :native-id                     "Native ID"
   #_#_:requests-order-seq-column "Requests order seq column"
   #_#_:source-id                 "Source ID"
   :person-table-id               "Person table ID"
   :received-date                 "Date request was received"
   :rya                           "Initial request whilst in RYA"
   :request-outcome-date          "Request outcome date"
   :request-outcome               "Request outcome"
   :request-mediation             "Request mediation"
   :request-tribunal              "Request tribunal"
   :exported                      "Exported – child or young person moves out of LA before assessment is completed"})


;;; ## Module 3: EHC needs assessments (`assessment`)
(def assessment-src-col-name->col-name
  "Map SEN2 module 3 \"EHC needs assessments\" source data file column name to column name for the dataset."
  {"Record Type"                                                                   :record-type       ; not in SEN2 Blade CSV Export
   "Person ID\n\n(This must match the relevant ID in the Person record table)"     :person-table-id     ; not in SEN2 Blade CSV Export
   #_                                                                              :assessment-table-id
   "LA\n(From Header Record)"                                                      :native-id
   #_                                                                              :assessment-order-seq-column
   #_                                                                              :source-id
   "Requests record ID\n\n(This must match the relevant ID in the Requests table)" :requests-table-id
   "Assessment Outcome - Decision to issue EHC plan"                               :assessment-outcome
   "Assessment Outcome Date"                                                       :assessment-outcome-date
   "Assessment Mediation"                                                          :assessment-mediation
   "Assessment Tribunal"                                                           :assessment-tribunal
   "Other Mediation"                                                               :other-mediation
   "Other Tribunal"                                                                :other-tribunal
   "Twenty week time limit exceptions apply"                                       :week20})

(def assessment-parser-fn
  "Parser function for SEN2 module 3 \"EHC needs assessments\"."
  {:record-type                     :string
   :person-table-id                 [:string parse-id]
   #_#_:assessment-table-id         :string
   :native-id                       :string
   #_#_:assessment-order-seq-column :int32
   #_#_:source-id                   :string
   :requests-table-id               [:string parse-id]
   :assessment-outcome              :string
   :assessment-outcome-date         [:local-date parse-date]
   :assessment-mediation            [:boolean parse-boolean]
   :assessment-tribunal             [:boolean parse-boolean]
   :other-mediation                 [:boolean parse-boolean]
   :other-tribunal                  [:boolean parse-boolean]
   :week20                          [:boolean parse-boolean]})

(def assessment-base-read-cfg
  "Base configuration map (without `:start-row` or `:end-row`) for reading SEN2 module 3 \"EHC needs assessments\" into a dataset."
  {:key-fn       #(or (assessment-src-col-name->col-name %) %)
   :parser-fn    assessment-parser-fn
   :dataset-name "assessment"
   ::post-fn     identity})

(def assessment-col-name->label
  "Map SEN2 module 3 \"EHC needs assessments\" dataset column names to display labels."
  {:record-type                     "Record type"
   :person-table-id                 "Person table ID"
   #_#_:assessment-table-id         "Assessment table ID"
   :native-id                       "Native ID"
   #_#_:assessment-order-seq-column "Assessment order seq column"
   #_#_:source-id                   "Source ID"
   :requests-table-id               "Requests table ID"
   :assessment-outcome              "Assessment outcome - decision to issue EHC plan"
   :assessment-outcome-date         "Assessment outcome date"
   :assessment-mediation            "Assessment mediation"
   :assessment-tribunal             "Assessment tribunal"
   :other-mediation                 "Other mediation"
   :other-tribunal                  "Other tribunal"
   :week20                          "20-week time limit exceptions apply"})


;;; ## Module 4a: Named plan (`named-plan`)
(def named-plan-src-col-name->col-name
  "Map SEN2 module 4a \"Named plan\" source data file column name to column name for the dataset."
  {"Record Type"                                                                   :record-type       ; not in SEN2 Blade CSV Export
   "Person ID\n\n(This must match the relevant ID in the Person record table)"     :person-table-id   ; not in SEN2 Blade CSV Export
   "Requests record ID\n\n(This must match the relevant ID in the Requests table)" :requests-table-id ; not in SEN2 Blade CSV Export
   #_                                                                              :named-plan-table-id
   "LA\n(From Header Record)"                                                      :native-id
   #_                                                                              :named-plan-order-seq-column
   #_                                                                              :source-id
   #_                                                                              :assessment-table-id
   "EHC Plan Start Date"                                                           :start-date
   "Residential Settings"                                                          :plan-res
   "Work Based Learning Activity"                                                  :plan-wbp
   "Personal Budget taken up"                                                      :pb
   "Personal Budget - Organised Arrangements"                                      :oa
   "Personal Budget - Direct Payments"                                             :dp
   "Date EHC plan ceased"                                                          :cease-date
   "Reason EHC plan ceased"                                                        :cease-reason})

(def named-plan-parser-fn
  "Parser function for SEN2 module 4a \"Named plan\"."
  {:record-type                     :string
   :person-table-id                 [:string parse-id]
   :requests-table-id               [:string parse-id]
   #_#_:named-plan-table-id         :string
   :native-id                       :string
   #_#_:named-plan-order-seq-column :int32
   #_#_:source-id                   :string
   #_#_:assessment-table-id         :string
   :start-date                      [:local-date parse-date]
   :pb                              [:boolean parse-boolean]
   :oa                              [:boolean parse-boolean]
   :cease-date                      [:local-date parse-date]
   :plan-res                        :string
   :plan-wbp                        :string
   :dp                              :string
   :cease-reason                    [:int8 parse-double]})

(def named-plan-base-read-cfg
  "Base configuration map (without `:start-row` or `:end-row`) for reading SEN2 module 4a \"Named plan\" into a dataset."
  {:key-fn       #(or (named-plan-src-col-name->col-name %) %)
   :parser-fn    named-plan-parser-fn
   :dataset-name "named-plan"
   ::post-fn     identity})

(def named-plan-col-name->label
  "Map SEN2 module 4a \"Named plan\" dataset column names to display labels."
  {:record-type                     "Record type"
   :person-table-id                 "Person table ID"
   :requests-table-id               "Requests table ID"
   #_#_:named-plan-table-id         "Named plan table ID"
   :native-id                       "Native ID"
   #_#_:named-plan-order-seq-column "Named plan order seq column"
   #_#_:source-id                   "Source ID"
   #_#_:assessment-table-id         "Assessment table ID"
   :start-date                      "EHC plan start date"
   :plan-res                        "Residential settings"
   :plan-wbp                        "Work-based learning activity"
   :pb                              "Personal budget taken up"
   :oa                              "Personal budget – organised arrangements"
   :dp                              "Personal budget – direct payments"
   :cease-date                      "Date EHC plan ceased"
   :cease-reason                    "Reason EHC plan ceased"})


;;; ## Module 4b: Plan detail records (`plan-detail`)
(def plan-detail-src-col-name->col-name
  "Map SEN2 module 4b \"Plan detail records\" source data file column name to column name for the dataset."
  {"Record Type"                                                                   :record-type       ; not in SEN2 Blade CSV Export
   "Person ID\n\n(This must match the relevant ID in the Person record table)"     :person-table-id   ; not in SEN2 Blade CSV Export
   "Requests record ID\n\n(This must match the relevant ID in the Requests table)" :requests-table-id ; not in SEN2 Blade CSV Export
   #_                                                                              :plan-detail-table-id
   "LA\n(From Header Record)"                                                      :native-id
   #_                                                                              :plan-detail-order-seq-column
   #_                                                                              :source-id
   #_                                                                              :named-plan-table-id
   "URN - Unique Reference Number"                                                 :urn
   "UKPRN - UK Provider Reference Number"                                          :ukprn
   "SEN Setting – Establishment type"                                              :sen-setting
   "SEN Setting - Other"                                                           :sen-setting-other
   "Placement Rank (1=Primary, 2=Secondary)"                                       :placement-rank
   "SEN Unit Indicator"                                                            :sen-unit-indicator
   "Resourced Provision Indicator"                                                 :resourced-provision-indicator})

(def plan-detail-parser-fn
  "Parser function for SEN2 module 4b \"Plan detail records\"."
  {:record-type                      :string
   :person-table-id                  [:string parse-id]
   :requests-table-id                [:string parse-id]
   #_#_:plan-detail-table-id         :string
   :native-id                        :string
   #_#_:plan-detail-order-seq-column :int32
   #_#_:source-id                    :string
   #_#_:named-plan-table-id          :string
   :urn                              [:string parse-id]
   :ukprn                            [:string parse-id]
   :sen-setting                      :string
   :sen-setting-other                :string
   :sen-unit-indicator               [:boolean parse-boolean]
   :resourced-provision-indicator    [:boolean parse-boolean]
   :placement-rank                   [:int8 parse-double]})

(def plan-detail-base-read-cfg
  "Base configuration map (without `:start-row` or `:end-row`) for reading SEN2 module 4b \"Plan detail records\" into a dataset."
  {:key-fn       #(or (plan-detail-src-col-name->col-name %) %)
   :parser-fn    plan-detail-parser-fn
   :dataset-name "plan-detail"
   ::post-fn     identity})

(def plan-detail-col-name->label
  "Map SEN2 module 4b \"Plan detail records\" dataset column names to display labels."
  {:record-type                      "Record type"
   :person-table-id                  "Person table ID"
   :requests-table-id                "Requests table ID"
   #_#_:plan-detail-table-id         "Plan detail table ID"
   :native-id                        "Native ID"
   #_#_:plan-detail-order-seq-column "Plan detail order seq column"
   #_#_:source-id                    "Source ID"
   #_#_:named-plan-table-id          "Named plan table ID"
   :urn                              "URN – Unique Reference Number"
   :ukprn                            "UKPRN - UK Provider Reference Number"
   :sen-setting                      "SEN setting - establishment type"
   :sen-setting-other                "SEN setting – Other"
   :placement-rank                   "Placement rank"
   :sen-unit-indicator               "SEN Unit indicator"
   :resourced-provision-indicator    "Resourced provision indicator"})


;;; ## Module 5a: Placements - Active plans (`active-plans`)
(def active-plans-src-col-name->col-name
  "Map SEN2 module 5a \"Active plans\" source data file column name to column name for the dataset."
  {"Record Type"                                                                   :record-type       ; not in SEN2 Blade CSV Export
   "Person ID\n\n(This must match the relevant ID in the Person record table)"     :person-table-id   ; not in SEN2 Blade CSV Export
   #_                                                                              :active-plans-table-id
   "LA\n(From Header Record)"                                                      :native-id
   #_                                                                              :active-plans-order-seq-column
   #_                                                                              :source-id
   "Requests record ID\n\n(This must match the relevant ID in the Requests table)" :requests-table-id
   "EHC Plan transferred in from another LA during calendar year"                  :transfer-la
   "Residential Settings"                                                          :res            ; <v1.2
   "Work Based Learning Activity"                                                  :wbp            ; <v1.2
   "EHC Plan Review Decisions Date"                                                :review-meeting ; ≥v1.2
   "Annual Review Decision"                                                        :review-outcome ; ≥v1.2
   "Annual Review Meeting Date"                                                    :last-review})

(def active-plans-parser-fn
  "Parser function for SEN2 module 5a \"Active plans\"."
  {:record-type                       :string
   :person-table-id                   [:string parse-id]
   #_#_:active-plans-table-id         :string
   :native-id                         :string
   #_#_:active-plans-order-seq-column :int32
   #_#_:source-id                     :string
   :requests-table-id                 [:string parse-id]
   :transfer-la                       [:string parse-id]
   :res                               :string                  ; <v1.2
   :wbp                               :string                  ; <v1.2
   :review-meeting                    [:local-date parse-date] ; ≥v1.2
   :review-outcome                    :string                  ; ≥v1.2
   :last-review                       [:local-date parse-date]})

(def active-plans-base-read-cfg
  "Base configuration map (without `:start-row` or `:end-row`) for reading SEN2 module 5a \"Active plans\" into a dataset."
  {:key-fn       #(or (active-plans-src-col-name->col-name %) %)
   :parser-fn    active-plans-parser-fn
   :dataset-name "active-plans"
   ::post-fn     identity})

(def active-plans-col-name->label
  "Map SEN2 module 5a \"Active plans\" dataset column names to display labels."
  {:record-type                       "Record type"
   :person-table-id                   "Person table ID"
   #_#_:active-plans-table-id         "Active plans table ID"
   :native-id                         "Native ID"
   #_#_:active-plans-order-seq-column "Active plans order seq column"
   #_#_:source-id                     "Source ID"
   :requests-table-id                 "Requests table ID"
   :transfer-la                       "EHC plan transferred in from another LA during calendar year"
   :res                               "Residential settings"          ; <v1.2
   :wbp                               "Work-based learning activity"  ; <v1.2
   :review-meeting                    "Annual review meeting date"    ; ≥v1.2
   :review-outcome                    "Annual review decision"        ; ≥v1.2
   :last-review                       "Annual review decision date"   ; ≥v1.2: "EHC plan review decisions date" ; <v1.2
   })


;;; ## Module 5b: Placements - Placement details (`placement-detail`)
(def placement-detail-src-col-name->col-name
  "Map SEN2 module 5b \"Placement details\" source data file column name to column name for the dataset."
  {"Record Type"                                                                   :record-type       ; not in SEN2 Blade CSV Export
   "Person ID\n\n(This must match the relevant ID in the Person record table)"     :person-table-id   ; not in SEN2 Blade CSV Export
   "Requests record ID\n\n(This must match the relevant ID in the Requests table)" :requests-table-id ; not in SEN2 Blade CSV Export
   #_                                                                              :placement-detail-table-id
   "LA\n(From Header Record)"                                                      :native-id
   #_                                                                              :placement-detail-order-seq-column
   #_                                                                              :source-id
   #_                                                                              :active-plans-table-id
   "Residential Settings"                                                          :res                ; ≥v1.2
   "Work Based Learning Activity"                                                  :wbp                ; ≥v1.2
   "URN - Unique Reference Number"                                                 :urn
   "UKPRN - UK Provider Reference Number"                                          :ukprn
   "SEN Setting – Establishment type"                                              :sen-setting
   "SEN Setting - Other"                                                           :sen-setting-other
   "Placement Rank (1=Primary, 2=Secondary)"                                       :placement-rank
   "Placement Start Date"                                                          :entry-date
   "Placement Leaving Date"                                                        :leaving-date
   "Attendance Pattern"                                                            :attendance-pattern ; <v1.2
   "SEN Unit Indicator"                                                            :sen-unit-indicator
   "Resourced Provision Indicator"                                                 :resourced-provision-indicator})

(def placement-detail-parser-fn
  "Parser function for SEN2 module 5b \"Placement details\"."
  {:record-type                           :string
   :person-table-id                       [:string parse-id]
   :requests-table-id                     [:string parse-id]
   #_#_:placement-detail-table-id         :string
   :native-id                             :string
   #_#_:placement-detail-order-seq-column :int32
   #_#_:source-id                         :string
   #_#_:active-plans-table-id             :string
   :res                                   :string ; ≥v1.2
   :wbp                                   :string ; ≥v1.2
   :urn                                   [:string parse-id]
   :ukprn                                 [:string parse-id]
   :sen-setting                           :string
   :sen-setting-other                     :string
   :placement-rank                        [:int8 parse-double]
   :entry-date                            [:local-date parse-date]
   :leaving-date                          [:local-date parse-date]
   :attendance-pattern                    :string ; <v1.2
   :sen-unit-indicator                    [:boolean parse-boolean]
   :resourced-provision-indicator         [:boolean parse-boolean]})

(def placement-detail-base-read-cfg
  "Base configuration map (without `:start-row` or `:end-row`) for reading SEN2 module 5b \"Placement details\" into a dataset."
  {:key-fn       #(or (placement-detail-src-col-name->col-name %) %)
   :parser-fn    placement-detail-parser-fn
   :dataset-name "placement-detail"
   ::post-fn     identity})

(def placement-detail-col-name->label
  "Map SEN2 module 5b \"Placement details\" dataset column names to display labels."
  {:record-type                           "Record type"
   :person-table-id                       "Person table ID"
   :requests-table-id                     "Requests table ID"
   #_#_:placement-detail-table-id         "Placement detail table ID"
   :native-id                             "Native ID"
   #_#_:placement-detail-order-seq-column "Placement detail order seq column"
   #_#_:source-id                         "Source ID"
   #_#_:active-plans-table-id             "Active plans table ID"
   :res                                   "Residential settings"         ; ≥v1.2
   :wbp                                   "Work-based learning activity" ; ≥v1.2
   :urn                                   "URN – Unique Reference Number"
   :ukprn                                 "UKPRN – UK Provider Reference Number"
   :sen-setting                           "SEN Setting - Establishment type"
   :sen-setting-other                     "Not in education – Other"
   :placement-rank                        "Placement rank"
   :entry-date                            "Placement start date"
   :leaving-date                          "Placement leaving date"
   :attendance-pattern                    "Attendance pattern" ; <v1.2
   :sen-unit-indicator                    "SEN Unit indicator"
   :resourced-provision-indicator         "Resourced provision indicator"})


;;; ## Module 5c: Placements - SEN need (`sen-need`)
(def sen-need-src-col-name->col-name
  "Map SEN2 module 5c \"SEN need\" source data file column name to column name for the dataset."
  {"Record Type"                                                                   :record-type       ; not in SEN2 Blade CSV Export
   "Person ID\n\n(This must match the relevant ID in the Person record table)"     :person-table-id   ; not in SEN2 Blade CSV Export
   "Requests record ID\n\n(This must match the relevant ID in the Requests table)" :requests-table-id ; not in SEN2 Blade CSV Export
   #_                                                                              :sen-need-table-id
   "LA\n(From Header Record)"                                                      :native-id
   #_                                                                              :sen-need-order-seq-column
   #_                                                                              :source-id
   #_                                                                              :active-plans-table-id
   "SEN Type of Need"                                                              :sen-type
   "SEN Type of Need rank  (1=Primary, 2=Secondary)"                               :sen-type-rank})

(def sen-need-parser-fn
  "Parser function for SEN2 module 5 \"SEN need\"."
  {:record-type                   :string
   :person-table-id               [:string parse-id]
   :requests-table-id             [:string parse-id]
   #_#_:sen-need-table-id         :string
   :native-id                     :string
   #_#_:sen-need-order-seq-column :int32
   #_#_:source-id                 :string
   #_#_:active-plans-table-id     :string
   :sen-type                      :string
   :sen-type-rank                 [:int8 parse-double]})

(def sen-need-base-read-cfg
  "Base configuration map (without `:start-row` or `:end-row`) for reading SEN2 module 5 \"SEN need\" into a dataset."
  {:key-fn       #(or (sen-need-src-col-name->col-name %) %)
   :parser-fn    sen-need-parser-fn
   :dataset-name "sen-need"
   ::post-fn     identity})

(def sen-need-col-name->label
  "Map SEN2 module 5c \"SEN need\" dataset column names to display labels."
  {:record-type                   "Record type"
   :person-table-id               "Person table ID"
   :requests-table-id             "Requests table ID"
   #_#_:sen-need-table-id         "SEN need table ID"
   :native-id                     "Native ID"
   #_#_:sen-need-order-seq-column "SEN need order seq column"
   #_#_:source-id                 "Source ID"
   #_#_:active-plans-table-id     "Active plans table ID"
   :sen-type                      "SEN type"
   :sen-type-rank                 "SEN type rank"})



;;; # Definitions and functions to read all modules
(def module-base-read-cfg
  "Map of base configuration maps (without `:start-row` or `:end-row`) for reading each module into a dataset."
  {:sen2             sen2-base-read-cfg
   :person           person-base-read-cfg
   :requests         requests-base-read-cfg
   :assessment       assessment-base-read-cfg
   :named-plan       named-plan-base-read-cfg
   :plan-detail      plan-detail-base-read-cfg
   :active-plans     active-plans-base-read-cfg
   :placement-detail placement-detail-base-read-cfg
   :sen-need         sen-need-base-read-cfg})

(defn make-module-read-cfg
  "Map of configuration maps for reading each module into a dataset."
  [module-base-read-cfg module-row-numbers]
  (merge-with merge module-base-read-cfg module-row-numbers))

(defn template-ds->ds-map
  "Read module datasets from `template-ds` using a `module-read-cfg'` specified via `opts` map,
   returning map of datasets with same keys as the `module-read-cfg'`.
  The `module-read-cfg'` used for reading the `template-ds` is derived from the `opts` map as follows:
  - if `opts` contains a `:module-read-cfg` key then the val of that is used,
  - otherwise the `module-read-cfg'` is derived by merging:
    - the val of the `:module-base-read-cfg` key (defaults to ns `module-base-read-cfg` if not specified)
    - the val of the `:module-row-numbers`   key (calculated from `template-ds` if not specified)."
  [template-ds & {:keys [module-read-cfg
                         module-base-read-cfg
                         module-row-numbers]
                  :or {module-base-read-cfg module-base-read-cfg}
                  :as opts}]
  (let [module-read-cfg' (or module-read-cfg
                             (merge-with merge
                                         module-base-read-cfg
                                         (or module-row-numbers
                                             (template-ds->module-row-numbers template-ds))))]
    (update-vals module-read-cfg' #(template-ds->module-ds template-ds %))))

(defn template-file->ds-map
  "Read module datasets from `template-filepath` using a `module-read-cfg'` specified via `opts` map,
   returning map of datasets with same keys as the `module-read-cfg'`."
  [template-filepath & {:as opts}]
  (template-ds->ds-map (template-file->template-ds template-filepath) opts))



;;; # Definitions and functions to manage module hierarchy
(def module-key->ancestors
  "Map module-keys to vector of ancestor module-keys, parent last."
  {:sen2             []
   :person           [:sen2]
   :requests         [:sen2 :person]
   :assessment       [:sen2 :person :requests]
   :named-plan       [:sen2 :person :requests :assessment]
   :plan-detail      [:sen2 :person :requests :assessment :named-plan]
   :active-plans     [:sen2 :person :requests]
   :placement-detail [:sen2 :person :requests :active-plans]
   :sen-need         [:sen2 :person :requests :active-plans]})

(def module-key->parent
  "Map module-key to module-key of parent."
  (update-vals module-key->ancestors last))



;;; # Collated column labels, for EDA and presentation
(def module-src-col-name->col-name
  "Map of maps mapping source data file column labels to column name for the dataset for each module."
  {:sen2             sen2-src-col-name->col-name
   :person           person-src-col-name->col-name
   :requests         requests-src-col-name->col-name
   :assessment       assessment-src-col-name->col-name
   :named-plan       named-plan-src-col-name->col-name
   :plan-detail      plan-detail-src-col-name->col-name
   :active-plans     active-plans-src-col-name->col-name
   :placement-detail placement-detail-src-col-name->col-name
   :sen-need         sen-need-src-col-name->col-name})

(def module-col-name->label
  "Map of maps mapping dataset column names to display labels for each module."
  {:sen2             sen2-col-name->label
   :person           person-col-name->label
   :requests         requests-col-name->label
   :assessment       assessment-col-name->label
   :named-plan       named-plan-col-name->label
   :plan-detail      plan-detail-col-name->label
   :active-plans     active-plans-col-name->label
   :placement-detail placement-detail-col-name->label
   :sen-need         sen-need-col-name->label})

(def col-name->label
  "Map SEN2 dataset column names to display labels."
  (apply merge (vals module-col-name->label)))

(defn parser-fn
  "Collated parser functions for all SEN2 modules."
  ([] (parser-fn module-base-read-cfg))
  ([module-read-cfg] (reduce-kv (fn [m _ v] (merge m (:parser-fn v))) {} module-read-cfg)))

