(ns witan.sen2.return.person-level.blade.csv
  "Read SEN2 Blade CSV export."
  (:require [tablecloth.api :as tc])
  (:import [java.time LocalDate]
           [java.time.format DateTimeFormatter]))

;;; # Utility functions
(defn- parse-date
  "Function to parse date strings of the form \"2022-Sep-28 00:00:00\" to `:local-date`"
  [s]
  (LocalDate/parse (re-find #"^[^ ]+" s) (DateTimeFormatter/ofPattern "uuuu-LLL-dd" (java.util.Locale. "en_US"))))



;;; # SEN2 Person Level Return Modules
(def module-order
  "Map SEN2 return module key to order."
  (zipmap [:sen2 :person :requests :assessment :named-plan :plan-detail :active-plans :placement-detail :sen-need]
          (range)))

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



;;; # CSV data files
(defn csv->ds
  "Read CSV file from `file-path` into dataset using options in `cfg` and post-process with `post-fn`."
  [file-path & {:keys  [key-fn parser-fn dataset-name]
                ::keys [post-fn]
                :or    {post-fn identity}
                :as    cfg}]
  (-> file-path
      (tc/dataset cfg)
      post-fn))


;;; ## CSV file names
(defn file-names
  "Default Blade CSV export file names for `export-date` (in \"DD-MM-YYYY\" format)."
  [export-date]
  (update-vals 
   {:sen2             "Blade-Export_%s_sen2.csv"
    :person           "Blade-Export_%s_person.csv"
    :requests         "Blade-Export_%s_requests.csv"
    :assessment       "Blade-Export_%s_assessment.csv"
    :named-plan       "Blade-Export_%s_namedplan.csv"
    :plan-detail      "Blade-Export_%s_plandetail.csv"
    :active-plans     "Blade-Export_%s_activeplans.csv"
    :placement-detail "Blade-Export_%s_placementdetail.csv"
    :sen-need         "Blade-Export_%s_senneed.csv"}
   (fn [v] (format v export-date))))

(defn file-paths
  "Default Blade CSV export file paths for `export-date` (as string in \"DD-MM-YYYY\" format) in `data-dir`."
  [export-dir export-date]
  (update-vals (file-names export-date) (partial str export-dir)))


;;; ## Module 0: SEN2 metadata (`sen2`)
(def sen2-src-col-name->col-name
  "Map SEN2 module 0 \"SEN2\" CSV file column name to column name for the dataset."
  {"sen2tableid"        :sen2-table-id
   "NativeId"           :native-id
   "sen2orderseqcolumn" :sen2-order-seq-column
   "sourceid"           :source-id
   "collection"         :collection
   "year"               :year
   "referencedate"      :reference-date
   "sourcelevel"        :source-level
   "softwarecode"       :software-code
   "release"            :release
   "serialno"           :serial-no
   "datetime"           :datetime
   "lea"                :lea
   "dmo"                :dmo
   "dco"                :dco})

(def sen2-parser-fn
  "Parser function for SEN2 module 0 \"SEN2\" CSV file columns."
  {:sen2-table-id         :string
   :native-id             :string
   :sen2-order-seq-column :int32
   :source-id             :string
   :collection            :string
   :year                  :int32
   :reference-date        [:local-date parse-date]
   :source-level          :string
   :software-code         :string
   :release               :string
   :serial-no             :int32
   :datetime              [:date-time #(LocalDate/parse
                                        %
                                        (DateTimeFormatter/ofPattern
                                         "yyyy-LLL-dd HH:mm:ss"
                                         (java.util.Locale. "en_US")))]
   :lea                   :string
   :dmo                   :string
   :dco                   :string})

(def sen2-read-cfg
  "Configuration map for reading SEN2 module 0 \"SEN2\" CSV file into a dataset."
  {:key-fn       #(or (sen2-src-col-name->col-name %) %)
   :parser-fn    sen2-parser-fn
   :dataset-name "sen2"})

(def sen2-col-name->label
  "Map SEN2 module 0 \"SEN2\" dataset column names to display labels."
  {:sen2-table-id         "SEN2 table ID"
   :native-id             "Native ID"
   :sen2-order-seq-column "SEN2 order seq column"
   :source-id             "Source ID"
   :collection            "Collection"
   :year                  "Year"
   :reference-date        "Reference date"
   :source-level          "Source level"
   :software-code         "Software code"
   :release               "Release"
   :serial-no             "Serial No"
   :datetime              "DateTime"
   :lea                   "LEA"
   :dmo                   "DMO"
   :dco                   "DCO"})


;;; ## Module 1: Person details (`person`)
(def person-src-col-name->col-name
  "Map SEN2 module 1 \"Person\" CSV file column name to column name for the dataset."
  {"persontableid"        :person-table-id
   "NativeId"             :native-id
   "personorderseqcolumn" :person-order-seq-column
   "sourceid"             :source-id
   "sen2tableid"          :sen2-table-id
   "surname"              :surname
   "forename"             :forename
   "personbirthdate"      :person-birth-date
   "gendercurrent"        :gender-current
   "ethnicity"            :ethnicity
   "postcode"             :postcode
   "upn"                  :upn
   "uniquelearnernumber"  :unique-learner-number
   "upnunknown"           :upn-unknown})

(def person-parser-fn
  "Parser function for SEN2 module 1 \"Person\" CSV file columns."
  {:person-table-id         :string
   :native-id               :string
   :person-order-seq-column :int32
   :source-id               :string
   :sen2-table-id           :string
   :surname                 :string
   :forename                :string
   :person-birth-date       [:local-date parse-date]
   :postcode                :string
   :upn                     :string
   :unique-learner-number   :string
   :gender-current          :string
   :ethnicity               :string
   :upn-unknown             :string})

(def person-read-cfg
  "Configuration map for reading SEN2 module 1 \"Person\" CSV file into a dataset."
  {:key-fn       #(or (person-src-col-name->col-name %) %)
   :parser-fn    person-parser-fn
   :dataset-name "person"})

(def person-col-name->label
  "Map SEN2 module 1 \"Person\" dataset column names to display labels."
  {:person-table-id         "Person table ID"
   :native-id               "Native ID"
   :person-order-seq-column "Person order seq column"
   :source-id               "Source ID"
   :sen2-table-id           "SEN2 table ID"
   :surname                 "Surname"
   :forename                "Forename"
   :person-birth-date       "Date of birth"
   :gender-current          "Gender"
   :ethnicity               "Ethnicity"
   :postcode                "Post code"
   :upn                     "UPN – Unique Pupil Number"
   :unique-learner-number   "ULN - Young person unique learner number"
   :upn-unknown             "UPN and ULN unavailable - reason"})


;;; ## Module 2: Requests (`requests`)
(def requests-src-col-name->col-name
  "Map SEN2 module 2 \"Requests\" CSV file column name to column name for the dataset."
  {"requeststableid"        :requests-table-id
   "NativeId"               :native-id
   "requestsorderseqcolumn" :requests-order-seq-column
   "sourceid"               :source-id
   "persontableid"          :person-table-id
   "receiveddate"           :received-date
   "rya"                    :rya
   "requestoutcomedate"     :request-outcome-date
   "requestoutcome"         :request-outcome
   "requestmediation"       :request-mediation
   "requesttribunal"        :request-tribunal
   "exported"               :exported})

(def requests-parser-fn
  "Parser function for SEN2 module 2 \"Requests\" CSV file columns."
  {:requests-table-id         :string
   :native-id                 :string
   :requests-order-seq-column :int32
   :source-id                 :string
   :person-table-id           :string
   :received-date             [:local-date parse-date]
   :rya                       :boolean
   :request-outcome-date      [:local-date parse-date]
   :request-outcome           :string
   :request-mediation         :boolean
   :request-tribunal          :boolean
   :exported                  :string})

(def requests-read-cfg
  "Configuration map for reading SEN2 module 2 \"Requests\" CSV file into a dataset."
  {:key-fn       #(or (requests-src-col-name->col-name %) %)
   :parser-fn    requests-parser-fn
   :dataset-name "requests"})

(def requests-col-name->label
  "Map SEN2 module 2 \"Requests\" dataset column names to display labels."
  {:requests-table-id         "Requests table ID"
   :native-id                 "Native ID"
   :requests-order-seq-column "Requests order seq column"
   :source-id                 "Source ID"
   :person-table-id           "Person table ID"
   :received-date             "Date request was received"
   :rya                       "Initial request whilst in RYA"
   :request-outcome-date      "Request outcome date"
   :request-outcome           "Request outcome"
   :request-mediation         "Request mediation"
   :request-tribunal          "Request tribunal"
   :exported                  "Exported – child or young person moves out of LA before assessment is completed"})


;;; ## Module 3: EHC needs assessments (`assessment`)
(def assessment-src-col-name->col-name
  "Map SEN2 module 3 \"EHC needs assessments\" CSV file column name to column name for the dataset."
  {"assessmenttableid"        :assessment-table-id
   "NativeId"                 :native-id
   "assessmentorderseqcolumn" :assessment-order-seq-column
   "sourceid"                 :source-id
   "requeststableid"          :requests-table-id
   "assessmentoutcome"        :assessment-outcome
   "assessmentoutcomedate"    :assessment-outcome-date
   "assessmentmediation"      :assessment-mediation
   "assessmenttribunal"       :assessment-tribunal
   "othermediation"           :other-mediation
   "othertribunal"            :other-tribunal
   "week20"                   :week20})

(def assessment-parser-fn
  "Parser function for SEN2 module 3 \"EHC needs assessments\" CSV file columns."
  {:assessment-table-id         :string
   :native-id                   :string
   :assessment-order-seq-column :int32
   :source-id                   :string
   :requests-table-id           :string
   :assessment-outcome-date     [:local-date parse-date]
   :assessment-mediation        :boolean
   :assessment-tribunal         :boolean  
   :other-mediation             :boolean
   :other-tribunal              :boolean
   :week20                      :boolean
   :assessment-outcome          :string})

(def assessment-read-cfg
  "Configuration map for reading SEN2 module 3 \"EHC needs assessments\" CSV file into a dataset."
  {:key-fn       #(or (assessment-src-col-name->col-name %) %)
   :parser-fn    assessment-parser-fn
   :dataset-name "assessment"})

(def assessment-col-name->label
  "Map SEN2 module 3 \"EHC needs assessments\" dataset column names to display labels."
  {:assessment-table-id         "Assessment table ID"
   :native-id                   "Native ID"
   :assessment-order-seq-column "Assessment order seq column"
   :source-id                   "Source ID"
   :requests-table-id           "Requests table ID"
   :assessment-outcome          "Assessment outcome - decision to issue EHC plan"
   :assessment-outcome-date     "Assessment outcome date"
   :assessment-mediation        "Assessment mediation"
   :assessment-tribunal         "Assessment tribunal"
   :other-mediation             "Other mediation"
   :other-tribunal              "Other tribunal"
   :week20                      "20-week time limit exceptions apply "})


;;; ## Module 4a: Named plan (`named-plan`)
(def named-plan-src-col-name->col-name
  "Map SEN2 module 4a \"Named plan\" CSV file column name to column name for the dataset."
  {"namedplantableid"        :named-plan-table-id
   "NativeId"                :native-id
   "namedplanorderseqcolumn" :named-plan-order-seq-column
   "sourceid"                :source-id
   "assessmenttableid"       :assessment-table-id
   "startdate"               :start-date
   "planres"                 :plan-res
   "planwbp"                 :plan-wbp
   "pb"                      :pb
   "oa"                      :oa
   "dp"                      :dp
   "ceasedate"               :cease-date
   "ceasereason"             :cease-reason})

(def named-plan-parser-fn
  "Parser function for SEN2 module 4a \"Named plan\" CSV file columns."
  {:named-plan-table-id         :string
   :native-id                   :string
   :named-plan-order-seq-column :int32
   :source-id                   :string
   :assessment-table-id         :string
   :start-date                  [:local-date parse-date]
   :pb                          :boolean
   :oa                          :boolean
   :cease-date                  [:local-date parse-date]
   :plan-res                    :string
   :plan-wbp                    :string
   :dp                          :string
   :cease-reason                [:int8 parse-long]})

(def named-plan-read-cfg
  "Configuration map for reading SEN2 module 4a \"Named plan\" CSV file into a dataset."
  {:key-fn       #(or (named-plan-src-col-name->col-name %) %)
   :parser-fn    named-plan-parser-fn
   :dataset-name "named-plan"})

(def named-plan-col-name->label
  "Map SEN2 module 4a \"Named plan\" dataset column names to display labels."
  {:named-plan-table-id         "Named plan table ID"
   :native-id                   "Native ID"
   :named-plan-order-seq-column "Named plan order seq column"
   :source-id                   "Source ID"
   :assessment-table-id         "Assessment table ID"
   :start-date                  "EHC plan start date"
   :plan-res                    "Residential settings"
   :plan-wbp                    "Work-based learning activity"
   :pb                          "Personal budget taken up"
   :oa                          "Personal budget – organised arrangements"
   :dp                          "Personal budget – direct payments"
   :cease-date                  "Date EHC plan ceased"
   :cease-reason                "Reason EHC plan ceased"})


;;; ## Module 4b: Plan detail records (`plan-detail`)
(def plan-detail-src-col-name->col-name
  "Map SEN2 module 4b \"Plan detail records\" CSV file column name to column name for the dataset."
  {"plandetailtableid"           :plan-detail-table-id
   "NativeId"                    :native-id
   "plandetailorderseqcolumn"    :plan-detail-order-seq-column
   "sourceid"                    :source-id
   "namedplantableid"            :named-plan-table-id
   "urn"                         :urn
   "ukprn"                       :ukprn
   "sensetting"                  :sen-setting
   "sensettingother"             :sen-setting-other
   "placementrank"               :placement-rank
   "senunitindicator"            :sen-unit-indicator
   "resourcedprovisionindicator" :resourced-provision-indicator})

(def plan-detail-parser-fn
  "Parser function for SEN2 module 4b \"Plan detail records\" CSV file columns."
  {:plan-detail-table-id          :string
   :native-id                     :string
   :plan-detail-order-seq-column  :int32
   :source-id                     :string
   :named-plan-table-id           :string
   :urn                           :string
   :ukprn                         :string
   :sen-setting-other             :string
   :sen-unit-indicator            :boolean
   :resourced-provision-indicator :boolean
   :placement-rank                [:int-8 parse-long]
   :sen-setting                   :string})

(def plan-detail-read-cfg
  "Configuration map for reading SEN2 module 4b \"Plan detail records\" CSV file into a dataset."
  {:key-fn       #(or (plan-detail-src-col-name->col-name %) %)
   :parser-fn    plan-detail-parser-fn
   :dataset-name "plan-detail"})

(def plan-detail-col-name->label
  "Map SEN2 module 4b \"Plan detail records\" dataset column names to display labels."
  {:plan-detail-table-id          "Plan detail table ID"
   :native-id                     "Native ID"
   :plan-detail-order-seq-column  "Plan detail order seq column"
   :source-id                     "Source ID"
   :named-plan-table-id           "Named plan table ID"
   :urn                           "URN – Unique Reference Number"
   :ukprn                         "UKPRN - UK Provider Reference Number"
   :sen-setting                   "SEN setting - establishment type"
   :sen-setting-other             "SEN setting – Other"
   :placement-rank                "Placement rank"
   :sen-unit-indicator            "SEN Unit indicator"
   :resourced-provision-indicator "Resourced provision indicator"})


;;; ## Module 5a: Placements - Active plans (`active-plans`)
(def active-plans-src-col-name->col-name
  "Map SEN2 module 5a \"Active plans\" CSV file column name to column name for the dataset."
  {"activeplanstableid"        :active-plans-table-id
   "NativeId"                  :native-id
   "activeplansorderseqcolumn" :active-plans-order-seq-column
   "sourceid"                  :source-id
   "requeststableid"           :requests-table-id
   "transferla"                :transfer-la
   "res"                       :res
   "wbp"                       :wbp
   "lastreview"                :last-review})

(def active-plans-parser-fn
  "Parser function for SEN2 module 5a \"Active plans\" CSV file columns."
  {:active-plans-table-id         :string
   :native-id                     :string
   :active-plans-order-seq-column :int32
   :source-id                     :string
   :requests-table-id             :string
   :last-review                   [:local-date parse-date]
   :transfer-la                   :string
   :res                           :string
   :wbp                           :string})

(def active-plans-read-cfg
  "Configuration map for reading SEN2 module 5a \"Active plans\" CSV file into a dataset."
  {:key-fn       #(or (active-plans-src-col-name->col-name %) %)
   :parser-fn    active-plans-parser-fn
   :dataset-name "active-plans"})

(def active-plans-col-name->label
  "Map SEN2 module 5a \"Active plans\" dataset column names to display labels."
  {:active-plans-table-id         "Active plans table ID"
   :native-id                     "Native ID"
   :active-plans-order-seq-column "Active plans order seq column"
   :source-id                     "Source ID"
   :requests-table-id             "Requests table ID"
   :transfer-la                   "EHC plan transferred in from another LA during calendar year"
   :res                           "Residential settings"
   :wbp                           "Work-based learning activity"
   :last-review                   "EHC plan review decisions date"})


;;; ## Module 5b: Placements - Placement details (`placement-detail`)
(def placement-detail-src-col-name->col-name
  "Map SEN2 module 5b \"Placement details\" CSV file column name to column name for the dataset."
  {"placementdetailtableid"        :placement-detail-table-id
   "NativeId"                      :native-id
   "placementdetailorderseqcolumn" :placement-detail-order-seq-column
   "sourceid"                      :source-id
   "activeplanstableid"            :active-plans-table-id
   "urn"                           :urn
   "ukprn"                         :ukprn
   "sensetting"                    :sen-setting
   "sensettingother"               :sen-setting-other
   "placementrank"                 :placement-rank
   "entrydate"                     :entry-date
   "leavingdate"                   :leaving-date
   "attendancepattern"             :attendance-pattern
   "senunitindicator"              :sen-unit-indicator
   "resourcedprovisionindicator"   :resourced-provision-indicator})

(def placement-detail-parser-fn
  "Parser function for SEN2 module 5b \"Placement details\" CSV file columns."
  {:placement-detail-table-id         :string
   :native-id                         :string
   :placement-detail-order-seq-column :int32
   :source-id                         :string
   :active-plans-table-id             :string
   :urn                               :string
   :ukprn                             :string
   :sen-setting-other                 :string
   :placement-rank                    [:int8 parse-long]
   :entry-date                        [:local-date parse-date]
   :leaving-date                      [:local-date parse-date]
   :sen-unit-indicator                :boolean
   :resourced-provision-indicator     :boolean
   :sen-setting                       :string
   :attendance-pattern                :string})

(def placement-detail-read-cfg
  "Configuration map for reading SEN2 module 5b \"Placement details\" CSV file into a dataset."
  {:key-fn       #(or (placement-detail-src-col-name->col-name %) %)
   :parser-fn    placement-detail-parser-fn
   :dataset-name "placement-detail"})

(def placement-detail-col-name->label
  "Map SEN2 module 5b \"Placement details\" dataset column names to display labels."
  {:placement-detail-table-id         "Placement detail table ID"
   :native-id                         "Native ID"
   :placement-detail-order-seq-column "Placement detail order seq column"
   :source-id                         "Source ID"
   :active-plans-table-id             "Active plans table ID"
   :urn                               "URN – Unique Reference Number"
   :ukprn                             "UKPRN – UK Provider Reference Number"
   :sen-setting                       "SEN Setting - Establishment type"
   :sen-setting-other                 "Not in education – Other"
   :placement-rank                    "Placement rank"
   :entry-date                        "Placement start date"
   :leaving-date                      "Placement leaving date"
   :attendance-pattern                "Attendance pattern"
   :sen-unit-indicator                "SEN Unit indicator"
   :resourced-provision-indicator     "Resourced provision indicator"})


;;; ## Module 5c: Placements - SEN need (`sen-need`)
(def sen-need-src-col-name->col-name
  "Map SEN2 module 5c \"SEN need\" CSV file column name to column name for the dataset."
  {"senneedtableid"        :sen-need-table-id
   "NativeId"              :native-id
   "senneedorderseqcolumn" :sen-need-order-seq-column
   "sourceid"              :source-id
   "activeplanstableid"    :active-plans-table-id
   "sentyperank"           :sen-type-rank
   "sentype"               :sen-type})

(def sen-need-parser-fn
  "Parser function for SEN2 module 5 \"SEN need\" CSV file columns."
  {:sen-need-table-id         :string
   :native-id                 :string
   :sen-need-order-seq-column :int32
   :source-id                 :string
   :active-plans-table-id     :string
   :sen-type-rank             [:int8 parse-long]
   :sen-type                  :string})

(def sen-need-read-cfg
  "Configuration map for reading SEN2 module 5 \"SEN need\" CSV file into a dataset."
  {:key-fn       #(or (sen-need-src-col-name->col-name %) %)
   :parser-fn    sen-need-parser-fn
   :dataset-name "sen-need"})

(def sen-need-col-name->label
  "Map SEN2 module 5c \"SEN need\" dataset column names to display labels."
  {:sen-need-table-id         "SEN need table ID"
   :native-id                 "Native ID"
   :sen-need-order-seq-column "SEN need order seq column"
   :source-id                 "Source ID"
   :active-plans-table-id     "Active plans table ID"
   :sen-type-rank             "SEN type rank"
   :sen-type                  "SEN type"})



;;; # Functions to read all modules
(def module-src-col-name->col-name
  "Map of maps mapping CSV file column labels to column name for the dataset for each module."
  {:sen2             sen2-src-col-name->col-name
   :person           person-src-col-name->col-name
   :requests         requests-src-col-name->col-name
   :assessment       assessment-src-col-name->col-name
   :named-plan       named-plan-src-col-name->col-name
   :plan-detail      plan-detail-src-col-name->col-name
   :active-plans     active-plans-src-col-name->col-name
   :placement-detail placement-detail-src-col-name->col-name
   :sen-need         sen-need-src-col-name->col-name})

(def module-read-cfg
  "Map of configuration maps for reading data from CSV files into dataset for each module."
  {:sen2             sen2-read-cfg
   :person           person-read-cfg
   :requests         requests-read-cfg
   :assessment       assessment-read-cfg
   :named-plan       named-plan-read-cfg
   :plan-detail      plan-detail-read-cfg
   :active-plans     active-plans-read-cfg
   :placement-detail placement-detail-read-cfg
   :sen-need         sen-need-read-cfg})

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

(defn file-paths->ds-map
  "Read CSV files specified in `file-paths'` map
  using read configuration from corresponding key of `module-read-cfg'`
  into map of datasets with same keys."
  ([file-paths'] (file-paths->ds-map file-paths' module-read-cfg))
  ([file-paths' read-cfg'] (reduce-kv #(assoc %1 %2 (csv->ds %3 (get read-cfg' %2))) {} file-paths')))

(defn ->ds-map
  "Read CSV files specified in `file-names'` map from `data-dir`
  using read configuration from corresponding key of `read-cfg'`
  into map of datasets with same keys."
  ([data-dir file-names'] (->ds-map data-dir file-names' module-read-cfg))
  ([data-dir file-names' read-cfg'] (file-paths->ds-map (update-vals file-names' (partial str data-dir)) read-cfg')))



;;; # Functions to manipulate the SEN2 datasets
(def col-name->label
  "Map SEN2 dataset column names to display labels."
  (merge sen2-col-name->label
         person-col-name->label
         requests-col-name->label
         assessment-col-name->label
         named-plan-col-name->label
         plan-detail-col-name->label
         active-plans-col-name->label
         placement-detail-col-name->label
         sen-need-col-name->label))

(def parser-fn
  "Collated parser functions for all SEN2 CSV file columns."
  (merge sen2-parser-fn
         person-parser-fn
         requests-parser-fn
         assessment-parser-fn
         named-plan-parser-fn
         plan-detail-parser-fn
         active-plans-parser-fn
         placement-detail-parser-fn
         sen-need-parser-fn))



;;; # Definitions and functions to facilitate `:*table-id` usage
(def table-id-col-names
  "Names of `:.+-table-id` columns."
  [:sen2-table-id :person-table-id :requests-table-id
   :assessment-table-id :named-plan-table-id :plan-detail-table-id
   :active-plans-table-id :placement-detail-table-id :sen-need-table-id])

(def table-id-col-name->label
  "Map names of table-id columns to labels."
  (select-keys col-name->label table-id-col-names))

(defn ds-map->table-id-ds
  "Return dataset of `:*table-id` key relationships, given map `ds-map` of datasets read from CSV files.
  The returned dataset permits traversing up each branch of the dataset hierarchy without going through all intermediate datasets."
  [{:keys [sen2 person requests assessment named-plan plan-detail active-plans placement-detail sen-need]
    :as   ds-map}]
  (-> (tc/select-columns sen2 [:sen2-table-id])
      (tc/left-join (tc/select-columns person [:person-table-id :sen2-table-id]) [:sen2-table-id])
      (tc/left-join (tc/select-columns requests [:requests-table-id :person-table-id]) [:person-table-id])
      (tc/left-join (tc/concat-copying 
                     (-> (tc/select-columns assessment [:assessment-table-id :requests-table-id])
                         (tc/left-join 
                          (tc/select-columns named-plan [:named-plan-table-id :assessment-table-id])
                          [:assessment-table-id])
                         (tc/left-join 
                          (tc/select-columns plan-detail [:plan-detail-table-id :named-plan-table-id])
                          [:named-plan-table-id]))
                     (-> (tc/select-columns active-plans [:active-plans-table-id :requests-table-id])
                         (tc/left-join 
                          (tc/concat-copying
                           (tc/select-columns placement-detail [:placement-detail-table-id :active-plans-table-id])
                           (tc/select-columns sen-need [:sen-need-table-id :active-plans-table-id]))
                          [:active-plans-table-id])))
                    [:requests-table-id])
      (tc/select-columns table-id-col-names)
      (tc/set-dataset-name "table-id-ds")))

(def table-id->ancestors
  "Map table-ids to vector of ancestor table-ids, parent last."
  {:sen2-table-id             []
   :person-table-id           [:sen2-table-id]
   :requests-table-id         [:sen2-table-id :person-table-id]
   :assessment-table-id       [:sen2-table-id :person-table-id :requests-table-id]
   :named-plan-table-id       [:sen2-table-id :person-table-id :requests-table-id :assessment-table-id]
   :plan-detail-table-id      [:sen2-table-id :person-table-id :requests-table-id :assessment-table-id :named-plan-table-id]
   :active-plans-table-id     [:sen2-table-id :person-table-id :requests-table-id]
   :placement-detail-table-id [:sen2-table-id :person-table-id :requests-table-id :active-plans-table-id]
   :sen-need-table-id         [:sen2-table-id :person-table-id :requests-table-id :active-plans-table-id]})

(def table-id->parent
  "Map table-id to table-id of parent."
  (update-vals table-id->ancestors last))

(defn ds-map->ancestor-table-id-ds
  "Return dataset of `:*table-id` key relationships for ancestors of table with primary key `table-id`,
  given map `ds-map` of datasets read from CSV files."
  [ds-map table-id]
  (-> ds-map
      ds-map->table-id-ds
      (tc/select-columns (table-id->ancestors table-id))
      (tc/drop-missing)
      (tc/unique-by)))
