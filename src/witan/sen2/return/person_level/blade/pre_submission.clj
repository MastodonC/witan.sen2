(ns witan.sen2.return.person-level.blade.pre-submission
  "Read SEN2 Blade from pore-submission Excel workbook."
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [tech.v3.libs.poi :as poi]
            [tech.v3.dataset.io.spreadsheet :as ss]
            [tablecloth.api :as tc])
  (:import [java.time LocalDate]
           [java.time LocalDateTime]
           [java.time.format DateTimeFormatter]))



;;; # Utility functions
(defn- parse-id
  "Parse numeric ID (which may be read from Excel worksheet as float) `x` back to string representations of integer."
  [x]
  (cond
    (float?  x) (format "%.0f" x)
    (string? x) (str/trim x)
    :else       (throw (ex-info "Unhandled item type." {:data x, :class (class x)}))))

(defn- parse-binary->boolean
  "Parse 0 & 1 `x` (read from Excel worksheet as string or floats) to boolean."
  [x]
  (cond
    (float?   x) (case (int x)  0 false 1 true nil)
    (string?  x) (case      x  "0" false "1" true nil)
    (boolean? x) x
    :else        (throw (ex-info "Unhandled item type." {:data x, :class (class x)}))))

(defn replace-missing-sen2-estab-indicators
  "Given dataset `ds` with `sen2-estab` columns [`:urn` `:ukprn` `:sen-unit-indicator` `:resourced-provision-indicator` `:sen-setting`],
   replaces missing `:sen-unit-indicator` `:resourced-provision-indicator` with `false`
   for records where at least one of `:urn`, `:ukprn` & `:sen-setting` is not `nil`."
  [ds]
  (tc/map-rows ds (fn [{:keys [urn ukprn sen-unit-indicator resourced-provision-indicator sen-setting]}]
                    (if (some some? [urn ukprn sen-setting])
                      {:sen-unit-indicator            (if (some? sen-unit-indicator)
                                                        sen-unit-indicator
                                                        false)
                       :resourced-provision-indicator (if (some? resourced-provision-indicator)
                                                        resourced-provision-indicator
                                                        false)}
                      {:sen-unit-indicator            sen-unit-indicator
                       :resourced-provision-indicator resourced-provision-indicator}))))

(defn update-read-cfg
  "Update configuration map `read-cfg` for reading SEN2 module into a dataset with `worksheet->ds`."
  [read-cfg & {:keys [src-col-name->col-name parser-fn dataset-name post-fn additional-post-fn]}]
  (cond-> read-cfg
    src-col-name->col-name (assoc :key-fn #(or (src-col-name->col-name %) %))
    parser-fn              (assoc :parser-fn parser-fn)
    dataset-name           (assoc :dataset-name dataset-name)
    post-fn                (assoc ::post-fn post-fn)
    additional-post-fn     (#(let [current-post-fn (::post-fn %)]
                               (if current-post-fn
                                 (assoc % ::post-fn (comp additional-post-fn current-post-fn))
                                 (assoc % ::post-fn additional-post-fn))))))



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



;;; # SEN2 Person Level Census Pre-submission Excel Workbook
(def module-sheet-names
  "Map module keys to pre-submission workbook sheet names."
  {#_#_:sen2         "Header"                                         ; not in pre-submission workbooks
   :person           "Person"
   :requests         "Requests"
   :assessment       "Assessments"
   :named-plan       "Named Plan"
   :plan-detail      "Plan Detail"
   :active-plans     "Active Plan"
   :placement-detail "Placement Detail"
   :sen-need         "SEN Need"})

(defn filepath->workbook
  "Read Excel file as workbook object."
  [filepath]
  (-> filepath
      poi/input->workbook))

(defn pre-submission-workbook->module-worksheets
  "Return map of module worksheets extracted from SEN2 pre-submission workbook `pre-submission-workbook`
   according to `module-sheet-names` mapping of module keys to sheet names."
  [pre-submission-workbook & {:keys [module-sheet-names]}]
  (update-vals module-sheet-names
               (fn [v] (-> pre-submission-workbook
                           ((partial some #(when (= v (.name %)) %)))))))

(defn pre-submission-workbook-filepath->module-worksheets
  "Return map of module worksheets extracted from SEN2 pre-submission workbook file at `pre-submission-workbook-filepath`
   according to `module-sheet-names` mapping of module keys to sheet names."
  [pre-submission-workbook-filepath & {:as opts}]
  (-> pre-submission-workbook-filepath
      filepath->workbook
      (pre-submission-workbook->module-worksheets opts)))

(defn worksheet->ds
  "Read `sheet` into dataset using options in `opts` and post-process with `post-fn`."
  [sheet & {:keys  [key-fn parser-fn dataset-name]
            ::keys [post-fn]
            :or    {post-fn identity}
            :as    opts}]
  (-> sheet
      (ss/sheet->dataset opts)
      post-fn))

;;; ## Module 0: SEN2 metadata (`sen2`)
;; Not in the pre-submission data.


;;; ## Module 1: Person details (`person`)
(def person-src-col-name->col-name
  "Map SEN2 module 1 \"Person\" source data file column name to column name for the dataset."
  {"persontableid"       :person-table-id
   #_                    :native-id                                   ; not in pre-submission workbooks
   #_                    :person-order-seq-column                     ; not in pre-submission workbooks
   #_                    :source-id                                   ; not in pre-submission workbooks
   #_                    :sen2-table-id                               ; not in pre-submission workbooks
   "surname"             :surname
   "forename"            :forename
   "personbirthdate"     :person-birth-date
   "gendercurrent"       :gender-current                              ; <v1.2
   "sex"                 :sex                                         ; ≥v1.2
   "ethnicity"           :ethnicity
   "postcode"            :postcode
   "upn"                 :upn
   "uniquelearnernumber" :unique-learner-number
   "upnunknown"          :upn-unknown})

(def person-parser-fn
  "Parser function for SEN2 module 1 \"Person\"."
  {:person-table-id             [:string parse-id]
   #_#_:native-id               :string                               ; not in pre-submission workbooks
   #_#_:person-order-seq-column :int32                                ; not in pre-submission workbooks
   #_#_:source-id               :string                               ; not in pre-submission workbooks
   #_#_:sen2-table-id           :string                               ; not in pre-submission workbooks
   :surname                     :string
   :forename                    :string
   :person-birth-date           :local-date
   :gender-current              :string                               ; <v1.2
   :sex                         :string                               ; ≥v1.2
   :ethnicity                   :string
   :postcode                    :string
   :upn                         :string
   :unique-learner-number       :string
   :upn-unknown                 :string})

(def person-read-cfg
  "Configuration map for reading SEN2 module 1 \"Person\" into a dataset."
  {:key-fn       #(or (person-src-col-name->col-name %) %)
   :parser-fn    person-parser-fn
   :dataset-name "person"})

(def person-col-name->label
  "Map SEN2 module 1 \"Person\" dataset column names to display labels."
  {:person-table-id             "Person table ID"
   #_#_:native-id               "Native ID"                           ; not in pre-submission workbooks
   #_#_:person-order-seq-column "Person order seq column"             ; not in pre-submission workbooks
   #_#_:source-id               "Source ID"                           ; not in pre-submission workbooks
   #_#_:sen2-table-id           "SEN2 table ID"                       ; not in pre-submission workbooks
   :surname                     "Surname"
   :forename                    "Forename"
   :person-birth-date           "Date of birth"
   :gender-current              "Gender"                              ; <v1.2
   :sex                         "Sex"                                 ; ≥v1.2
   :ethnicity                   "Ethnicity"
   :postcode                    "Post code"
   :upn                         "UPN – Unique Pupil Number"
   :unique-learner-number       "ULN - Young person unique learner number"
   :upn-unknown                 "UPN and ULN unavailable - reason"})


;;; ## Module 2: Requests (`requests`)
(def requests-src-col-name->col-name
  "Map SEN2 module 2 \"Requests\" source data file column name to column name for the dataset."
  {"requeststableid"    :requests-table-id
   #_                   :native-id                                    ; not in pre-submission workbooks
   #_                   :requests-order-seq-column                    ; not in pre-submission workbooks
   #_                   :source-id                                    ; not in pre-submission workbooks
   "persontableid"      :person-table-id
   "receiveddate"       :received-date
   "rquestsource"       :request-source                               ; ≥v1.3
   "rya"                :rya
   "requestoutcomedate" :request-outcome-date
   "requestoutcome"     :request-outcome
   "requestmediation"   :request-mediation
   "requesttribunal"    :request-tribunal
   "exported"           :exported})

(def requests-parser-fn
  "Parser function for SEN2 module 2 \"Requests\"."
  {:requests-table-id             [:string parse-id]
   #_#_:native-id                 :string                             ; not in pre-submission workbooks
   #_#_:requests-order-seq-column :int32                              ; not in pre-submission workbooks
   #_#_:source-id                 :string                             ; not in pre-submission workbooks
   :person-table-id               [:string parse-id]
   :received-date                 :local-date
   :request-source                :int8                               ; ≥v1.3
   :rya                           [:boolean parse-binary->boolean]
   :request-outcome-date          :local-date
   :request-outcome               :string
   :request-mediation             [:boolean parse-binary->boolean]
   :request-tribunal              [:boolean parse-binary->boolean]
   :exported                      [:string parse-id]})

(def requests-read-cfg
  "Configuration map for reading SEN2 module 2 \"Requests\" into a dataset."
  {:key-fn       #(or (requests-src-col-name->col-name %) %)
   :parser-fn    requests-parser-fn
   :dataset-name "requests"})

(def requests-col-name->label
  "Map SEN2 module 2 \"Requests\" dataset column names to display labels."
  {:requests-table-id             "Requests table ID"
   #_#_:native-id                 "Native ID"                         ; not in pre-submission workbooks
   #_#_:requests-order-seq-column "Requests order seq column"         ; not in pre-submission workbooks
   #_#_:source-id                 "Source ID"                         ; not in pre-submission workbooks
   :person-table-id               "Person table ID"
   :received-date                 "Date request was received"
   :request-source                "Source of request for an EHC needs assessment" ; ≥v1.3
   :rya                           "Initial request whilst in RYA"
   :request-outcome-date          "Request outcome date"
   :request-outcome               "Request outcome"
   :request-mediation             "Request mediation"
   :request-tribunal              "Request tribunal"
   :exported                      "Exported – child or young person moves out of LA before assessment is completed"})


;;; ## Module 3: EHC needs assessments (`assessment`)
(def assessment-src-col-name->col-name
  "Map SEN2 module 3 \"EHC needs assessments\" source data file column name to column name for the dataset."
  {"persontableid"         :person-table-id                           ; not in SEN2 Blade CSV Export
   #_                      :assessment-table-id                       ; not in pre-submission workbooks
   #_                      :native-id                                 ; not in pre-submission workbooks
   #_                      :assessment-order-seq-column               ; not in pre-submission workbooks
   #_                      :source-id                                 ; not in pre-submission workbooks
   "requeststableid"       :requests-table-id
   "assessmentoutcome"     :assessment-outcome
   "assessmentoutcomedate" :assessment-outcome-date
   "assessmentmediation"   :assessment-mediation
   "assessmenttribunal"    :assessment-tribunal
   "othermediation"        :other-mediation
   "othertribunal"         :other-tribunal
   "week20"                :week20})

(def assessment-parser-fn
  "Parser function for SEN2 module 3 \"EHC needs assessments\"."
  {:person-table-id                 [:string parse-id]                ; not in SEN2 Blade CSV Export
   #_#_:assessment-table-id         :string                           ; not in pre-submission workbooks
   #_#_:native-id                   :string                           ; not in pre-submission workbooks
   #_#_:assessment-order-seq-column :int32                            ; not in pre-submission workbooks
   #_#_:source-id                   :string                           ; not in pre-submission workbooks
   :requests-table-id               :string
   :assessment-outcome              :string
   :assessment-outcome-date         :local-date
   :assessment-mediation            [:boolean parse-binary->boolean]
   :assessment-tribunal             [:boolean parse-binary->boolean]  
   :other-mediation                 [:boolean parse-binary->boolean]
   :other-tribunal                  [:boolean parse-binary->boolean]
   :week20                          [:boolean parse-binary->boolean]})

(def assessment-read-cfg
  "Configuration map for reading SEN2 module 3 \"EHC needs assessments\" into a dataset."
  {:key-fn       #(or (assessment-src-col-name->col-name %) %)
   :parser-fn    assessment-parser-fn
   :dataset-name "assessment"})

(def assessment-col-name->label
  "Map SEN2 module 3 \"EHC needs assessments\" dataset column names to display labels."
  {:person-table-id                 "Person table ID"                 ; not in SEN2 Blade CSV Export
   #_#_:assessment-table-id         "Assessment table ID"             ; not in pre-submission workbooks
   #_#_:native-id                   "Native ID"                       ; not in pre-submission workbooks
   #_#_:assessment-order-seq-column "Assessment order seq column"     ; not in pre-submission workbooks
   #_#_:source-id                   "Source ID"                       ; not in pre-submission workbooks
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
  {"persontableid"   :person-table-id                                 ; not in SEN2 Blade CSV Export
   "requeststableid" :requests-table-id                               ; not in SEN2 Blade CSV Export
   #_                :named-plan-table-id                             ; not in pre-submission workbooks
   #_                :native-id                                       ; not in pre-submission workbooks
   #_                :named-plan-order-seq-column                     ; not in pre-submission workbooks
   #_                :source-id                                       ; not in pre-submission workbooks
   #_                :assessment-table-id                             ; not in pre-submission workbooks
   "startdate"       :start-date
   "planres"         :plan-res
   "planwbp"         :plan-wbp
   "pb"              :pb
   "oa"              :oa
   "dp"              :dp
   "ceasedate"       :cease-date
   "ceasereason"     :cease-reason})

(def named-plan-parser-fn
  "Parser function for SEN2 module 4a \"Named plan\"."
  {:person-table-id                 [:string parse-id]                ; not in SEN2 Blade CSV Export
   :requests-table-id               [:string parse-id]                ; not in SEN2 Blade CSV Export
   #_#_:named-plan-table-id         :string                           ; not in pre-submission workbooks
   #_#_:native-id                   :string                           ; not in pre-submission workbooks
   #_#_:named-plan-order-seq-column :int32                            ; not in pre-submission workbooks
   #_#_:source-id                   :string                           ; not in pre-submission workbooks
   #_#_:assessment-table-id         :string                           ; not in pre-submission workbooks
   :start-date                      :local-date
   :plan-res                        :string
   :plan-wbp                        :string
   :pb                              [:boolean parse-binary->boolean]
   :oa                              [:boolean parse-binary->boolean]
   :dp                              :string
   :cease-date                      :local-date
   :cease-reason                    :int8})

(def named-plan-read-cfg
  "Configuration map for reading SEN2 module 4a \"Named plan\" into a dataset."
  {:key-fn       #(or (named-plan-src-col-name->col-name %) %)
   :parser-fn    named-plan-parser-fn
   :dataset-name "named-plan"})

(def named-plan-col-name->label
  "Map SEN2 module 4a \"Named plan\" dataset column names to display labels."
  {:person-table-id                 "Person table ID"                 ; not in SEN2 Blade CSV Export
   :requests-table-id               "Requests table ID"               ; not in SEN2 Blade CSV Export
   #_#_:named-plan-table-id         "Named plan table ID"             ; not in pre-submission workbooks
   #_#_:native-id                   "Native ID"                       ; not in pre-submission workbooks
   #_#_:named-plan-order-seq-column "Named plan order seq column"     ; not in pre-submission workbooks
   #_#_:source-id                   "Source ID"                       ; not in pre-submission workbooks
   #_#_:assessment-table-id         "Assessment table ID"             ; not in pre-submission workbooks
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
  {"persontableid"               :person-table-id                     ; not in SEN2 Blade CSV Export
   "requeststableid"             :requests-table-id                   ; not in SEN2 Blade CSV Export
   #_                            :plan-detail-table-id                ; not in pre-submission workbooks
   #_                            :native-id                           ; not in pre-submission workbooks
   #_                            :plan-detail-order-seq-column        ; not in pre-submission workbooks
   #_                            :source-id                           ; not in pre-submission workbooks
   #_                            :named-plan-table-id                 ; not in pre-submission workbooks
   "urn"                         :urn
   "ukprn"                       :ukprn
   "sensetting"                  :sen-setting
   "sensettingother"             :sen-setting-other
   "placementrank"               :placement-rank
   "senunitindicator"            :sen-unit-indicator
   "resourcedprovisionindicator" :resourced-provision-indicator})

(def plan-detail-parser-fn
  "Parser function for SEN2 module 4b \"Plan detail records\"."
  {:person-table-id                  [:string parse-id]               ; not in SEN2 Blade CSV Export
   :requests-table-id                [:string parse-id]               ; not in SEN2 Blade CSV Export
   #_#_:plan-detail-table-id         :string                          ; not in pre-submission workbooks
   #_#_:native-id                    :string                          ; not in pre-submission workbooks
   #_#_:plan-detail-order-seq-column :int32                           ; not in pre-submission workbooks
   #_#_:source-id                    :string                          ; not in pre-submission workbooks
   #_#_:named-plan-table-id          :string                          ; not in pre-submission workbooks
   :urn                              [:string parse-id]
   :ukprn                            [:string parse-id]
   :sen-setting                      :string
   :sen-setting-other                :string
   :sen-unit-indicator               :boolean
   :resourced-provision-indicator    :boolean
   :placement-rank                   :int8})

(def plan-detail-read-cfg
  "Configuration map for reading SEN2 module 4b \"Plan detail records\" into a dataset."
  {:key-fn       #(or (plan-detail-src-col-name->col-name %) %)
   :parser-fn    plan-detail-parser-fn
   :dataset-name "plan-detail"})

(def plan-detail-col-name->label
  "Map SEN2 module 4b \"Plan detail records\" dataset column names to display labels."
  {:person-table-id                  "Person table ID"                ; not in SEN2 Blade CSV Export
   :requests-table-id                "Requests table ID"              ; not in SEN2 Blade CSV Export
   #_#_:plan-detail-table-id         "Plan detail table ID"           ; not in pre-submission workbooks
   #_#_:native-id                    "Native ID"                      ; not in pre-submission workbooks
   #_#_:plan-detail-order-seq-column "Plan detail order seq column"   ; not in pre-submission workbooks
   #_#_:source-id                    "Source ID"                      ; not in pre-submission workbooks
   #_#_:named-plan-table-id          "Named plan table ID"            ; not in pre-submission workbooks
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
  {"persontableid"          :person-table-id                          ; not in SEN2 Blade CSV Export
   #_                       :active-plans-table-id                    ; not in pre-submission workbooks
   #_                       :native-id                                ; not in pre-submission workbooks
   #_                       :active-plans-order-seq-column            ; not in pre-submission workbooks
   #_                       :source-id                                ; not in pre-submission workbooks
   "requeststableid"        :requests-table-id
   "transferla"             :transfer-la
   "res"                    :res                                      ; <v1.2
   "wbp"                    :wbp                                      ; <v1.2
   "reviewmeeting"          :review-meeting                           ; ≥v1.2
   "reviewoutcome"          :review-outcome                           ; ≥v1.2
   "reviewdraft"            :review-draft                             ; ≥v1.3
   "phasetransferduedate"   :phase-transfer-due-date                  ; ≥v1.3
   "phasetransferfinaldate" :phase-transfer-final-date                ; ≥v1.3
   "lastreview"             :last-review})

(def active-plans-parser-fn
  "Parser function for SEN2 module 5a \"Active plans\"."
  {:person-table-id                   [:string parse-id]              ; not in SEN2 Blade CSV Export
   #_#_:active-plans-table-id         :string                         ; not in pre-submission workbooks
   #_#_:native-id                     :string                         ; not in pre-submission workbooks
   #_#_:active-plans-order-seq-column :int32                          ; not in pre-submission workbooks
   #_#_:source-id                     :string                         ; not in pre-submission workbooks
   :requests-table-id                 [:string parse-id]
   :transfer-la                       [:string parse-id]
   :res                               :string                         ; <v1.2
   :wbp                               :string                         ; <v1.2
   :review-meeting                    :local-date                     ; ≥v1.2
   :review-outcome                    :string                         ; ≥v1.2
   :review-draft                      :local-date                     ; ≥v1.3
   :phase-transfer-due-date           :local-date                     ; ≥v1.3
   :phase-transfer-final-date         :local-date                     ; ≥v1.3
   :last-review                       :local-date})

(def active-plans-read-cfg
  "Configuration map for reading SEN2 module 5a \"Active plans\" into a dataset."
  {:key-fn       #(or (active-plans-src-col-name->col-name %) %)
   :parser-fn    active-plans-parser-fn
   :dataset-name "active-plans"})

(def active-plans-col-name->label
  "Map SEN2 module 5a \"Active plans\" dataset column names to display labels."
  {:person-table-id                   "Person table ID"               ; not in SEN2 Blade CSV Export
   #_#_:active-plans-table-id         "Active plans table ID"         ; not in pre-submission workbooks
   #_#_:native-id                     "Native ID"                     ; not in pre-submission workbooks
   #_#_:active-plans-order-seq-column "Active plans order seq column" ; not in pre-submission workbooks
   #_#_:source-id                     "Source ID"                     ; not in pre-submission workbooks
   :requests-table-id                 "Requests table ID"
   :transfer-la                       "EHC plan transferred in from another LA during calendar year"
   :res                               "Residential settings"          ; <v1.2
   :wbp                               "Work-based learning activity"  ; <v1.2
   :review-meeting                    "Annual review meeting date"    ; ≥v1.2
   :review-outcome                    "Annual review decision"        ; ≥v1.2
   :review-draft                      "Annual review – date draft amended EHC plan issued"    ; ≥v1.3
   :phase-transfer-due-date           "Phase transfer review - due date for any amended plan" ; ≥v1.3
   :phase-transfer-final-date         "Phase transfer review - final plan date"               ; ≥v1.3
   :last-review                       "Annual review decision date"   ; ≥v1.2: "EHC plan review decisions date" <v1.2
   })


;;; ## Module 5b: Placements - Placement details (`placement-detail`)
(def placement-detail-src-col-name->col-name
  "Map SEN2 module 5b \"Placement details\" source data file column name to column name for the dataset."
  {"persontableid"               :person-table-id                     ; not in SEN2 Blade CSV Export
   "requeststableid"             :requests-table-id                   ; not in SEN2 Blade CSV Export
   #_                            :placement-detail-table-id           ; not in pre-submission workbooks
   #_                            :native-id                           ; not in pre-submission workbooks
   #_                            :placement-detail-order-seq-column   ; not in pre-submission workbooks
   #_                            :source-id                           ; not in pre-submission workbooks
   #_                            :active-plans-table-id               ; not in pre-submission workbooks
   "urn"                         :urn
   "ukprn"                       :ukprn
   "sensetting"                  :sen-setting
   "sensettingother"             :sen-setting-other
   "placementrank"               :placement-rank
   "entrydate"                   :entry-date
   "leavingdate"                 :leaving-date
   "attendancepattern"           :attendance-pattern                  ; <v1.2
   "senunitindicator"            :sen-unit-indicator
   "resourcedprovisionindicator" :resourced-provision-indicator
   "res"                         :res                                 ; ≥v1.2
   "wbp"                         :wbp                                 ; ≥v1.2
   })

(def placement-detail-parser-fn
  "Parser function for SEN2 module 5b \"Placement details\"."
  {:person-table-id                       [:string parse-id]          ; not in SEN2 Blade CSV Export
   :requests-table-id                     [:string parse-id]          ; not in SEN2 Blade CSV Export
   #_#_:placement-detail-table-id         :string                     ; not in pre-submission workbooks
   #_#_:native-id                         :string                     ; not in pre-submission workbooks
   #_#_:placement-detail-order-seq-column :int32                      ; not in pre-submission workbooks
   #_#_:source-id                         :string                     ; not in pre-submission workbooks
   #_#_:active-plans-table-id             :string                     ; not in pre-submission workbooks
   :urn                                   [:string parse-id]
   :ukprn                                 [:string parse-id]
   :sen-setting                           :string
   :sen-setting-other                     :string
   :placement-rank                        :int8
   :entry-date                            :local-date
   :leaving-date                          :local-date
   :attendance-pattern                    :string                     ; <v1.2
   :sen-unit-indicator                    :boolean
   :resourced-provision-indicator         :boolean
   :res                                   :string                     ; ≥v1.2
   :wbp                                   :string                     ; ≥v1.2
   })

(def placement-detail-read-cfg
  "Configuration map for reading SEN2 module 5b \"Placement details\" into a dataset."
  {:key-fn       #(or (placement-detail-src-col-name->col-name %) %)
   :parser-fn    placement-detail-parser-fn
   :dataset-name "placement-detail"})

(def placement-detail-col-name->label
  "Map SEN2 module 5b \"Placement details\" dataset column names to display labels."
  {:person-table-id                       "Person table ID"           ; not in SEN2 Blade CSV Export
   :requests-table-id                     "Requests table ID"         ; not in SEN2 Blade CSV Export
   #_#_:placement-detail-table-id         "Placement detail table ID" ; not in pre-submission workbooks
   #_#_:native-id                         "Native ID"                 ; not in pre-submission workbooks
   #_#_:placement-detail-order-seq-column "Placement detail order seq column" ; not in pre-submission workbooks
   #_#_:source-id                         "Source ID"                 ; not in pre-submission workbooks
   #_#_:active-plans-table-id             "Active plans table ID"     ; not in pre-submission workbooks
   :urn                                   "URN – Unique Reference Number"
   :ukprn                                 "UKPRN – UK Provider Reference Number"
   :sen-setting                           "SEN Setting - Establishment type"
   :sen-setting-other                     "Not in education – Other"
   :placement-rank                        "Placement rank"
   :entry-date                            "Placement start date"
   :leaving-date                          "Placement leaving date"
   :attendance-pattern                    "Attendance pattern"           ; <v1.2
   :sen-unit-indicator                    "SEN Unit indicator"
   :resourced-provision-indicator         "Resourced provision indicator"
   :res                                   "Residential settings"         ; ≥v1.2
   :wbp                                   "Work-based learning activity" ; ≥v1.2
   })


;;; ## Module 5c: Placements - SEN need (`sen-need`)
(def sen-need-src-col-name->col-name
  "Map SEN2 module 5c \"SEN need\" source data file column name to column name for the dataset."
  {"persontableid"   :person-table-id                                 ; not in SEN2 Blade CSV Export
   "requeststableid" :requests-table-id                               ; not in SEN2 Blade CSV Export
   #_                :sen-need-table-id                               ; not in pre-submission workbooks
   #_                :native-id                                       ; not in pre-submission workbooks
   #_                :sen-need-order-seq-column                       ; not in pre-submission workbooks
   #_                :source-id                                       ; not in pre-submission workbooks
   #_                :active-plans-table-id                           ; not in pre-submission workbooks
   "sentype"         :sen-type
   "sentyperank"     :sen-type-rank})

(def sen-need-parser-fn
  "Parser function for SEN2 module 5 \"SEN need\"."
  {:person-table-id               [:string parse-id]                  ; not in SEN2 Blade CSV Export
   :requests-table-id             [:string parse-id]                  ; not in SEN2 Blade CSV Export
   #_#_:sen-need-table-id         :string                             ; not in pre-submission workbooks
   #_#_:native-id                 :string                             ; not in pre-submission workbooks
   #_#_:sen-need-order-seq-column :int32                              ; not in pre-submission workbooks
   #_#_:source-id                 :string                             ; not in pre-submission workbooks
   #_#_:active-plans-table-id     :string                             ; not in pre-submission workbooks
   :sen-type                      :string                          
   :sen-type-rank                 :int8})

(def sen-need-read-cfg
  "Configuration map for reading SEN2 module 5 \"SEN need\" into a dataset."
  {:key-fn       #(or (sen-need-src-col-name->col-name %) %)
   :parser-fn    sen-need-parser-fn
   :dataset-name "sen-need"})

(def sen-need-col-name->label
  "Map SEN2 module 5c \"SEN need\" dataset column names to display labels."
  {:person-table-id               "Person table ID"                   ; not in SEN2 Blade CSV Export
   :requests-table-id             "Requests table ID"                 ; not in SEN2 Blade CSV Export
   #_#_:sen-need-table-id         "SEN need table ID"                 ; not in pre-submission workbooks
   #_#_:native-id                 "Native ID"                         ; not in pre-submission workbooks
   #_#_:sen-need-order-seq-column "SEN need order seq column"         ; not in pre-submission workbooks
   #_#_:source-id                 "Source ID"                         ; not in pre-submission workbooks
   #_#_:active-plans-table-id     "Active plans table ID"             ; not in pre-submission workbooks
   :sen-type                      "SEN type"
   :sen-type-rank                 "SEN type rank"})



;;; # Definitions and functions to read all modules
(def module-read-cfg
  "Map of configuration maps for reading each module into a dataset."
  {#_#_:sen2         sen2-read-cfg                                    ; not in pre-submission workbooks
   :person           person-read-cfg
   :requests         requests-read-cfg
   :assessment       assessment-read-cfg
   :named-plan       named-plan-read-cfg
   :plan-detail      plan-detail-read-cfg
   :active-plans     active-plans-read-cfg
   :placement-detail placement-detail-read-cfg
   :sen-need         sen-need-read-cfg})

(defn module-worksheets->ds-map
  "Read module datasets from `module-worksheets` map using `module-read-cfg'` specified via `opts` map,
   returning as map of datasets with same keys as the `module-worksheets'`."
  [module-worksheets & {:keys [module-read-cfg]
                        :or   {module-read-cfg module-read-cfg}
                        :as   opts}]
  (reduce-kv (fn [m k v] (assoc m k (worksheet->ds v (get module-read-cfg k)))) {} module-worksheets))

(defn pre-submission-workbook->ds-map
  "Read module datasets from SEN2 pre-submission workbook `pre-submission-workbook`
   using module sheets specified by `module-sheet-names` and `module-read-cfg` specified via `opts` map,
   returning as map of datasets with same keys as the `module-sheet-names`."
  [pre-submission-workbook & {:as opts}]
  (-> pre-submission-workbook
      (pre-submission-workbook->module-worksheets opts)
      (module-worksheets->ds-map opts)))

(defn pre-submission-workbook-filepath->ds-map
  "Read module datasets from SEN2 pre-submission workbook file at `pre-submission-workbook-filepath`
   using module sheets specified by `module-sheet-names` and `module-read-cfg` specified via `opts` map,
   returning as map of datasets with same keys as the `module-sheet-names`."
  [pre-submission-workbook-filepath & {:as opts}]
  (-> pre-submission-workbook-filepath
      filepath->workbook
      (pre-submission-workbook->ds-map opts)))



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
  {#_#_:sen2         sen2-src-col-name->col-name
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
  {#_#_:sen2         sen2-col-name->label
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
  ([] (parser-fn module-read-cfg))
  ([module-read-cfg] (reduce-kv (fn [m _ v] (merge m (:parser-fn v))) {} module-read-cfg)))

