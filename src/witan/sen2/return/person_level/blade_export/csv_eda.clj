(ns witan.sen2.return.person-level.blade-export.csv-eda
  "Functions to facilitate EDA of datasets read from SEN2 COLLECT Blade CSV Export files."
  (:require [clojure.string :as string]
            [nextjournal.clerk :as clerk]
            [tablecloth.api :as tc]
            [witan.sen2.return.person-level.blade-export.csv :as sen2-blade-csv]))



;;; # Dataset structure and distinct values.
(defn report-csv-ds-column-info
  "Display column info for a sen2 dataset `ds` read from CSV file."
  [ds col-name->csv-label col-name->label]
  (clerk/table {::clerk/width #_:wide :full}
               (-> ds
                   (tc/info)
                   (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max])
                   (tc/map-columns :csv-col-label [:col-name] col-name->csv-label)
                   (tc/map-columns :col-label     [:col-name] col-name->label)
                   (tc/reorder-columns [:csv-col-label :col-name :col-label])
                   (tc/rename-columns (merge {:col-name  "Column Name"
                                              :datatype  "Data Type"
                                              :n-valid   "# Valid"
                                              :n-missing "# Missing"
                                              :min       "Min"
                                              :max       "Max"}
                                             {:csv-col-label "CSV File Column Name"
                                              :col-name      "Dataset Column Name"
                                              :col-label     "Column Label"})))))

(defn- distinct-vals 
  "Returns map with column names from `cols` as keys and sorted set of values of that column in `ds` as value."
  [ds cols]
  (reduce (fn [m k] (merge m {k (into (sorted-set) (ds k))})) {} cols))

(defn- distinct-vals-with-freq 
  "Returns map with column names from `cols` as keys and sorted set of values of that column in `ds` as value."
  [ds cols]
  (reduce 
   (fn [m k] 
     (merge m 
            {k (frequencies (ds k))}))
   {}
   cols))

(defn- report-distinct-vals
  "Display distinct-values of columns `cols` of dataset `ds`."
  [ds cols]
  (clerk/table {::clerk/width :prose}
               (#(hash-map "Column name" (keys %) "Values" (vals %))
                (distinct-vals-with-freq ds cols))))

(defn report-csv-ds-info
  "Report SEN2 Blade CSV Export `module-key` module dataset `ds` structure and distinct values (using clerk)."
  [ds module-key]
  (let [title                        (get {:sen2             "0: SEN2 metadata (`sen2`)"
                                           :person           "1: Person details (`person`)"
                                           :requests         "2: Requests (`requests`)"
                                           :assessment       "3: EHC needs assessments (`assessment`)"
                                           :named-plan       "4a: Named plan (`named-plan`)"
                                           :plan-detail      "4b: Plan detail records (`plan-detail`)"
                                           :active-plans     "5a: Placements - Active plans (`active-plans`)"
                                           :placement-detail "5b: Placements - Placement details (`placement-detail`)"
                                           :sen-need         "5c: Placements - SEN need (`sen-need`)"}
                                          module-key)
        col-name->csv-label          (get {:sen2             sen2-blade-csv/sen2-col-name->csv-label
                                           :person           sen2-blade-csv/person-col-name->csv-label
                                           :requests         sen2-blade-csv/requests-col-name->csv-label
                                           :assessment       sen2-blade-csv/assessment-col-name->csv-label
                                           :named-plan       sen2-blade-csv/named-plan-col-name->csv-label
                                           :plan-detail      sen2-blade-csv/plan-detail-col-name->csv-label
                                           :active-plans     sen2-blade-csv/active-plans-col-name->csv-label
                                           :placement-detail sen2-blade-csv/placement-detail-col-name->csv-label
                                           :sen-need         sen2-blade-csv/sen-need-col-name->csv-label}
                                          module-key)
        col-name->label              (get {:sen2             sen2-blade-csv/sen2-col-name->label
                                           :person           sen2-blade-csv/person-col-name->label
                                           :requests         sen2-blade-csv/requests-col-name->label
                                           :assessment       sen2-blade-csv/assessment-col-name->label
                                           :named-plan       sen2-blade-csv/named-plan-col-name->label
                                           :plan-detail      sen2-blade-csv/plan-detail-col-name->label
                                           :active-plans     sen2-blade-csv/active-plans-col-name->label
                                           :placement-detail sen2-blade-csv/placement-detail-col-name->label
                                           :sen-need         sen2-blade-csv/sen-need-col-name->label}
                                          module-key)
        cols-to-report-distinct-vals (get {:person           [#_:person-table-id
                                                              :native-id
                                                              #_:person-order-seq-column
                                                              :source-id
                                                              :sen2-table-id
                                                              :surname
                                                              :forename
                                                              #_:person-birth-date
                                                              :gender-current
                                                              #_:ethnicity
                                                              #_:postcode
                                                              #_:upn
                                                              #_:unique-learner-number
                                                              :upn-unknown]
                                           :requests         [#_:requests-table-id
                                                              :native-id
                                                              #_:requests-order-seq-column
                                                              :source-id
                                                              #_:person-table-id
                                                              #_:received-date
                                                              :rya
                                                              #_:request-outcome-date
                                                              :request-outcome
                                                              :request-mediation
                                                              :request-tribunal
                                                              :exported] 
                                           :assessment       [#_:assessment-table-id
                                                              :native-id
                                                              #_:assessment-order-seq-column
                                                              :source-id
                                                              #_:requests-table-id
                                                              :assessment-outcome
                                                              #_:assessment-outcome-date
                                                              :assessment-mediation
                                                              :assessment-tribunal
                                                              :other-mediation
                                                              :other-tribunal
                                                              :week20]
                                           :named-plan       [#_:named-plan-table-id
                                                              :native-id
                                                              #_:named-plan-order-seq-column
                                                              :source-id
                                                              #_:assessment-table-id
                                                              #_:start-date
                                                              :plan-res
                                                              :plan-wbp
                                                              :pb
                                                              :oa
                                                              :dp
                                                              #_:cease-date
                                                              :cease-reason]
                                           :plan-detail      [#_:plan-detail-table-id
                                                              :native-id
                                                              #_:plan-detail-order-seq-column
                                                              :source-id
                                                              #_:named-plan-table-id
                                                              #_:urn
                                                              #_:ukprn
                                                              :sen-setting
                                                              :sen-setting-other
                                                              :placement-rank
                                                              :sen-unit-indicator
                                                              :resourced-provision-indicator]
                                           :active-plans     [#_:active-plans-table-id
                                                              :native-id
                                                              #_:active-plans-order-seq-column
                                                              :source-id
                                                              #_:requests-table-id
                                                              :transfer-la
                                                              :res
                                                              :wbp
                                                              #_:last-review]
                                           :placement-detail [#_:placement-detail-table-id
                                                              :native-id
                                                              #_:placement-detail-order-seq-column
                                                              :source-id
                                                              #_:active-plans-table-id
                                                              #_:urn
                                                              #_:ukprn
                                                              :sen-setting
                                                              :sen-setting-other
                                                              :placement-rank
                                                              #_:entry-date
                                                              #_:leaving-date
                                                              :attendance-pattern
                                                              :sen-unit-indicator
                                                              :resourced-provision-indicator]
                                           :sen-need         [#_:sen-need-table-id
                                                              :native-id
                                                              #_:sen-need-order-seq-column
                                                              :source-id
                                                              #_:active-plans-table-id
                                                              :sen-type-rank
                                                              :sen-type]}
                                          module-key)]
    (clerk/fragment
     (clerk/md (str "### " title))
     (clerk/md "Dataset structure:")
     (report-csv-ds-column-info ds col-name->csv-label col-name->label)
     (if (= module-key :sen2)
       (clerk/fragment
        (clerk/md "Values:")
        (clerk/table {::clerk/width :prose}
                     (-> (tc/pivot->longer ds)
                         (tc/rename-columns {:$column "Column Name"
                                             :$value  "value"}))))
       (clerk/fragment
        (clerk/md "Distinct values of selected categorical columns:")
        (report-distinct-vals ds cols-to-report-distinct-vals))))))

(defn report-csv-ds-map-info
  "Report SEN2 Blade CSV Export `module-key` module dataset structure and distinct values (using clerk) for dataset from `ds-map`"
  [ds-map module-key]
  (report-csv-ds-info (get ds-map module-key) module-key))

(defn report-csv-ds-map-info-all
  "Report SEN2 Blade CSV Export dataset structure and distinct values (using clerk) for all datasets in `ds-map`"
  [ds-map]
  (clerk/fragment
   (map (partial report-csv-ds-map-info ds-map)
        [:sen2 :person :requests :assessment :named-plan :plan-detail :active-plans :placement-detail :sen-need])))



;;; # Dataset keys
(defn report-unique-key? [ds key-cols]
  (clerk/md (let [key-str        (str "[`:" ((comp (partial clojure.string/join "`, :`") (partial map name)) key-cols) "`]")
                  ds-name        (tc/dataset-name ds)
                  num-non-unique (-> ds
                                     (tc/group-by key-cols)
                                     (tc/aggregate {:num-rows tc/row-count})
                                     (tc/select-rows #(not= 1 (:num-rows %)))
                                     (tc/row-count))]
              (format (str "Q: Considering %s:  \nis %s a unique key?  \n"
                           "A: " (if (zero? num-non-unique) "Yes" "NO") ": "
                           "There are %,d combinations with more than one record.")
                      ds-name
                      key-str
                      num-non-unique))))

(defn report-composite-keys
  [ds-map]
  (let [table-id-ds  (sen2-blade-csv/->table-id-ds ds-map)
        named-plan   (-> (ds-map :named-plan)
                         (tc/left-join (sen2-blade-csv/ancestor-table-id-ds table-id-ds :named-plan-table-id) [:assessment-table-id])
                         (tc/set-dataset-name "`named-plan` (with ancestor `table-id`s)"))
        active-plans (-> (ds-map :active-plans)
                         (tc/left-join (sen2-blade-csv/ancestor-table-id-ds table-id-ds :active-plans-table-id) [:requests-table-id])
                         (tc/set-dataset-name "`active-plans` (with ancestor `table-id`s)"))
        sen-need     (-> (ds-map :sen-need)
                         (tc/left-join (sen2-blade-csv/ancestor-table-id-ds table-id-ds :sen-need-table-id) [:active-plans-table-id])
                         (tc/set-dataset-name "`sen-need` (with ancestor `table-id`s)"))]
    (clerk/fragment
     (report-unique-key? named-plan   [:person-table-id :requests-table-id])
     (report-unique-key? named-plan   [:person-table-id])
     (report-unique-key? active-plans [:person-table-id :requests-table-id])
     (report-unique-key? active-plans [:person-table-id])
     (report-unique-key? sen-need     [:person-table-id :requests-table-id :sen-type-rank])
     (report-unique-key? sen-need     [:person-table-id :sen-type-rank]))))



;;; # Relational database structure: schema and `table-id` key relationships
(defn report-expected-schema
  []
  (clerk/fragment
   (clerk/md "Drawn using [asciiflow](https://asciiflow.com/#/share/eJytUkFuwjAQ%2FEq0V7AUm6aHfYsvi7MSSIlLsy4CIW59QhX%2BwbHqa3hJ7VSibiFRD135sJ4Zr0djH8BTy4Ag7I1ysi3ErbglmENDe%2B4ic7Cws4C6LBdzC%2FvYmkcTu8C7EDcWiqxKTIOs9cWPuvTnW%2Bzt4xemccOdPPm%2FHtezLzS291f%2FbrDj5xeWIJfT65juOqUYrVxVKj2tWiCJsEjLPkxM6s%2F%2FcF8ko%2FCBML1jrTYN%2BZzOs6gIyYX1lgfVdyATdsYyye1pZW5ZPbt%2FKoqT3SUmC6rmQOtm4KoBcpwyu%2BKVS99JeebawhGOn8S42w8%3D%29):")
   (clerk/md (str " ```                                                                          \n"
                  "                            0:sen2                                            \n"
                  "                              ^                                               \n"
                  "                              │                                               \n"
                  "                            1:person                                          \n"
                  "                              ^                                               \n"
                  "                              │1+                                             \n"
                  "                   ┌───────>2:requests<───────┐                               \n"
                  "                   │                          │                               \n"
                  "                   │0-1                       │                               \n"
                  "                 3:assessment                 │                               \n"
                  "                   ^                          │                               \n"
                  "                   │0-1                       │0-1                            \n"
                  "                4a:named-plan         ┌───>5a:active-plans<─┐                 \n"
                  "                   ^                  │                     │                 \n"
                  "                   │1-2               │1+                   │1-2              \n"
                  "                4b:plan-detail     5b:placement-detail   5c:sen-need          \n"
                  " ```                                                                          \n"))
   (clerk/md (str "Ranges on relationships indicate _expected_ number of children "
                  "(according to [2023 SEN2 guide v1.0]"
                  "(https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1099346/2023_SEN2_Person_level_-_Guide_Version_1.0.pdf))."))))

(defn report-table-keys
  []
  (clerk/md (str "| table            | primary key                  | foreign key(s)           |\n"
                 "|:-----------------|:-----------------------------|:-------------------------|\n"
                 "| sen2             | `:sen2-table-id`             |                          |\n"
                 "| person           | `:person-table-id`           | `:sen2-table-id`         |\n"
                 "| requests         | `:requests-table-id`         | `:person-table-id`       |\n"
                 "| assessment       | `:assessment-table-id`       | `:requests-table-id`     |\n"
                 "| named-plan       | `:named-plan-table-id`       | `:assessment-table-id`   |\n"
                 "| plan-detail      | `:plan-detail-table-id`      | `:named-plantable-id`    |\n"
                 "| active-plans     | `:active-plans-table-id`     | `:requests-table-id`     |\n"
                 "| placement-detail | `:placement-detail-table-id` | `:active-plans-table-id` |\n"
                 "| sen-need         | `:sen-need-table-id`         | `:active-plans-table-id` |\n")))

(defn report-key-relationship [ds-map module-key-child module-key-parent key-col expected-children-s]
  (let [ds-child                  (get ds-map module-key-child)
        ds-parent                 (get ds-map module-key-parent)
        ds-child-name             (name module-key-child)
        ds-parent-name            (name module-key-parent)
        ds-parent-non-unique-keys (-> ds-parent
                                      (tc/group-by [key-col])
                                      (tc/aggregate {:num-rows tc/row-count})
                                      (tc/select-rows #(not= 1 (:num-rows %))))
        unique-child-keys         (-> ds-child
                                      (tc/select-columns [key-col])
                                      (tc/unique-by))
        num-unique-child-key-vals (-> unique-child-keys
                                      (tc/row-count))
        num-orphan-child-keys     (-> unique-child-keys
                                      (tc/anti-join ds-parent [key-col])
                                      (tc/row-count))
        num-childless-parent-keys (-> ds-parent
                                      (tc/anti-join ds-child [key-col])
                                      (tc/row-count))
        num-key-repeats           (-> ds-child
                                      (tc/group-by [key-col])
                                      (tc/aggregate {:num-key-repeats tc/row-count})
                                      (tc/right-join (-> ds-parent
                                                         (tc/select-columns [key-col])
                                                         (tc/set-dataset-name "parent"))
                                                     [key-col])
                                      (tc/replace-missing :num-key-repeats :value 0)
                                      (tc/drop-columns [key-col])
                                      (tc/rename-columns {(keyword (str "parent." (name key-col))) key-col})
                                      (tc/reorder-columns [key-col]))
        min-num-key-repeats       (reduce min (:num-key-repeats num-key-repeats))
        max-num-key-repeats       (reduce max(:num-key-repeats num-key-repeats))]
    (clerk/md (str (format "### `%s` (child) -> `%s` (parent):\n" ds-child-name ds-parent-name)
                   (format "- `%s` (parent) has %,d rows\n" ds-parent-name (tc/row-count ds-parent))
                   (format "- `%s` (child) has %,d rows\n" ds-child-name (tc/row-count ds-child))
                   (format "- `%s` %s unique key for (parent) table `%s`\n"
                           key-col (if (= 0 (tc/row-count ds-parent-non-unique-keys)) "is a" "is **not**") ds-parent-name)
                   (if (= 0 num-orphan-child-keys)
                     (format "- All %,d unique `%s` in (child) table `%s`'s are in (parent) table `%s`.\n"
                             num-unique-child-key-vals key-col ds-child-name ds-parent-name)
                     (format "- **%,d unique `%s` (of %,d) in (child) table `%s` are NOT in (parent) table `%s` (orphans)!\n"
                             num-orphan-child-keys key-col num-unique-child-key-vals ds-child-name ds-parent-name))
                   (if (= 0 num-childless-parent-keys)
                     (format "- All %,d `%s` in (parent) table `%s` are in (child) table `%s`.\n"
                             (tc/row-count ds-parent) key-col ds-parent-name ds-child-name)
                     (format "- **%,d** (of the %,d) `%s` in (parent) table `%s` are **NOT** not in (child) table `%s` (childless)!\n"
                             num-childless-parent-keys (tc/row-count ds-parent) key-col ds-parent-name ds-child-name))
                   (format "- Each key `%s` in parent table `%s` is present %d - %d times in child table `%s`. %s"
                           key-col ds-parent-name min-num-key-repeats max-num-key-repeats ds-child-name expected-children-s)))))

(defn report-key-relationships
  [ds-map]
  (clerk/fragment
   (report-key-relationship ds-map :person           :sen2         :sen2-table-id         "")
   (report-key-relationship ds-map :requests         :person       :person-table-id       "(Should be 1+  per 2023 SEN2 guide v1.0 Module 2 description on p18.)")
   (report-key-relationship ds-map :assessment       :requests     :requests-table-id     "(Should be 0-1 per 2023 SEN2 guide v1.0 Module 3 description on p21.)")
   (report-key-relationship ds-map :named-plan       :assessment   :assessment-table-id   "(Should be 0-1 per 2023 SEN2 guide v1.0 Module 3 description on p21 & Module 4 description on p24.)")
   (report-key-relationship ds-map :plan-detail      :named-plan   :named-plan-table-id   "(Should be 1-2 per 2023 SEN2 guide v1.0 Module 4 <PlanDetail> description on p26.)")
   (report-key-relationship ds-map :active-plans     :requests     :requests-table-id     "(Should be 0-1 per 2023 SEN2 guide v1.0 Module 5 <ActivePlans> description on p28.)")
   (report-key-relationship ds-map :placement-detail :active-plans :active-plans-table-id "(Should be 1+  per 2023 SEN2 guide v1.0 Module 5 <PlacementDetail> description on p29.)")
   (report-key-relationship ds-map :sen-need         :active-plans :active-plans-table-id "(Should be 1-2 per 2023 SEN2 guide v1.0 Module 5 <SENtype> description on p31.)")))



;;; # table-id's dataset
(defn report-table-id-ds
  [table-id-ds]
  (clerk/fragment
   (clerk/md (str "`table-id-ds` is a dataset of `:*table-id` key relationships, "
                  "which permits traversing up each branch of the dataset hierarchy "
                  "without going through all intermediate datasets:"))
   (clerk/table {::clerk/width :prose}
                (-> (tc/info table-id-ds)
                    (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max])
                    (tc/rename-columns {:col-name  "Column Name"
                                        :datatype  "Data Type"
                                        :n-valid   "# Valid"
                                        :n-missing "# Missing"
                                        :min       "Min"
                                        :max       "Max"})))
   (clerk/md (str "Note pattern of non-nil `:.*-table-id` keys in dataset reflects the hierarchy, "
                  "as the dataset is designed to facilitate traversing up the hierarchy, "
                  "going from a child key up to to ancestor keys (from right to left):"))
   (clerk/table {::clerk/width :full}
                (as-> table-id-ds $
                  (tc/add-columns $ (update-vals (tc/columns $ :as-map) (fn [v] (map #(if % "+" " ") v))))
                  (tc/unique-by $)
                  (tc/rename-columns $ (into {} (map (fn [k] [k
                                                              (clojure.string/replace (name k) #"-table-id$" "")])
                                                     (tc/column-names $))))))))
