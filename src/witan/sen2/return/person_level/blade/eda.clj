(ns witan.sen2.return.person-level.blade.eda
  "Functions to facilitate EDA of SEN2 Blade."
  (:require [clojure.set :as set]
            [clojure.string :as string]
            [nextjournal.clerk :as clerk]
            [tablecloth.api :as tc]))

;;; # Utilities
(def module-order
  "Map SEN2 return module key to order."
  (zipmap [:sen2 :person :requests :assessment :named-plan :plan-detail :active-plans :placement-detail :sen-need]
          (range)))



;;; # Dataset structure and distinct values.
(defn report-ds-column-info
  "Display column info for a SEN2 dataset `ds` with labels and source file column names."
  [ds col-name->label col-name->src-col-name]
  (clerk/table {::clerk/width #_:wide :full}
               (-> ds
                   (tc/info)
                   (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max])
                   (tc/map-columns :src-col-name [:col-name] col-name->src-col-name)
                   (tc/map-columns :col-label     [:col-name] col-name->label)
                   (tc/reorder-columns [:src-col-name :col-name :col-label])
                   (tc/rename-columns (merge {:col-name  "Column Name"
                                              :datatype  "Data Type"
                                              :n-valid   "# Valid"
                                              :n-missing "# Missing"
                                              :min       "Min"
                                              :max       "Max"}
                                             {:src-col-name "Source File Column Name"
                                              :col-name     "Dataset Column Name"
                                              :col-label    "Column Label"})))))

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
            (when (ds k) {k (frequencies (ds k))})))
   {}
   cols))

(defn- report-distinct-vals
  "Display distinct-values of columns `cols` of dataset `ds`."
  [ds cols]
  (clerk/table {::clerk/width :prose}
               (#(hash-map "Column name" (keys %) "Values with frequencies" (vals %))
                (distinct-vals-with-freq ds cols))))

(def default-module-cols-to-report-distinct-vals
  "Default map of columns to report distinct values for for each module."
  ;; Columns considered but excluded are retained in the code but ignored using #_.
  {:person           [:record-type                          ; template only
                      #_:person-table-id
                      :native-id
                      #_:person-order-seq-column
                      :source-id
                      :sen2-table-id
                      :surname
                      :forename
                      #_:person-birth-date
                      :gender-current                       ; <v1.2
                      :sex                                  ; ≥v1.2
                      :ethnicity
                      #_:postcode
                      #_:upn
                      #_:unique-learner-number
                      :upn-unknown]
   :requests         [:record-type                          ; template only
                      #_:requests-table-id
                      :native-id
                      #_:requests-order-seq-column
                      :source-id
                      #_:person-table-id
                      #_:received-date
                      :request-source                       ; ≥v1.3
                      :rya
                      #_:request-outcome-date
                      :request-outcome
                      :request-mediation
                      :request-tribunal
                      :exported]
   :assessment       [:record-type                          ; template only
                      #_:assessment-table-id
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
   :named-plan       [:record-type                          ; template only
                      #_:named-plan-table-id
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
   :plan-detail      [:record-type                          ; template only
                      #_:plan-detail-table-id
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
   :active-plans     [:record-type                          ; template only
                      #_:active-plans-table-id
                      :native-id
                      #_:active-plans-order-seq-column
                      :source-id
                      #_:requests-table-id
                      :transfer-la
                      :res                                  ; <v1.2
                      :wbp                                  ; <v1.2
                      #_:review-meeting                     ; ≥v1.2
                      :review-outcome                       ; ≥v1.2
                      #_:review-draft                       ; ≥v1.3
                      #_:phase-transfer-due-date            ; ≥v1.3
                      #_:phase-transfer-final-date          ; ≥v1.3
                      #_:last-review]
   :placement-detail [:record-type                          ; template only
                      #_:placement-detail-table-id
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
                      :attendance-pattern                   ; <v1.2
                      :sen-unit-indicator
                      :resourced-provision-indicator
                      :res                                  ; ≥v1.2
                      :wbp                                  ; ≥v1.2
                      ]
   :sen-need         [:record-type                          ; template only
                      #_:sen-need-table-id
                      :native-id
                      #_:sen-need-order-seq-column
                      :source-id
                      #_:active-plans-table-id
                      :sen-type-rank
                      :sen-type]})

(defn report-module-info
  "Report SEN2 Blade export `module-key` module dataset `ds` structure and distinct values (using clerk)."
  [& {:keys [module-key ds title src-col-name->col-name col-name->label cols-to-report-distinct-vals]}]
  (clerk/fragment
   (clerk/md (str "### " title))
   (clerk/md "Dataset structure:")
   (report-ds-column-info ds col-name->label (set/map-invert src-col-name->col-name))
   (if (= module-key :sen2)
     (clerk/fragment
      (clerk/md "Values:")
      (clerk/table {::clerk/width :prose}
                   (-> (tc/pivot->longer ds
                                         :all
                                         {:target-columns    "Column Name"
                                          :value-column-name "value"})
                       (tc/drop-columns [0]))))
     (clerk/fragment
      (clerk/md "Distinct values of selected categorical columns:")
      (report-distinct-vals ds cols-to-report-distinct-vals)))))

(defn report-all-module-info
  "Report SEN2 Blade export dataset structure and distinct values (using clerk) for all modules in `ds-map`."
  [ds-map & {:keys [module-titles module-col-name->label module-src-col-name->col-name module-cols-to-report-distinct-vals]
             :or   {module-cols-to-report-distinct-vals default-module-cols-to-report-distinct-vals}}]
  (clerk/fragment
   (map (fn [[k v]] (report-module-info {:module-key                   k
                                         :ds                           v
                                         :title                        (get module-titles k (name k))
                                         :col-name->label              (get module-col-name->label k {})
                                         :src-col-name->col-name       (get module-src-col-name->col-name k {})
                                         :cols-to-report-distinct-vals (get module-cols-to-report-distinct-vals k {})}))
        (into (sorted-map-by #(compare (module-order %1 %1) (module-order %2 %2))) ds-map))))



;;; # Relational database structure
;;; ## Schema
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


;;; ## COLLECT `:*-table-id`s
(defn report-collect-keys
  []
  (clerk/md (str "|  | table            | primary key                  | foreign key(s)           |\n"
                 "|:-|:-----------------|:-----------------------------|:-------------------------|\n"
                 "|0 | sen2             | `:sen2-table-id`             |                          |\n"
                 "|1 | person           | `:person-table-id`           | `:sen2-table-id`         |\n"
                 "|2 | requests         | `:requests-table-id`         | `:person-table-id`       |\n"
                 "|3 | assessment       | `:assessment-table-id`       | `:requests-table-id`     |\n"
                 "|4a| named-plan       | `:named-plan-table-id`       | `:assessment-table-id`   |\n"
                 "|4b| plan-detail      | `:plan-detail-table-id`      | `:named-plantable-id`    |\n"
                 "|5a| active-plans     | `:active-plans-table-id`     | `:requests-table-id`     |\n"
                 "|5b| placement-detail | `:placement-detail-table-id` | `:active-plans-table-id` |\n"
                 "|5c| sen-need         | `:sen-need-table-id`         | `:active-plans-table-id` |\n")))

(defn report-collect-key-relationship [ds-map module-key-child module-key-parent key-col expected-children-s]
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
    (clerk/md (str (format "### `%s` (child) ↗ `%s` (parent):\n" ds-child-name ds-parent-name)
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

(defn report-collect-key-relationships
  [ds-map]
  (clerk/fragment
   (report-collect-key-relationship ds-map :person           :sen2         :sen2-table-id         "")
   (report-collect-key-relationship ds-map :requests         :person       :person-table-id       "(Should be 1+  per 2023 SEN2 guide v1.0 Module 2 description on p18.)")
   (report-collect-key-relationship ds-map :assessment       :requests     :requests-table-id     "(Should be 0-1 per 2023 SEN2 guide v1.0 Module 3 description on p21.)")
   (report-collect-key-relationship ds-map :named-plan       :assessment   :assessment-table-id   "(Should be 0-1 per 2023 SEN2 guide v1.0 Module 3 description on p21 & Module 4 description on p24.)")
   (report-collect-key-relationship ds-map :plan-detail      :named-plan   :named-plan-table-id   "(Should be 1-2 per 2023 SEN2 guide v1.0 Module 4 <PlanDetail> description on p26.)")
   (report-collect-key-relationship ds-map :active-plans     :requests     :requests-table-id     "(Should be 0-1 per 2023 SEN2 guide v1.0 Module 5 <ActivePlans> description on p28.)")
   (report-collect-key-relationship ds-map :placement-detail :active-plans :active-plans-table-id "(Should be 1+  per 2023 SEN2 guide v1.0 Module 5 <PlacementDetail> description on p29.)")
   (report-collect-key-relationship ds-map :sen-need         :active-plans :active-plans-table-id "(Should be 1-2 per 2023 SEN2 guide v1.0 Module 5 <SENtype> description on p31.)")))

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
                (-> table-id-ds
                    (tc/update-columns :all (partial map #(if % "+" " ")))
                    (tc/unique-by)
                    (tc/rename-columns #(clojure.string/replace % #"^:(.*)-table-id$" "$1"))
                    (tc/order-by ["sen2" "person" "requests"
                                  "active-plans" "sen-need" "placement-detail"
                                  "assessment" "named-plan" "plan-detail"])))))



;;; # `:person-table-id` & `:requests-table-id``
(defn report-table-keys
  []
  (clerk/md (str "| table               | primary key                             | foreign key(s)                          |\n"
                 "|:--------------------|:----------------------------------------|:----------------------------------------|\n"
                 "| 1:person            | `:person-table-id`                      |                                         |\n"
                 "| 2:requests          | `:requests-table-id`                    | `:person-table-id`                      |\n"
                 "| 3:~~assessment~~    | --                                      | --                                      |\n"
                 "| 4a:named-plan       | `:requests-table-id`                    | `:person-table-id`                      |\n"
                 "| 4b:~~plan-detail~~  | --                                      | --                                      |\n"
                 "| 5a:active-plans     | `:requests-table-id`                    | `:person-table-id`                      |\n"
                 "| 5b:placement-detail | ??                                      | `:person-table-id` `:requests-table-id` |\n"
                 "| 5c:sen-need         | [`:requests-table-id` `:sen-type-rank`] | `:person-table-id` `:requests-table-id` |\n")))

(defn report-key-relationship [ds-map module-key]
  (let [ds                          (get ds-map module-key)
        ds-name                     (tc/dataset-name ds)
        row-count                   (tc/row-count ds)
        num-unique-person-id        (-> ds
                                        (tc/unique-by [:person-table-id])
                                        (tc/row-count))
        num-unknown-person          (-> ds
                                        (tc/unique-by [:person-table-id])
                                        (tc/anti-join (:person ds-map) [:person-table-id])
                                        (tc/row-count))
        num-records-per-person      (-> ds
                                        (tc/group-by [:person-table-id])
                                        (tc/aggregate {:row-count tc/row-count})
                                        (tc/right-join (-> (:person ds-map)
                                                           (tc/select-columns [:person-table-id])
                                                           (tc/set-dataset-name "parent"))
                                                       [:person-table-id])
                                        (tc/replace-missing :row-count :value 0)
                                        (tc/drop-columns [:person-table-id])
                                        (tc/rename-columns {(keyword (str "parent." (name :person-table-id))) :person-table-id})
                                        (tc/reorder-columns [:person-table-id]))
        min-num-records-per-person  (reduce min (:row-count num-records-per-person ))
        max-num-records-per-person  (reduce max(:row-count num-records-per-person ))
        num-records-per-request     (-> ds
                                        (tc/group-by [:person-table-id :requests-table-id])
                                        (tc/aggregate {:row-count tc/row-count})
                                        (tc/right-join (-> (:requests ds-map)
                                                           (tc/select-columns [:person-table-id :requests-table-id])
                                                           (tc/set-dataset-name "parent"))
                                                       [:person-table-id :requests-table-id])
                                        (tc/replace-missing :row-count :value 0)
                                        (tc/drop-columns [:person-table-id :requests-table-id])
                                        (tc/rename-columns {:parent.person-table-id   :person-table-id
                                                            :parent.requests-table-id :requests-table-id})
                                        (tc/reorder-columns [:person-table-id :requests-table-id]))
        min-num-records-per-request (reduce min (:row-count num-records-per-request))
        max-num-records-per-request (reduce max(:row-count num-records-per-request ))]
    (clerk/md (str (format "`%s`:\n" ds-name)
                   (format "- Has %,d rows on %,d distinct `:person-table-id`,  \n" row-count num-unique-person-id)
                   (format " %s in the `person` dataset.\n" (if (= 0 num-unknown-person)
                                                              "all of which are"
                                                              (format "**%,d** of which are **NOT**" num-unknown-person)))
                   (format "- Each `:person-table-id` in `person` table is present %d - %d times in `%s`.\n"
                           min-num-records-per-person max-num-records-per-person ds-name)
                   (format "- Each [`:person-table-id` `:requests-table-id`] in `requests` table  \noccurs %d - %d times in `%s`."
                           min-num-records-per-request max-num-records-per-request ds-name)))))

(defn report-key-relationships
  [ds-map]
  (clerk/fragment
   (clerk/md (str "Q: For `named-plan`, `placement-detail` & `sen-need`:\n"
                  "- How many rows, on how many `:person-table-id`s are there?  \n"
                  "…and are all `:person-table-id`s in the `person` table?\n"
                  "- How many times does a `:person-table-id` from the `person` table appear?\n"
                  "- How many times do distinct [`:person-table-id` `:requests-table-id`] from the `requests` table appear?\n"))
   (report-key-relationship ds-map :named-plan)
   (report-key-relationship ds-map :placement-detail)
   (report-key-relationship ds-map :sen-need)))



;;; # Dataset keys
(defn report-unique-key [ds key-cols]
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

(defn report-unique-keys
  [ds-map]
  (let [person       (-> (ds-map :person))
        named-plan   (-> (ds-map :named-plan))
        active-plans (-> (ds-map :active-plans))
        sen-need     (-> (ds-map :sen-need))]
    (clerk/fragment
     (report-unique-key person       [:person-table-id])
     (report-unique-key named-plan   [:person-table-id :requests-table-id])
     (report-unique-key named-plan   [                 :requests-table-id])
     (report-unique-key named-plan   [:person-table-id])
     (report-unique-key active-plans [:person-table-id :requests-table-id])
     (report-unique-key active-plans [                 :requests-table-id])
     (report-unique-key active-plans [:person-table-id])
     (report-unique-key sen-need     [:person-table-id :requests-table-id :sen-type-rank])
     (report-unique-key sen-need     [                 :requests-table-id :sen-type-rank])
     (report-unique-key sen-need     [:person-table-id                    :sen-type-rank]))))

