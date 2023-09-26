(ns witan.sen2.return.person-level.blade-export.csv.plans-placements
  "Tools to extract and manipulate plans & placements open on census dates
   (with person details and EHCP primary need)
  from SEN2 person level return COLLECT Blade Export CSV files"
  (:require [tablecloth.api :as tc]
            [witan.sen2 :as sen2]
            [witan.sen2.ncy :as ncy]
            [witan.sen2.return.person-level.blade-export.csv :as sen2-blade-csv]))




;;; # Extract SEN2 information on census dates

(defn select-episodes-open-on-census-dates
  "Select records from `episodes-ds` that are open for each `:census-date` in `census-dates-ds`,
  where episode start and end dates are in columns `episode-start-date-col` & `episode-end-date-col`."
  [episode-ds census-dates-ds episode-start-date-col episode-end-date-col]
  (-> episode-ds
      (tc/cross-join census-dates-ds)
      (tc/select-rows #(and (or (nil?      (% episode-start-date-col))
                                (.isEqual  (% episode-start-date-col) (:census-date %))
                                (.isBefore (% episode-start-date-col) (:census-date %)))
                            (or (nil?      (% episode-end-date-col))
                                (.isEqual  (:census-date %) (% episode-end-date-col))
                                (.isBefore (:census-date %) (% episode-end-date-col)))))))



;;; ## Person information

(defn person-on-census-dates
  "Selected columns from `person` table with record for each of `census-dates-ds`,
   with age at the start of school year containing the `:census-date` and corresponding nominal NCY."
  [sen2-blade-csv-ds-map census-dates-ds]
  (-> (:person sen2-blade-csv-ds-map)
      (tc/select-columns [:sen2-table-id :person-table-id :person-order-seq-column :upn :unique-learner-number :person-birth-date])
      (tc/cross-join census-dates-ds)
      (tc/map-columns :age-at-start-of-school-year [:census-date :person-birth-date] ncy/age-at-start-of-school-year-for-date)
      (tc/map-columns :nominal-ncy [:age-at-start-of-school-year] ncy/age-at-start-of-school-year->ncy)
      (tc/set-dataset-name "Person ages and nominal NCYs on census dates")))

(defn person-on-census-dates-col-name->label
  "Column labels for display."
  ([]
   (merge sen2-blade-csv/person-col-name->label
          (sen2/census-dates-col-name->label)
          {:age-at-start-of-school-year "Age at start of school year"
           :nominal-ncy                 "Nominal NCY for age"}))
  ([ds]
   (-> (person-on-census-dates-col-name->label)
       (select-keys (tc/column-names ds)))))



;;; ## `named-plan`s open on census dates

(defn named-plan-on-census-dates
  "All `named-plan` records open on census dates, with ancestor table IDs.

  Note that open `named-plan`s are identified based on their `:start-date` and `:cease-date`:
  - The `:start-date` is the date of the EHC plan,
    which may be before the EHC plan transferred in.
  - The `:cease-date` is the date the EHC plan ended
    or the date the EHC plan was transferred to another LA.
  - Thus for a census date not at the end of the SEN2 collection period,
    this may include plans that were open with another LA (prior to
    transferring in later during the collection year).
  "
  [sen2-blade-csv-ds-map census-dates-ds]
  (-> (sen2-blade-csv-ds-map :named-plan)
      (select-episodes-open-on-census-dates census-dates-ds :start-date :cease-date)
      (tc/left-join (sen2-blade-csv/ds-map->ancestor-table-id-ds sen2-blade-csv-ds-map :named-plan-table-id)
                    [:assessment-table-id])
      (tc/drop-columns [:table-id-ds.assessment-table-id])
      (tc/order-by        (concat sen2-blade-csv/table-id-col-names (tc/column-names census-dates-ds)))
      (tc/reorder-columns (concat sen2-blade-csv/table-id-col-names (tc/column-names census-dates-ds)))
      (tc/set-dataset-name "named-plan-on-census-dates")))

(defn named-plan-on-census-dates-col-name->label
  "Column labels for display."
  ([]
   (merge sen2-blade-csv/table-id-col-name->label
          (sen2/census-dates-col-name->label)
          sen2-blade-csv/named-plan-col-name->label))
  ([ds]
   (-> (named-plan-on-census-dates-col-name->label)
       (select-keys (tc/column-names ds)))))



;;; ## `placement-detail`s open on census dates

(defn placement-detail-on-census-dates
  "All `placement-detail` records open on census dates, with ancestor table IDs.
  
  Some `:person-table-id` will have a `:requests-table-id` with both
  `:placement-rank` 1 and `:placement-rank` 2 placements open on a census date,
  and there may be some CYP with only a `:placement-rank` 2 placement
  open on a `:census-date`.

  To permit selection of the placement with highest rank open on a census date,
  column `:census-date-placement-idx` is added to the dataset, indexing
  placements by [`:person-table-id` `:requests-table-id` `:census-date`]
  in `:placement-rank` order: this is zero for the placement with highest rank
  open on the `:census-date`.

  Note that taking rank 2 placements for analysis may result in
  transitions from a rank 1 placement to a rank 2 placement if a rank
  1 placement open at a census date ends before the next census-date
  but the rank 2 placement continues beyond it.
  "
  [sen2-blade-csv-ds-map census-dates-ds]
  (-> (sen2-blade-csv-ds-map :placement-detail)
      (select-episodes-open-on-census-dates census-dates-ds :entry-date :leaving-date )
      (tc/left-join (sen2-blade-csv/ds-map->ancestor-table-id-ds sen2-blade-csv-ds-map :placement-detail-table-id)
                    [:active-plans-table-id])
      (tc/drop-columns [:table-id-ds.active-plans-table-id])
      ;; Add `:census-date-placement-idx` to index multiple placements in `:placement-rank` order.
      (tc/order-by [:person-table-id :requests-table-id :census-date :placement-rank])
      (tc/group-by [:person-table-id :requests-table-id :census-date])
      (tc/add-column :census-date-placement-idx (range))
      (tc/ungroup)
      ;; Order
      (tc/order-by        (concat sen2-blade-csv/table-id-col-names (tc/column-names census-dates-ds) [:placement-rank]))
      (tc/reorder-columns (concat sen2-blade-csv/table-id-col-names (tc/column-names census-dates-ds) [:census-date-placement-idx]))
      (tc/set-dataset-name "placement-detail-on-census-dates")))

(defn placement-detail-on-census-dates-col-name->label
  "Column labels for display."
  ([]
   (merge sen2-blade-csv/table-id-col-name->label
          (sen2/census-dates-col-name->label)
          {:census-date-placement-idx "Census date placement index"}
          sen2-blade-csv/placement-detail-col-name->label))
  ([ds]
   (-> (placement-detail-on-census-dates-col-name->label)
       (select-keys (tc/column-names ds)))))



;;; ## EHCP primary need from `sen-need`

(defn sen-need-primary
  "Primary SEN need from `sen-need`, where primary is `:sen-type-rank`=1, with ancestor table IDs."
  [sen2-blade-csv-ds-map] 
  (-> (sen2-blade-csv-ds-map :sen-need)
      (tc/select-rows #(= 1 (:sen-type-rank %)))
      (tc/left-join (sen2-blade-csv/ds-map->ancestor-table-id-ds sen2-blade-csv-ds-map :sen-need-table-id)
                    [:active-plans-table-id])
      (tc/drop-columns [:table-id-ds.active-plans-table-id])
      (tc/order-by        sen2-blade-csv/table-id-col-names)
      (tc/reorder-columns sen2-blade-csv/table-id-col-names)
      (tc/set-dataset-name "sen-need-primary")))

(defn sen-need-primary-col-name->label
  "Column labels for display."
  ([]
   (merge sen2-blade-csv/table-id-col-name->label
          sen2-blade-csv/sen-need-col-name->label))
  ([ds]
   (-> (sen-need-primary-col-name->label)
       (select-keys (tc/column-names ds)))))




;;; # Collate

;; Collate open `named-plan`s with open primary `placement-detail`,
;; add primary `sen-need`, `active-plans` (for `:transfer-la`) and `person` age/NCY details.
;; Note merge order:
;; - Merging `named-plan-on-census-dates` with highest ranking `placement-detail-on-census-dates` first:
;;   - Full (outer) join, as may not have open EHCP `named-plan` for every open `placement-detail` (& vice versa).
;;   - Joining by `:requests-table-id` (in addition to `:person-table-id` and `:census-date`), as:
;;     - may have CYP with multiple `named-plan`s  from different `requests` open on a census date, and
;;     - may have CYP with multiple `active-plans` from different `requests` open on a census date, and
;;     - a `named-plan` & `active-plan` (for a census-date) for a CYP may be from different `requests`.
;; - Then merge in `sen-need-primary`, by `:requests-table-id`:
;;   - Merging in *after* joining `named-plans` & `placement-detail`
;;     in case have open `named-plan`s (with an `active-plans`) without corresponding open `placement-detail`.
;;   - Recall that there is at most 1 `active-plans` record for each `requests` record,
;;     so can merge `sen-need-primary` by `:requests-table-id` without loss of uniqueness.
;;   - Left join as only want primary needs for open `named-plan`s as well as primary `placement-detail`s
;;     (`sen-need`s are for each `:active-plans-table-id`).
;; - Then merge in `active-plans` (for `:transfer-la`), by `:requests-table-id`:
;;   - Merging in *after* joining `named-plans` & `placement-detail`
;;     in case have open `named-plan`s (with an `active-plans`) without corresponding open `placement-detail`.
;; - Then merge in `person` level info (including age and nominal NCY):
;;   - Left join as only want info for CYP with open `named-plan`s or primary `placement-detail`s.

(defn plans-placements-on-census-dates
  "Collate `named-plan`s and highest ranking `placement-detail`s open on census dates,
   with primary `sen-need`, `active-plans` and `person` details including age & nominal NCY for age.
   - For a census date not at the end of the SEN2 collection period,
     this may include plans that were open with another LA prior to transferring in.
   - Where a CYP has both rank 1 and rank 2 placements open on a census date for the same request,
     then we take the rank 1 one, but if the CYP only has a rank 2 placement open for a given request,
     then we take that. However, note that doing so may result in transitions from a rank 1 placement
     to a rank 2 placement if a rank 1 placement open at a census date ends before the next census date
     but the rank 2 placement continues up to or beyond it.
   - As from SEN2 return only, does not have `:settings` or `:designations`,
     and uses `:census-date` & `:nominal-ncy`
     (rather than `:calendar-year` & `:academic-year`).
   - Unique key is [`:person-table-id` `:requests-table-id` `:census-date`]: CYP
     with `named-plan`s or `placement-detail`s from more than one `request`
     open on a `:census-date` will have more than one record for that `:census-date`.
  "
  ([sen2-blade-csv-ds-map census-dates-ds] (plans-placements-on-census-dates sen2-blade-csv-ds-map census-dates-ds {}))
  ([sen2-blade-csv-ds-map census-dates-ds {:keys [person-on-census-dates-ds
                                                  named-plan-on-census-dates-ds
                                                  placement-detail-on-census-dates-ds
                                                  sen-need-primary-ds]}]
   (let [person-on-census-dates-ds           (or person-on-census-dates-ds
                                                 (person-on-census-dates sen2-blade-csv-ds-map census-dates-ds))
         named-plan-on-census-dates-ds       (or named-plan-on-census-dates-ds
                                                 (named-plan-on-census-dates sen2-blade-csv-ds-map census-dates-ds))
         placement-detail-on-census-dates-ds (or placement-detail-on-census-dates-ds
                                                 (placement-detail-on-census-dates sen2-blade-csv-ds-map census-dates-ds))
         sen-need-primary-ds                 (or sen-need-primary-ds
                                                 (sen-need-primary sen2-blade-csv-ds-map))]
     (-> (tc/full-join (-> named-plan-on-census-dates-ds
                           (tc/select-columns [:sen2-table-id :person-table-id :requests-table-id :assessment-table-id
                                               :named-plan-table-id :named-plan-order-seq-column
                                               :census-date
                                               :start-date :cease-date :cease-reason])
                           (tc/add-column :named-plan? true)
                           (tc/set-dataset-name "named-plan"))
                       (-> placement-detail-on-census-dates-ds
                           (tc/select-rows (comp zero? :census-date-placement-idx))
                           (tc/select-columns [:sen2-table-id :person-table-id :requests-table-id :active-plans-table-id
                                               :placement-detail-table-id :placement-detail-order-seq-column
                                               :census-date
                                               :entry-date :leaving-date :placement-rank
                                               :urn :ukprn :sen-unit-indicator :resourced-provision-indicator :sen-setting :sen-setting-other])
                           (tc/add-column :placement-detail? true)
                           (tc/set-dataset-name "placement-detail"))
                       [:sen2-table-id :person-table-id :requests-table-id :census-date])
         ;; As full (outer) join the join keys may be nil in either dataset so coalesce into single column.
         (tc/map-columns :sen2-table-id      [:sen2-table-id     :placement-detail.sen2-table-id    ] #(or %1 %2))
         (tc/map-columns :person-table-id    [:person-table-id   :placement-detail.person-table-id  ] #(or %1 %2))
         (tc/map-columns :requests-table-id  [:requests-table-id :placement-detail.requests-table-id] #(or %1 %2))
         (tc/map-columns :census-date        [:census-date       :placement-detail.census-date      ] #(or %1 %2))
         (tc/drop-columns #"^:placement-detail\..+$")
         ;;
         ;; Merge in `sen-need` EHCP primary need (if available)
         (tc/left-join (-> sen-need-primary-ds
                           (tc/select-columns [:sen2-table-id :person-table-id :requests-table-id :active-plans-table-id
                                               :sen-need-table-id :sen-need-order-seq-column
                                               :sen-type-rank :sen-type])
                           (tc/add-column :sen-need? true)
                           (tc/set-dataset-name "sen-need"))
                       [:sen2-table-id :person-table-id :requests-table-id])
         ;; As may have merged in a `sen-need` record for a `named-plan-on-census-dates-ds` without
         ;; a `placement-detail-on-census-dates-ds`, coalesce the `:active-plans-table-id`.
         (tc/map-columns :active-plans-table-id [:active-plans-table-id :sen-need.active-plans-table-id] #(or %1 %2))
         (tc/drop-columns #"^:sen-need\..+$")
         ;;
         ;; Merge in `active-plans` (if available)
         (tc/left-join (-> (sen2-blade-csv-ds-map :active-plans)
                           (tc/select-columns [:requests-table-id
                                               :active-plans-table-id :active-plans-order-seq-column
                                               :transfer-la])
                           (tc/add-column :active-plans? true)
                           (tc/set-dataset-name "active-plans"))
                       [:requests-table-id])
         (tc/drop-columns #"^:active-plans\..+$")
         ;;
         ;; Merge in `personal` details, inc. age & NCY for census dates (also bring in other `census-dates-ds` columns here)
         (tc/left-join (-> person-on-census-dates-ds
                           (tc/select-columns (concat [:sen2-table-id :person-table-id :person-order-seq-column
                                                       :upn :unique-learner-number
                                                       :person-birth-date]
                                                      (tc/column-names census-dates-ds)
                                                      [:age-at-start-of-school-year :nominal-ncy]))
                           (tc/add-column :person? true)
                           (tc/set-dataset-name "person"))
                       [:sen2-table-id :person-table-id :census-date])
         (tc/drop-columns #"^:person\..+$")
         ;; Arrange dataset
         (tc/reorder-columns (distinct (concat sen2-blade-csv/table-id-col-names
                                               [                  ] (tc/column-names census-dates-ds)
                                               [:person?          ] (tc/column-names person-on-census-dates-ds)
                                               [:named-plan?      ] (tc/column-names named-plan-on-census-dates-ds)
                                               [:active-plans?    ] (tc/column-names (sen2-blade-csv-ds-map :active-plans))
                                               [:placement-detail?] (tc/column-names placement-detail-on-census-dates-ds)
                                               [:sen-need?        ] (tc/column-names sen-need-primary-ds))))
         #_(tc/order-by        (conj sen2-blade-csv/table-id-col-names :census-date))
         (tc/set-dataset-name "plans-placements-on-census-dates")))))

(defn plans-placements-on-census-dates-col-name->label
  "Column labels for display."
  ([]
   (merge  sen2-blade-csv/table-id-col-name->label
           (sen2/census-dates-col-name->label)
           (person-on-census-dates-col-name->label)
           (named-plan-on-census-dates-col-name->label)
           (placement-detail-on-census-dates-col-name->label)
           sen2-blade-csv/active-plans-col-name->label
           (sen-need-primary-col-name->label)
           {:person?           "Got CYP details from `person`?"
            :named-plan?       "Got named plan from `named-plan`?"
            :active-plans?     "Got active plan from `active plans`?"
            :placement-detail? "Got placement details from `placement-detail`?"
            :sen-need?         "Got an EHCP primary need from `sen-need`?"}))
  ([ds]
   (-> (plans-placements-on-census-dates-col-name->label)
       (select-keys (tc/column-names ds)))))
