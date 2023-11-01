(ns witan.sen2.return.person-level.blade-export.csv.plans-placements
  "Tools to extract and manipulate plans & placements on census dates
   (with person details and EHCP primary need)
  from SEN2 person level return COLLECT Blade Export CSV files"
  (:require [clojure.string :as string]
            [tablecloth.api :as tc]
            [witan.sen2 :as sen2]
            [witan.sen2.ncy :as ncy]
            [witan.sen2.return.person-level.blade-export.csv :as sen2-blade-csv]
            [witan.sen2.return.person-level.dictionary :as sen2-dictionary]))




;;; # Definitions

(def person-id-cols
  "Person ID columns to carry through from `person` table (in addition to `:person-table-id`)."
  [:person-order-seq-column :upn :unique-learner-number])

(def sen2-estab-keys
  "SEN2 establishment column keywords from `placement-detail` table"
  [:urn :ukprn :sen-unit-indicator :resourced-provision-indicator :sen-setting])

(def cols-for-census
  "Columns from collated plans & placements required to construct census."
  (distinct (concat [:person-table-id] person-id-cols
                    [:requests-table-id]
                    [:census-year :census-date]
                    [#_:age-at-start-of-school-year :ncy-nominal]
                    sen2-estab-keys
                    [:sen-type])))




;;; # Extract SEN2 information on census dates
;;; ## Utilities
(defn- episode-on-census-date?
  "Identify if `census-date` is between episode `start-date` and `end-date`.
   Assumes open ended episodes if either `start-datte` or `end-date` is missing."
  [census-date start-date end-date]
  (and (or (nil?      start-date)
           (.isEqual  start-date census-date)
           (.isBefore start-date census-date))
       (or (nil?                             end-date)
           (.isEqual             census-date end-date)
           (.isBefore            census-date end-date))))

(defn extract-episodes-on-census-dates
  "Extract records from `episodes-ds` that span each `:census-date` in `census-dates-ds`,
  where episode start and end dates are in columns `episode-start-date-col` & `episode-end-date-col`.
  If an episode record spans multiple `:census-date`s then a record for each is returned."
  [episode-ds census-dates-ds episode-start-date-col episode-end-date-col]
  (-> episode-ds
      (tc/cross-join census-dates-ds)
      (tc/select-rows #(episode-on-census-date? (:census-date %) (% episode-start-date-col) (% episode-end-date-col)))))



;;; ## Person information

(defn person-on-census-dates
  "Selected columns from `person` table with record for each of `census-dates-ds`,
   with age at the start of school year containing the `:census-date` and corresponding nominal NCY."
  [sen2-blade-csv-ds-map census-dates-ds]
  (-> (:person sen2-blade-csv-ds-map)
      (tc/select-columns (distinct (concat [:sen2-table-id :person-table-id]
                                           person-id-cols
                                           [:person-birth-date])))
      (tc/cross-join census-dates-ds)
      (tc/map-columns :age-at-start-of-school-year [:census-date :person-birth-date] #(when %1 (ncy/age-at-start-of-school-year-for-date %1 %2)))
      (tc/map-columns :ncy-nominal [:age-at-start-of-school-year] ncy/age-at-start-of-school-year->ncy)
      (tc/convert-types {:age-at-start-of-school-year :int8
                         :ncy-nominal                 :int8})
      (tc/set-dataset-name "person-on-census-dates")))

(def person-on-census-dates-col-name->label
  "Column labels for display."
  (merge (select-keys sen2-blade-csv/person-col-name->label
                      (distinct (concat [:sen2-table-id :person-table-id]
                                        person-id-cols
                                        [:person-birth-date])))
         sen2/census-dates-col-name->label
         {:age-at-start-of-school-year "Age at start of school year"
          :ncy-nominal                 "Nominal NCY for age"}))



;;; ## `named-plan`s on census dates

(defn named-plan-on-census-dates
  "All `named-plan` records on census dates, with ancestor table IDs.

  Note that `named-plan`s on census dates are identified based on their `:start-date` and `:cease-date`:
  - The `:start-date` is the date of the EHC plan,
    which may be before the EHC plan transferred in.
  - The `:cease-date` is the date the EHC plan ended
    or the date the EHC plan was transferred to another LA.
  - Thus for a census date not at the end of the SEN2 collection period,
    this may include plans that were with another LA,
    prior to transferring in later during the collection year.
  "
  [sen2-blade-csv-ds-map census-dates-ds]
  (-> (sen2-blade-csv-ds-map :named-plan)
      (extract-episodes-on-census-dates census-dates-ds :start-date :cease-date)
      (tc/left-join (sen2-blade-csv/ds-map->ancestor-table-id-ds sen2-blade-csv-ds-map :named-plan-table-id)
                    [:assessment-table-id])
      (tc/drop-columns [:table-id-ds.assessment-table-id])
      (tc/order-by        (concat sen2-blade-csv/table-id-col-names [:census-date]))
      (tc/reorder-columns (concat sen2-blade-csv/table-id-col-names (tc/column-names census-dates-ds)))
      (tc/set-dataset-name "named-plan-on-census-dates")))

(def named-plan-on-census-dates-col-name->label
  "Column labels for display."
  (merge sen2-blade-csv/table-id-col-name->label
         sen2/census-dates-col-name->label
         sen2-blade-csv/named-plan-col-name->label))



;;; ## `placement-detail`s on census dates

(defn placement-detail-on-census-dates
  "All `placement-detail` records on census dates, with ancestor table IDs.
  
  Some `:person-table-id` will have a `:requests-table-id` with both
  `:placement-rank` 1 and `:placement-rank` 2 placements on a census date,
  and there may be some CYP with only a `:placement-rank` 2 placement
  on a `:census-date`.

  To permit selection of the placement with highest rank on a census date,
  column `:census-date-placement-idx` is added to the dataset, indexing
  placements by [`:person-table-id` `:requests-table-id` `:census-date`]
  in `:placement-rank` order: this is zero for the placement with highest rank
  on the `:census-date`.

  Note that taking rank 2 placements for analysis may result in
  transitions from a rank 1 placement to a rank 2 placement if a rank
  1 placement on a census date ends before the next census date
  but the rank 2 placement continues beyond it.
  "
  [sen2-blade-csv-ds-map census-dates-ds]
  (-> (sen2-blade-csv-ds-map :placement-detail)
      (extract-episodes-on-census-dates census-dates-ds :entry-date :leaving-date )
      (tc/left-join (sen2-blade-csv/ds-map->ancestor-table-id-ds sen2-blade-csv-ds-map :placement-detail-table-id)
                    [:active-plans-table-id])
      (tc/drop-columns [:table-id-ds.active-plans-table-id])
      ;; Add `:census-date-placement-idx` to index multiple placements in `:placement-rank` order.
      (tc/order-by [:person-table-id :requests-table-id :census-date :placement-rank])
      (tc/group-by [:person-table-id :requests-table-id :census-date])
      (tc/add-column :census-date-placement-idx (range))
      (tc/ungroup)
      ;; Order
      (tc/order-by        (concat sen2-blade-csv/table-id-col-names [:census-date] [:placement-rank]))
      (tc/reorder-columns (concat sen2-blade-csv/table-id-col-names (tc/column-names census-dates-ds) [:census-date-placement-idx]))
      (tc/set-dataset-name "placement-detail-on-census-dates")))

(def placement-detail-on-census-dates-col-name->label
  "Column labels for display."
  (merge sen2-blade-csv/table-id-col-name->label
         sen2/census-dates-col-name->label
         {:census-date-placement-idx "Census date placement index"}
         sen2-blade-csv/placement-detail-col-name->label))



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

(def sen-need-primary-col-name->label
  "Column labels for display."
  (merge sen2-blade-csv/table-id-col-name->label
         sen2-blade-csv/sen-need-col-name->label))




;;; # Collate

;; Collate `named-plan`s on census dates with primary `placement-detail`s on census dates,
;; add primary `sen-need`, `active-plans` (for `:transfer-la`) and `person` age/NCY details.
;; Note merge order:
;; - Merging `named-plan-on-census-dates` with highest ranking `placement-detail-on-census-dates` first:
;;   - Full (outer) join, as may not have both an EHCP `named-plan` and a `placement-detail` for a census date.
;;   - Joining by `:requests-table-id` (in addition to `:person-table-id` and `:census-date`), as:
;;     - may have CYP with multiple `named-plan`s  from different `requests` on a census date, and
;;     - may have CYP with multiple `active-plans` from different `requests` on a census date, and
;;     - a `named-plan` & `active-plan` (for a census-date) for a CYP may be from different `requests`.
;; - Then merge in `sen-need-primary`, by `:requests-table-id`:
;;   - Merging in *after* joining `named-plans` & `placement-detail`
;;     in case have `named-plan`s (with an `active-plans`) on a census date without corresponding `placement-detail`.
;;   - Recall that there is at most 1 `active-plans` record for each `requests` record,
;;     so can merge `sen-need-primary` by `:requests-table-id` without loss of uniqueness.
;;   - Left join as only want primary needs where have `named-plan` or `placement-detail` on census date
;;     (`sen-need`s are for each `:active-plans-table-id`).
;; - Then merge in `active-plans` (for `:transfer-la`), by `:requests-table-id`:
;;   - Merging in *after* joining `named-plans` & `placement-detail`
;;     in case have `named-plan`s (with an `active-plans`) on a census date without corresponding `placement-detail`.
;; - Then merge in `person` level info (including age and nominal NCY):
;;   - Left join as only want info for CYP with `named-plan`s or `placement-detail` on census date.

(defn plans-placements-on-census-dates
  "Collate `named-plan`s and highest ranking `placement-detail`s on census dates,
   with primary `sen-need`, `active-plans` and `person` details including age & nominal NCY for age.
   - For a census date not at the end of the SEN2 collection period,
     this may include plans that were with another LA prior to transferring in.
   - Where a CYP has both rank 1 and rank 2 placements on a census date for the same request,
     then we take the rank 1 one, but if the CYP only has a rank 2 placement on a census date for a given request,
     then we take that. However, note that doing so may result in transitions from a rank 1 placement
     to a rank 2 placement if a rank 1 placement on a census date ends before the next census date
     but the rank 2 placement continues up to or beyond it.
   - As from SEN2 return only, does not have `:settings` or `:designations`,
     and uses `:census-date` & `:ncy-nominal`
     (rather than `:calendar-year` & `:academic-year`).
   - Unique key is [`:person-table-id` `:requests-table-id` `:census-date`]:
     CYP with `named-plan`s or `placement-detail`s from more than one `request`
     on a `:census-date` will have more than one record for that `:census-date`.
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
                           (tc/select-columns (distinct (concat [:sen2-table-id :person-table-id :requests-table-id :active-plans-table-id
                                                                 :placement-detail-table-id :placement-detail-order-seq-column
                                                                 :census-date
                                                                 :entry-date :leaving-date :placement-rank]
                                                                sen2-estab-keys
                                                                [:sen-setting-other])))
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
                           #_(tc/select-columns (distinct (concat [:sen2-table-id :person-table-id]
                                                                  person-id-cols
                                                                  [:person-birth-date]
                                                                  (tc/column-names census-dates-ds)
                                                                  [:age-at-start-of-school-year :ncy-nominal])))
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
         (tc/order-by [:person-table-id :census-date :requests-table-id])
         (tc/set-dataset-name "plans-placements-on-census-dates")))))

(def plans-placements-on-census-dates-col-name->label
  "Column labels for display."
  (merge  sen2-blade-csv/table-id-col-name->label
          sen2/census-dates-col-name->label
          person-on-census-dates-col-name->label
          named-plan-on-census-dates-col-name->label
          placement-detail-on-census-dates-col-name->label
          sen2-blade-csv/active-plans-col-name->label
          sen-need-primary-col-name->label
          {:person?           "Got CYP details from `person`?"
           :named-plan?       "Got named plan from `named-plan`?"
           :active-plans?     "Got active plan from `active plans`?"
           :placement-detail? "Got placement details from `placement-detail`?"
           :sen-need?         "Got an EHCP primary need from `sen-need`?"}))




;;; # Checks
(def checks
  "Definitions for standard checks for issues in dataset of plans & placements on census dates."
  (let [count-true? #(comp count (partial filter true?) %)
        ;; Check definitions:
        ;; - `:col-fn`s:
        ;;   - are applied to the dataset in turn.
        ;;   - should (only) add a single column to the dataset, with name matching the check key.
        ;;   - should otherwise leave the dataset intact.
        m           {:issue-non-unique-key
                     {:idx           001
                      :label         "[:person-table-id :census-date :requests-table-id] not unique key"
                      :action        ["- Should be unique."]
                      :col-fn        (fn [ds] (-> ds
                                                  (tc/group-by [:person-table-id :census-date :requests-table-id])
                                                  (tc/add-column :issue-non-unique-key tc/row-count)
                                                  (tc/ungroup)
                                                  (tc/map-columns :issue-non-unique-key
                                                                  [:issue-non-unique-key]
                                                                  (partial < 1))))
                      :summary-fn    #(-> %
                                          (tc/select-rows :issue-non-unique-key)
                                          (tc/unique-by [:person-table-id :census-date :requests-table-id])
                                          tc/row-count)
                      :summary-label "#keys"}
                     :issue-multiple-requests
                     {:idx           101
                      :label         "CYP has plans|placements from multiple requests"
                      :action        ["- Drop all but one."]
                      :col-fn        (fn [ds] (-> ds
                                                  (tc/group-by [:person-table-id :census-date])
                                                  (tc/add-column :issue-multiple-requests tc/row-count)
                                                  (tc/ungroup)
                                                  (tc/map-columns :issue-multiple-requests
                                                                  [:issue-multiple-requests]
                                                                  (partial < 1))))
                      :summary-fn    (comp count distinct :person-table-id
                                           #(tc/select-rows % :issue-multiple-requests))
                      :summary-label "#CYP"}
                     :issue-no-age-at-start-of-school-year
                     {:idx           102
                      :label         "Could not calculate age at the start of the school year"
                      :action        ["- Get DoB or otherwise assign NCY."]
                      :col-fn        (fn [ds] (tc/map-columns ds
                                                              :issue-no-age-at-start-of-school-year
                                                              [:age-at-start-of-school-year]
                                                              nil?))
                      :summary-fn    (comp count distinct :person-table-id
                                           #(tc/select-rows % :issue-no-age-at-start-of-school-year))
                      :summary-label "#CYP"}
                     :issue-cyp-age-25-plus
                     {:idx           111
                      :label         "CYP aged 25+ at the start of the school year"
                      :action        ["- Consider dropping."]
                      :col-fn        (fn [ds] (tc/map-columns ds
                                                              :issue-cyp-age-25-plus
                                                              [:age-at-start-of-school-year]
                                                              #(when % (<= 25 %))))
                      :summary-fn    (comp count distinct :person-table-id
                                           #(tc/select-rows % :issue-cyp-age-25-plus))
                      :summary-label "#CYP"}
                     :issue-no-named-plan
                     {:idx           211
                      :label         "No named-plan on census date"
                      :action        ["- Not an issue for modelling as don't need any details from named-plan."
                                      "- Flag to client as incomplete/incoherent data and consider dropping."]
                      :col-fn        (fn [ds] (tc/map-columns ds
                                                              :issue-no-named-plan
                                                              [:named-plan?]
                                                              not))
                      :summary-fn    (count-true? :issue-no-named-plan)
                      :summary-label "#rows"}
                     :issue-no-placement-detail
                     {:idx           221
                      :label         "No placement-detail"
                      :action        ["- Drop if transferred in after `:census-date`."
                                      "- Otherwise get `placement-detail` (or will be considered unknown)."]
                      :col-fn        (fn [ds] (tc/map-columns ds
                                                              :issue-no-placement-detail
                                                              [:placement-detail?]
                                                              not))
                      :summary-fn    (count-true? :issue-no-placement-detail)
                      :summary-label "#rows"}
                     :issue-no-placement-detail-transferred-in
                     {:idx           222
                      :label         "No placement-detail - transferred in"
                      :action        ["- Drop if transferred in after `:census-date`."
                                      "- Otherwise get `placement-detail` (or will be considered unknown)."]
                      :col-fn        (fn [ds]  (tc/map-columns ds
                                                               :issue-no-placement-detail-transferred-in
                                                               [:placement-detail? :transfer-la]
                                                               #(and (not %1) (some? %2))))
                      :summary-fn    (count-true? :issue-no-placement-detail-transferred-in)
                      :summary-label "#rows"}
                     :issue-no-placement-detail-not-transferred-in
                     {:idx           223
                      :label         "No placement-detail - not transferred in"
                      :action        ["- Get `placement-detail` (or will be considered unknown)."]
                      :col-fn        (fn [ds] (tc/map-columns ds
                                                              :issue-no-placement-detail-not-transferred-in
                                                              [:placement-detail? :transfer-la]
                                                              #(and (not %1) (nil?  %2))))
                      :summary-fn    (count-true? :issue-no-placement-detail-not-transferred-in)
                      :summary-label "#rows"}
                     :issue-invalid-sen-setting
                     {:idx           228
                      :label         "Invalid sen-setting"
                      :action        ["- Assign a recognised SENsetting."]
                      :col-fn        (fn [ds] (tc/map-columns ds
                                                              :issue-invalid-sen-setting
                                                              [:sen-setting]
                                                              (complement (partial contains? (conj sen2-dictionary/sen-settings nil)))))
                      :summary-fn    (count-true? :issue-invalid-sen-setting)
                      :summary-label "#rows"}
                     :issue-no-sen-type
                     {:idx           231
                      :label         "No sen-type (EHCP need)"
                      :action        ["- Get sen-type (or will be considered unknown)."]
                      :col-fn        (fn [ds] (tc/map-columns ds
                                                              :issue-no-sen-type
                                                              [:sen-type]
                                                              nil?))
                      :summary-fn    (count-true? :issue-no-sen-type)
                      :summary-label "#rows"}
                     :issue-invalid-sen-type
                     {:idx           232
                      :label         "Invalid sen-type (EHCP need)"
                      :action        ["- Correct or consider as custom EHCP primary need."]
                      :col-fn        (fn [ds] (tc/map-columns ds
                                                              :issue-invalid-sen-type
                                                              [:sen-type]
                                                              (complement (partial contains? (conj sen2-dictionary/sen-types nil)))))
                      :summary-fn    (count-true? :issue-invalid-sen-type)
                      :summary-label "#rows"}}]
    (into (sorted-map-by (fn [k1 k2] (compare [(get-in m [k1 :idx]) k1]
                                              [(get-in m [k2 :idx]) k2]))) m)))

(defn flag-issues
  "Run `checks'` on `plans-placements-on-census-dates` dataset, adding issue flag columns."
  ([plans-placements-on-census-dates'] (flag-issues plans-placements-on-census-dates' checks))
  ([plans-placements-on-census-dates' checks']
   (-> plans-placements-on-census-dates'
       ((apply comp (reverse (map :col-fn (vals checks')))))
       (tc/set-dataset-name "plans-placements-on-census-dates-issues-flagged"))))

(defn plans-placements-on-census-dates-issues-flagged-col-name->label
  "Column labels for display"
  ([]
   (plans-placements-on-census-dates-issues-flagged-col-name->label checks))
  ([checks']
   (merge plans-placements-on-census-dates-col-name->label
          (update-vals checks' :label))))

(def names-of-update-cols
  "Names of columns to add to issues dataset for updates."
  [:update-drop?
   :update-ncy-nominal
   :update-urn
   :update-ukprn
   :update-sen-unit-indicator
   :update-resourced-provision-indicator
   :update-sen-setting
   :update-sen-type
   :update-notes])

(defn flagged-issues->ds
  "Extract issues dataset from `plans-placements-on-census-dates-issues-flagged` containing
   rows with issues flagged by `checks'`,
   selected columns for review,
   and blank columns for manual updates."
  ([plans-placements-on-census-dates-issues-flagged]
   (flagged-issues->ds plans-placements-on-census-dates-issues-flagged checks))
  ([plans-placements-on-census-dates-issues-flagged checks']
   (-> plans-placements-on-census-dates-issues-flagged
       ;; Select only rows with an issue flagged:
       (tc/select-rows #((apply some-fn (keys checks')) %))
       ;; Add empty columns for updated data:
       (tc/add-columns (zipmap names-of-update-cols (repeat nil)))
       ;; Select columns in sensible order for manual review of issues:
       (tc/select-columns (distinct (concat
                                     ;; Person ID columns
                                     [:person-table-id] person-id-cols
                                     ;; Census date & year
                                     [:census-year :census-date]
                                     ;; Age & NCY
                                     [:age-at-start-of-school-year :ncy-nominal]
                                     ;; Request ID (as may vary by `:census-year`)
                                     [:requests-table-id]
                                     ;; Issue flags
                                     (keys checks')
                                     ;; Blank columns for updates
                                     names-of-update-cols
                                     ;; Plan info from SEN2 `named-plan` module
                                     [:named-plan?
                                      :start-date :cease-date :cease-reason]
                                     ;; Active plan info from SEN2 `active-plans` module
                                     [:active-plans?
                                      :transfer-la]
                                     ;; Placement info from SEN2 `placement-detail` module
                                     [:placement-detail?
                                      :placement-rank :entry-date :leaving-date]
                                     sen2-estab-keys
                                     ;; SEN need info from SEN2 `sen-need` module
                                     [:sen-need?
                                      :sen-type])))
       (tc/order-by [:person-table-id :census-date :requests-table-id])
       (tc/set-dataset-name "plans-placements-on-census-dates-issues"))))

(defn plans-placements-on-census-dates-issues-col-name->label
  "Column labels for display."
  ([]
   (plans-placements-on-census-dates-issues-col-name->label checks))
  ([checks']
   (merge plans-placements-on-census-dates-col-name->label
          (update-vals checks' :label)
          (reduce (fn [m k] (assoc m k (-> k
                                           name
                                           (#(string/replace % "update-" ""))
                                           (#(plans-placements-on-census-dates-col-name->label (keyword %) %))
                                           ((partial str "Update: ")))))
                  {} names-of-update-cols))))

(defn issues->ds
  "Run `checks'` on `plans-placements-on-census-dates` dataset, extracting
   rows with issues flagged by `checks'`,
   selected columns for review,
   and blank columns for manual updates."
  ([plans-placements-on-census-dates'] (issues->ds plans-placements-on-census-dates' checks))
  ([plans-placements-on-census-dates' checks']
   (-> plans-placements-on-census-dates'
       (flag-issues        checks')
       (flagged-issues->ds checks'))))

(defn summarise-issues
  "Summarise issues flagged in `plans-placements-on-census-dates-issues-flagged` as a result of running `checks'`."
  ([plans-placements-on-census-dates-issues-flagged]
   (summarise-issues plans-placements-on-census-dates-issues-flagged checks))
  ([plans-placements-on-census-dates-issues-flagged checks']
   (-> plans-placements-on-census-dates-issues-flagged
       (tc/group-by [:census-date])
       (tc/aggregate (merge (update-vals checks' :summary-fn)
                            {:num-cyp   (comp count distinct :person-table-id)
                             :row-count tc/row-count}))
       (tc/order-by [:census-date])
       (tc/update-columns [:census-date] (partial map #(.format % (java.time.format.DateTimeFormatter/ISO_LOCAL_DATE))))
       (tc/pivot->longer (complement #{:census-date}))
       (tc/pivot->wider :census-date :$value)
       (tc/rename-columns {:$column :issue-key})
       (tc/map-columns :idx [:issue-key] (merge (update-vals checks' :idx)
                                                {:num-cyp   200
                                                 :row-count 300}))
       (tc/map-columns :label [:issue-key] (merge (update-vals checks' :label)
                                                  {:num-cyp   "TOTAL number of CYP"
                                                   :row-count "TOTAL number of records"}))
       (tc/map-columns :summary-label [:issue-key] (merge (update-vals checks' :summary-label)
                                                          {:num-cyp   "#CYP"
                                                           :row-count "#rows"}))
       (tc/map-columns :desc [:issue-key] (merge (update-vals checks'
                                                              #(apply format "%03d: %s (%s)"
                                                                      ((juxt :idx :label :summary-label) %)))
                                                 {:num-cyp   "(#CYP)"
                                                  :row-count "(#rows)"}))
       (tc/order-by [(comp (merge (update-vals checks' :idx)
                                  {:num-cyp   200
                                   :row-count 300})
                           :issue-key)])
       (tc/reorder-columns [:issue-key :idx :label :summary-label])
       (tc/drop-columns [:issue-key :desc])
       (tc/rename-columns {:issue-key     "key"
                           :idx           "Index"
                           :label         "Issue | TOTAL"
                           :summary-label "Summary"
                           :desc          "Index: Issue (summary statistic)"}))))




;;; # CSV file read
(defn csv-file->ds
  "Read columns required to construct census
   from CSV file of `plans-placements-on-census-dates` into a dataset."
  [filepath]
  (tc/dataset filepath
              {:column-allowlist (map name cols-for-census)
               :key-fn           keyword
               :parser-fn        (merge (select-keys sen2-blade-csv/parser-fn
                                                     cols-for-census)
                                        {:census-date                 :packed-local-date
                                         :census-year                 :int16
                                         :age-at-start-of-school-year [:int8 parse-long]
                                         :ncy-nominal                 [:int8 parse-long]})}))

(defn updates-csv-file->ds
  "Read columns required to update columns for census
   from CSV file of `plans-placements-on-census-dates-issues`
   with update columns filled in into a dataset."
  [filepath]
  (tc/dataset filepath
              {:column-allowlist (map name [:person-table-id
                                            :census-date
                                            :requests-table-id
                                            :update-drop?
                                            :update-ncy-nominal
                                            :update-urn
                                            :update-ukprn
                                            :update-sen-unit-indicator
                                            :update-resourced-provision-indicator
                                            :update-sen-setting
                                            :update-sen-type])
               :key-fn           keyword
               :parser-fn        (merge {:person-table-id    (sen2-blade-csv/parser-fn :person-table-id)
                                         :census-date        :packed-local-date
                                         :census-year        :int16
                                         :requests-table-id  (sen2-blade-csv/parser-fn :requests-table-id)
                                         :update-drop?       :boolean
                                         :update-ncy-nominal [:int8 parse-long]}
                                        (update-vals   {:update-urn                           :urn
                                                        :update-ukprn                         :ukprn
                                                        :update-sen-unit-indicator            :sen-unit-indicator
                                                        :update-resourced-provision-indicator :resourced-provision-indicator
                                                        :update-sen-setting                   :sen-setting
                                                        :update-sen-type                      :sen-type}
                                                       sen2-blade-csv/parser-fn))}))




;;; # Updating
(defn summarise-updates
  "Summarise updates in `plans-placements-on-census-dates-updates` dataset."
  [plans-placements-on-census-dates-updates]
  (-> plans-placements-on-census-dates-updates
      (tc/map-columns :update-sen2-establishment [:update-urn
                                                  :update-ukprn
                                                  :update-sen-unit-indicator
                                                  :update-resourced-provision-indicator
                                                  :update-sen-setting]
                      (fn [& args] (some some? args)))
      (tc/update-columns [:census-date] (partial map #(.format % (java.time.format.DateTimeFormatter/ISO_LOCAL_DATE))))
      (tc/update-columns [:update-drop?] (partial map #(if % "✓" " ")))
      (tc/update-columns #"^:update-[^\?]*" (partial map #(if (some? %) "Δ" " ")))
      (tc/group-by [:census-date :update-drop? :update-ncy-nominal :update-sen2-establishment :update-sen-type])
      (tc/aggregate {:row-count tc/row-count})
      (tc/pivot->wider :census-date :row-count {:drop-missing? false})
      (tc/rename-columns {:update-drop?              "drop?"
                          :update-ncy-nominal        "Nominal NCY"
                          :update-sen2-establishment "SEN2 Establishment"
                          :update-sen-type           "sen-type (need)"})))

(defn update-plans-placements-on-census-dates
  "Apply updates from `plans-placements-on-census-dates-updates'` to `plans-placements-on-census-dates'`."
  [plans-placements-on-census-dates' plans-placements-on-census-dates-updates']
  (-> plans-placements-on-census-dates'
      (tc/select-columns cols-for-census)
      (tc/left-join (-> plans-placements-on-census-dates-updates'
                        (tc/select-columns [:person-table-id :census-date :requests-table-id
                                            :update-drop?
                                            :update-ncy-nominal
                                            :update-urn
                                            :update-ukprn
                                            :update-sen-unit-indicator
                                            :update-resourced-provision-indicator
                                            :update-sen-setting
                                            :update-sen-type])
                        (tc/set-dataset-name "update"))
                    [:person-table-id :census-date :requests-table-id])
      (tc/drop-columns #"^:update\..*$")
      ;; Drop records with `:update-drop?`
      (tc/drop-rows :update-drop?)
      ;; Update `:ncy-nominal` if non-nil in update dataset
      (tc/map-columns :ncy-nominal
                      [:update-ncy-nominal :ncy-nominal]
                      #(if (some? %1) %1 %2))
      ;; Update all sen2-establishment columns if any are non nil in update dataset
      (tc/map-columns :update-sen2-establishment-cols? [:update-urn
                                                        :update-ukprn
                                                        :update-sen-unit-indicator
                                                        :update-resourced-provision-indicator
                                                        :update-sen-setting]
                      (fn [& args] (some some? args)))
      (tc/map-columns :urn
                      [:update-sen2-establishment-cols? :update-urn :urn]
                      #(if %1 %2 %3))
      (tc/map-columns :ukprn
                      [:update-sen2-establishment-cols? :update-ukprn :ukprn]
                      #(if %1 %2 %3))
      (tc/map-columns :sen-unit-indicator
                      [:update-sen2-establishment-cols? :update-sen-unit-indicator :sen-unit-indicator]
                      #(if %1 (if (some? %2) %2 false) %3))
      (tc/map-columns :resourced-provision-indicator
                      [:update-sen2-establishment-cols? :update-resourced-provision-indicator :resourced-provision-indicator]
                      #(if %1 (if (some? %2) %2 false) %3))
      (tc/map-columns :sen-setting
                      [:update-sen2-establishment-cols? :update-sen-setting :sen-setting]
                      #(if %1 %2 %3))
      ;; Update `:sen-type` if non-nil in update dataset
      (tc/map-columns :sen-type
                      [:update-sen-type :sen-type]
                      #(if (some? %1) %1 %2))
      ;; Drop update columns
      (tc/drop-columns #"^:update-.*$")
      ;; Arrange dataset
      (tc/order-by [:person-table-id :census-date :requests-table-id])
      (tc/set-dataset-name "plans-placements-on-census-dates-updated")))

