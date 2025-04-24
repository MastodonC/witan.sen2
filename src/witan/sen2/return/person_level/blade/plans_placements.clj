(ns witan.sen2.return.person-level.blade.plans-placements
  "Tools to extract and manipulate plans & placements on census dates
   (with person details and EHCP primary need) from SEN2 Blade."
  (:require [clojure.set :as set]
            [clojure.string :as string]
            [tablecloth.api :as tc]
            [witan.gias :as gias]
            [witan.sen2.ncy :as ncy]
            [witan.sen2.return.person-level.dictionary :as sen2-dictionary]))



;;; # Definitions
(def sen2-blade-table-id-col-names
  "SEN2 Blade `:*-table-id` column names"
  [:sen2-table-id :person-table-id :requests-table-id
   :assessment-table-id :named-plan-table-id :plan-detail-table-id
   :active-plans-table-id :placement-detail-table-id :sen-need-table-id])

(def sen2-estab-keys
  "SEN2 establishment column keywords from `placement-detail` table"
  [:urn :ukprn :sen-unit-indicator :resourced-provision-indicator :sen-setting])

(def sen2-blade-module-cols-to-select
  "Map of SEN2 Blade columns to select for inclusion from each module (in addition to `:*-table-id` cols)."
  {:person           [:person-order-seq-column :upn :unique-learner-number :person-birth-date]
   :named-plan       [:start-date :cease-date :cease-reason]
   :placement-detail (vec (concat [:placement-rank :entry-date :leaving-date]
                                  sen2-estab-keys [:sen-setting-other]))
   :active-plans     [:transfer-la]
   :sen-need         [:sen-type]})

(def person-id-cols-to-keep
  "Person ID columns to keep when reading/updating plans-placements-on-census-dates datasets."
  [:person-table-id :person-order-seq-column :upn :unique-learner-number])

(def val-cols-to-keep
  "Default value columns to keep when reading/updating plans-placements-on-census-dates datasets."
  (distinct (concat [:sen-type]
                    [:ncy-nominal]
                    sen2-estab-keys)))

(def cols-to-keep
  "Default columns from collated plans & placements to keep when reading/updating."
  (distinct (concat person-id-cols-to-keep
                    [:census-year :census-date]
                    [:requests-table-id]
                    val-cols-to-keep)))

(def key-cols-for-census
  "Key (as in unique composite database key) plans-placements-on-census-dates columns for census.
   Note that only `:person-table-id` & `:census-year` are actually required.
   Note that `:requests-table-id` is _not_ included,
        as by census stage multiple plans|placements on census dates
        from different requests should have been resolved."
  (concat person-id-cols-to-keep
          [:census-year :census-date]))



;;; # Extract SEN2 information on census dates
;;; ## Utilities
(defn episode-on-census-date?
  "Identify if `census-date` is between episode `start-date` and `end-date`.
   Assumes open ended episodes if either `start-date` or `end-date` is missing."
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
  "Columns from `person` table with record for each of `census-dates-ds`,
   with age at the start of school year containing the `:census-date` and corresponding nominal NCY."
  [sen2-blade-ds-map census-dates-ds]
  (-> (:person sen2-blade-ds-map)
      (tc/cross-join census-dates-ds)
      (tc/map-columns :age-at-start-of-school-year [:census-date :person-birth-date] #(when %1 (ncy/age-at-start-of-school-year-for-date %1 %2)))
      (tc/map-columns :ncy-nominal [:age-at-start-of-school-year] ncy/age-at-start-of-school-year->ncy)
      (tc/convert-types {:age-at-start-of-school-year :int8
                         :ncy-nominal                 :int8})
      (tc/set-dataset-name "person-on-census-dates")))

(defn person-on-census-dates-col-name->label
  "Column labels for display, given mappings of col-name->label for `person` and `census-dates` datasets."
  [& {person-col-name->label       :person
      census-dates-col-name->label :census-dates}]
  (merge person-col-name->label
         census-dates-col-name->label
         {:age-at-start-of-school-year "Age at start of school year"
          :ncy-nominal                 "Nominal NCY for age"}))


;;; ## `named-plan`s on census dates
(defn named-plan-on-census-dates
  "All `named-plan` records on census dates.

  Note that `named-plan`s on census dates are identified based on their `:start-date` and `:cease-date`:
  - The `:start-date` is the date of the EHC plan,
    which may be before the EHC plan transferred in.
  - The `:cease-date` is the date the EHC plan ended
    or the date the EHC plan was transferred to another LA.
  - Thus for a census date not at the end of the SEN2 collection period,
    this may include plans that were with another LA,
    prior to transferring in later during the collection year.
  "
  [sen2-blade-ds-map census-dates-ds]
  (-> (sen2-blade-ds-map :named-plan)
      (extract-episodes-on-census-dates census-dates-ds :start-date :cease-date)
      (tc/order-by        (concat sen2-blade-table-id-col-names [:census-date]))
      (tc/reorder-columns (concat sen2-blade-table-id-col-names (tc/column-names census-dates-ds)))
      (tc/set-dataset-name "named-plan-on-census-dates")))

(defn named-plan-on-census-dates-col-name->label
  "Column labels for display, given mappings of col-name->label for `named-plan` and `census-dates` datasets."
  [& {named-plan-col-name->label   :named-plan
      census-dates-col-name->label :census-dates}]
  (merge census-dates-col-name->label
         named-plan-col-name->label))


;;; ## `placement-detail`s on census dates
(defn placement-detail-on-census-dates
  "All `placement-detail` records on census dates.

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
  [sen2-blade-ds-map census-dates-ds]
  (-> (sen2-blade-ds-map :placement-detail)
      (extract-episodes-on-census-dates census-dates-ds :entry-date :leaving-date )
      ;; Add `:census-date-placement-idx` to index multiple placements in `:placement-rank` order.
      (tc/order-by [:person-table-id :requests-table-id :census-date :placement-rank])
      (tc/group-by [:person-table-id :requests-table-id :census-date])
      (tc/add-column :census-date-placement-idx (range))
      (tc/ungroup)
      ;; Order
      (tc/order-by        (concat sen2-blade-table-id-col-names [:census-date] [:placement-rank]))
      (tc/reorder-columns (concat sen2-blade-table-id-col-names (tc/column-names census-dates-ds) [:census-date-placement-idx]))
      (tc/set-dataset-name "placement-detail-on-census-dates")))

(defn placement-detail-on-census-dates-col-name->label
  "Column labels for display, given mappings of col-name->label for `placement-detail` and `census-dates` datasets."
  [& {placement-detail-col-name->label :placement-detail
      census-dates-col-name->label     :census-dates}]
  (merge census-dates-col-name->label
         {:census-date-placement-idx "Census date placement index"}
         placement-detail-col-name->label))


;;; ## EHCP primary need from `sen-need`
(defn sen-need-primary
  "Primary SEN need from `sen-need`, where primary is `:sen-type-rank`=1."
  [sen2-blade-ds-map]
  (-> (sen2-blade-ds-map :sen-need)
      (tc/select-rows #(= 1 (:sen-type-rank %)))
      (tc/order-by        sen2-blade-table-id-col-names)
      (tc/reorder-columns sen2-blade-table-id-col-names)
      (tc/set-dataset-name "sen-need-primary")))

(defn sen-need-primary-col-name->label
  "Column labels for display, given mappings of col-name->label for `sen-need` dataset."
  [& {sen-need-col-name->label :sen-need}]
  (merge sen-need-col-name->label))



;;; # Collate
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
     on a `:census-date` will have more than one record for that `:census-date`."
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
  [sen2-blade-ds-map census-dates-ds & {:keys [sen2-blade-module-cols-to-select
                                               person-on-census-dates-ds
                                               named-plan-on-census-dates-ds
                                               placement-detail-on-census-dates-ds
                                               sen-need-primary-ds]
                                        :or   {sen2-blade-module-cols-to-select sen2-blade-module-cols-to-select}}]
  (let [person-on-census-dates-ds           (or person-on-census-dates-ds
                                                (person-on-census-dates sen2-blade-ds-map census-dates-ds))
        named-plan-on-census-dates-ds       (or named-plan-on-census-dates-ds
                                                (named-plan-on-census-dates sen2-blade-ds-map census-dates-ds))
        placement-detail-on-census-dates-ds (or placement-detail-on-census-dates-ds
                                                (placement-detail-on-census-dates sen2-blade-ds-map census-dates-ds))
        sen-need-primary-ds                 (or sen-need-primary-ds
                                                (sen-need-primary sen2-blade-ds-map))]
    (-> (tc/full-join (-> named-plan-on-census-dates-ds
                          (tc/select-columns (concat sen2-blade-table-id-col-names
                                                     [:census-date]
                                                     (:named-plan sen2-blade-module-cols-to-select)))
                          (tc/add-column :named-plan? true)
                          (tc/set-dataset-name "named-plan"))
                      (-> placement-detail-on-census-dates-ds
                          (tc/select-rows (comp zero? :census-date-placement-idx))
                          (tc/select-columns (concat sen2-blade-table-id-col-names
                                                     [:census-date]
                                                     (:placement-detail sen2-blade-module-cols-to-select)))
                          (tc/add-column :placement-detail? true)
                          (tc/set-dataset-name "placement-detail"))
                      [:sen2-table-id :person-table-id :requests-table-id :census-date])
        ;; As full (outer) join the join keys may be nil in either dataset so coalesce into single column.
        ((fn [ds] (cond-> ds
                    (contains? ds :placement-detail.sen2-table-id) ; May not have `:sen2-table-id`
                    (tc/map-columns :sen2-table-id         [:sen2-table-id     :placement-detail.sen2-table-id    ] #(or %1 %2))
                    true
                    (tc/map-columns :person-table-id       [:person-table-id   :placement-detail.person-table-id  ] #(or %1 %2))
                    true
                    (tc/map-columns :requests-table-id     [:requests-table-id :placement-detail.requests-table-id] #(or %1 %2))
                    true
                    (tc/map-columns :census-date           [:census-date       :placement-detail.census-date      ] #(or %1 %2)))))
        (tc/drop-columns #"^:placement-detail\..+$")
        ;;
        ;; Merge in `sen-need` EHCP primary need (if available)
        (tc/left-join (-> sen-need-primary-ds
                          (tc/select-columns (concat sen2-blade-table-id-col-names
                                                     (:sen-need sen2-blade-module-cols-to-select)))
                          (tc/add-column :sen-need? true)
                          (tc/set-dataset-name "sen-need"))
                      [:sen2-table-id :person-table-id :requests-table-id])
        ;; As may have merged in a `sen-need` record for a `named-plan-on-census-dates-ds` without
        ;; a `placement-detail-on-census-dates-ds`, coalesce the `:active-plans-table-id` (if present).
        ((fn [ds] (cond-> ds
                    (contains? ds :sen-need.active-plans-table-id)
                    (tc/map-columns :active-plans-table-id [:active-plans-table-id :sen-need.active-plans-table-id] #(or %1 %2)))))
        (tc/drop-columns #"^:sen-need\..+$")
        ;;
        ;; Merge in `active-plans` (if available)
        (tc/left-join (-> (sen2-blade-ds-map :active-plans)
                          (tc/select-columns (concat [:requests-table-id] #_sen2-blade-table-id-col-names
                                                     (:active-plans sen2-blade-module-cols-to-select)))
                          (tc/add-column :active-plans? true)
                          (tc/set-dataset-name "active-plans"))
                      [#_:sen2-table-id #_:person-table-id :requests-table-id])
        (tc/drop-columns #"^:active-plans\..+$")
        ;;
        ;; Merge in `personal` details, inc. age & NCY for census dates (also bring in other `census-dates-ds` columns here)
        (tc/left-join (-> person-on-census-dates-ds
                          (tc/select-columns (concat sen2-blade-table-id-col-names
                                                     (:person sen2-blade-module-cols-to-select)
                                                     (tc/column-names census-dates-ds)
                                                     [:age-at-start-of-school-year :ncy-nominal]))
                          (tc/add-column :person? true)
                          (tc/set-dataset-name "person"))
                      [:sen2-table-id :person-table-id :census-date])
        (tc/drop-columns #"^:person\..+$")
        ;; Arrange dataset
        (tc/reorder-columns (distinct (concat sen2-blade-table-id-col-names
                                              [                  ] (tc/column-names census-dates-ds)
                                              [:person?          ] (sen2-blade-module-cols-to-select :person) [:age-at-start-of-school-year :ncy-nominal]
                                              [:named-plan?      ] (sen2-blade-module-cols-to-select :named-plan)
                                              [:active-plans?    ] (sen2-blade-module-cols-to-select :active-plans)
                                              [:placement-detail?] (sen2-blade-module-cols-to-select :placement-detail)
                                              [:sen-need?        ] (sen2-blade-module-cols-to-select :sen-need))))
        (tc/order-by [:person-table-id :census-date :requests-table-id])
        (tc/set-dataset-name "plans-placements-on-census-dates"))))

(defn plans-placements-on-census-dates-col-name->label
  "Column labels for display, given mappings of col-name->label for `person`, `named-plan`,
  `placement-detail`, `active-plans`, `sen-need` and `census-dates` datasets."
  [& {person-col-name->label           :person
      named-plan-col-name->label       :named-plan
      placement-detail-col-name->label :placement-detail
      active-plans-col-name->label     :active-plans
      sen-need-col-name->label         :sen-need
      census-dates-col-name->label     :census-dates}]
  (merge  census-dates-col-name->label
          (person-on-census-dates-col-name->label :person person-col-name->label)
          (named-plan-on-census-dates-col-name->label :named-plan named-plan-col-name->label)
          (placement-detail-on-census-dates-col-name->label :placement-detail placement-detail-col-name->label)
          active-plans-col-name->label
          (sen-need-primary-col-name->label :sen-need sen-need-col-name->label)
          {:person?           "Got CYP details from `person`?"
           :named-plan?       "Got named plan from `named-plan`?"
           :active-plans?     "Got active plan from `active plans`?"
           :placement-detail? "Got placement details from `placement-detail`?"
           :sen-need?         "Got an EHCP primary need from `sen-need`?"}))



;;; # Checks
(defn checks
  "Definitions of checks for issues in dataset of plans & placements on census dates.
   Optional trailing key-value parameters are as follows:
   - `sen-settings`: custom set of valid `sen-setting` abbreviations to check against:
     - Defaults if not specified to standard `sen2-dictionary/sen-settings`.
   - `sen-types`: custom set of valid `sen-type` abbreviations to check against:
     - Defaults if not specified to standard `sen2-dictionary/sen-types`.
   - `edubaseall-send-map`: specific map for GIAS based checks:
     - This should map (string) URNs to maps containing at least:
       - `:type-of-establishment-name`
       - `:sen-unit?`
       - `:resourced-provision?`
     - Specify as `nil` to suppress GIAS based checks.
     - Defaults if not specified to standard map from `gias` lib.
   - `sen2-estab-min-expected-placed`: map specifying expected minimum numbers of CYP placed:
     - If specified, then the `sen2-estab`s included have the number of CYP placed on
       each `:census-date` calculated and those that have less than the minimum
       number specified are flagged as issues with the number placed.
     - The map should be keyed by `sen2-estab` maps
       (with keys {`:urn` `:ukprn` `:sen-unit-indicator` `:resourced-provision-indicator` `:sen-setting`})
       with values the minimum expected number placed.
   - `additional-checks`: map of additional checks to include.
   - `checks-to-omit`: seq of (keywords for) checks to omit:
     - There is some duplication in the checks when running on plans-placements, due to the
       inclusion of checks that work on the more limited set of columns remaining after applying updates.
     - There are also some checks that should be guaranteed by appropriate construction of the census-dates.
     - This option allows for checks to be `dissoc`ed prior to ordering the map."
  [& {:keys [sen-settings
             sen-types
             edubaseall-send-map
             sen2-estab-min-expected-placed
             additional-checks
             checks-to-omit]
      :or   {sen-settings        sen2-dictionary/sen-settings
             sen-types           sen2-dictionary/sen-types
             edubaseall-send-map (gias/edubaseall-send->map)}}]
  (let [count-true? #(comp count (partial filter true?) %)]
    ;; Check definitions:
    ;; - `:idx`:           index to order checks for reporting.
    ;; - `:label`:         description of issue used in reporting.
    ;; - `:cols-required`: set of columns required to proceed with the check
    ;; - `:col-fn`:        check function supplied to tc/add-column so receives dataset as single argument and
    ;;                     must return either a column, sequence or single value with truthy values indicating an issue.
    ;; - `:summary-fn`:    function used by `summarise-issues` as an aggregation function to summarise the issue
    ;;                     within (i.e. grouped by) `:census-year`, so must return a single value.
    ;; - `:summary-label`: label used by `summarise-issues` to indicate what has been counted by the `summary-fn`
    ;; - `:action`:        string describing action to take to investigate flagged issues.
    (cond-> {}
      true ; 1##: Define checks for ID and unique-key columns
      (assoc :issue-missing-person-table-id
             {:idx           112
              :label         "Missing :person-table-id"
              :cols-required #{:person-table-id}
              :col-fn        #(->> % :person-table-id (map nil?))
              :summary-fn    (count-true? :issue-missing-person-table-id)
              :summary-label "#rows"
              :action        "Required: fill in the blanks."}
             :issue-missing-census-year
             {:idx           122
              :label         "Missing :census-year"
              :cols-required #{:census-year}
              :col-fn        #(->> % :census-year (map nil?))
              :summary-fn    (count-true? :issue-missing-census-year)
              :summary-label "#rows"
              :action        "Required: fill in the blanks."}
             :issue-missing-census-date
             {:idx           124
              :label         "Missing :census-date"
              :cols-required #{:census-date}
              :col-fn        #(->> % :census-date (map nil?))
              :summary-fn    (count-true? :issue-missing-census-date)
              :summary-label "#rows"
              :action        "Required: fill in the blanks."}
             :issue-missing-requests-table-id
             {:idx           132
              :label         "Missing :requests-table-id"
              :cols-required #{:requests-table-id}
              :col-fn        #(->> % :requests-table-id (map nil?))
              :summary-fn    (count-true? :issue-missing-requests-table-id)
              :summary-label "#rows"
              :action        "Required: fill in the blanks."}
             :issue-non-unique-key
             {:idx           134
              :label         "[:person-table-id :census-date :requests-table-id] not unique key"
              :cols-required #{:person-table-id :census-date :requests-table-id}
              :col-fn        (fn [ds] (-> ds
                                          (tc/group-by [:person-table-id :census-date :requests-table-id])
                                          (tc/add-column :row-count tc/row-count)
                                          (tc/ungroup)
                                          (tc/map-columns :issue [:row-count] (partial < 1))
                                          :issue))
              :summary-fn    #(-> %
                                  (tc/select-rows :issue-non-unique-key)
                                  (tc/unique-by [:person-table-id :census-date :requests-table-id])
                                  tc/row-count)
              :summary-label "#keys"
              :action        "Should be unique."}
             :issue-missing-upn
             {:idx           142
              :label         "Missing UPN, preventing joining with other SEN2 returns"
              :cols-required #{:upn}
              :col-fn        #(->> % :upn (map nil?))
              :summary-fn    (count-true? :issue-missing-upn)
              :summary-label "#rows"
              :action        "Complete with unique ID or consider removing non-new EHCPs without a UPN."})
      true ; 2##: Define checks for CYP details
      (assoc :issue-multiple-requests
             {:idx           212
              :label         "CYP has plans|placements from multiple requests"
              :cols-required #{:person-table-id :census-date}
              :col-fn        (fn [ds] (-> ds
                                          (tc/group-by [:person-table-id :census-date])
                                          (tc/add-column :row-count tc/row-count)
                                          (tc/ungroup)
                                          (tc/map-columns :issue [:row-count] (partial < 1))
                                          :issue))
              :summary-fn    (comp count distinct :person-table-id
                                   #(tc/select-rows % :issue-multiple-requests))
              :summary-label "#CYP"
              :action        "Drop all but one."}
             :issue-unknown-age-at-start-of-school-year
             {:idx           224
              :label         "Could not calculate age at the start of the school year"
              :cols-required #{:age-at-start-of-school-year}
              :col-fn        #(->> % :age-at-start-of-school-year (map nil?))
              :summary-fn    (comp count distinct :person-table-id
                                   #(tc/select-rows % :issue-unknown-age-at-start-of-school-year))
              :summary-label "#CYP"
              :action        "Get DoB and/or otherwise assign NCY."}
             :issue-not-send-age
             {:idx           232
              :label         "CYP outside SEND age at start of the school year"
              :cols-required #{:age-at-start-of-school-year}
              :col-fn        #(->> % :age-at-start-of-school-year
                                   (map (complement (partial contains? (conj (apply sorted-set (range 0 25)) nil)))))
              :summary-fn    (count-true? :issue-not-send-age)
              :summary-label "#rows"
              :action        "Consider dropping."}
             :issue-missing-ncy-nominal
             {:idx           234
              :label         "Missing NCY (nominal)"
              :cols-required #{:ncy-nominal}
              :col-fn        #(->> % :ncy-nominal (map nil?))
              :summary-fn    (comp count distinct :person-table-id
                                   #(tc/select-rows % :issue-missing-ncy-nominal))
              :summary-label "#CYP"
              :action        "Required: Assign, impute or drop."}
             :issue-invalid-ncy-nominal
             {:idx           236
              :label         "Invalid (non nil) NCY"
              :cols-required #{:ncy-nominal}
              :col-fn        #(->> % :ncy-nominal
                                   (map (complement (partial contains? (conj (apply sorted-set (range -4 21)) nil)))))
              :summary-fn    (count-true? :issue-invalid-ncy-nominal)
              :summary-label "#rows"
              :action        "Consider dropping."})
      true ; 3##: Define checks for named-plan
      (assoc :issue-no-named-plan
             {:idx           312
              :label         "No named-plan on census date"
              :cols-required #{:named-plan?}
              :col-fn        #(->> % :named-plan? (map not))
              :summary-fn    (count-true? :issue-no-named-plan)
              :summary-label "#rows"
              :action        (str "Not an issue for modelling as don't need any details from named-plan: "
                                  "Flag to client as incomplete/incoherent data and consider dropping.")})
      true ; 4##: Define checks for placement-detail
      (assoc :issue-no-placement-detail
             {:idx           412
              :label         "No placement-detail on census date"
              :cols-required #{:placement-detail?}
              :col-fn        #(->> % :placement-detail? (map not))
              :summary-fn    (count-true? :issue-no-placement-detail)
              :summary-label "#rows"
              :action        (str "Drop if transferred in after `:census-date`, "
                                  "otherwise get `placement-detail` to determine setting.")}
             :issue-no-placement-detail-transferred-in
             {:idx           414
              :label         "No placement-detail - transferred in"
              :cols-required #{:placement-detail? :transfer-la}
              :col-fn        (fn [{:keys [placement-detail? transfer-la]}]
                               (map #(and (not %1) (some? %2)) placement-detail? transfer-la))
              :summary-fn    (count-true? :issue-no-placement-detail-transferred-in)
              :summary-label "#rows"
              :action        (str "Drop if transferred in after `:census-date`, "
                                  "otherwise get `placement-detail` to determine setting.")}
             :issue-no-placement-detail-not-transferred-in
             {:idx           416
              :label         "No placement-detail - not transferred in"
              :cols-required #{:placement-detail? :transfer-la}
              :col-fn        (fn [{:keys [placement-detail? transfer-la]}]
                               (map #(and (not %1) (nil? %2)) placement-detail? transfer-la))
              :summary-fn    (count-true? :issue-no-placement-detail-not-transferred-in)
              :summary-label "#rows"
              :action        "Get `placement-detail` to determine setting."}
             :issue-placement-detail-missing-sen2-estab
             {:idx           422
              :label         "Placement detail but missing SEN2 Estab"
              :cols-required #{:placement-detail? :urn :ukprn :sen-setting}
              :col-fn        (fn [{:keys [placement-detail? urn ukprn sen-setting]}]
                               (map #(and %1 (every? nil? [%2 %3 %4])) placement-detail? urn ukprn sen-setting))
              :summary-fn    (count-true? :issue-placement-detail-missing-sen2-estab)
              :summary-label "#rows"
              :action        "Specify urn|ukprn (with indicators) or sen-setting."})
      true ; 5##: Define checks for sen2-estab
      (assoc :issue-missing-sen2-estab
             {:idx           512
              :label         "Missing placement SEN2 Estab"
              :cols-required #{:urn :ukprn :sen-setting}
              :col-fn        (fn [{:keys [urn ukprn sen-setting]}]
                               (map #(every? nil? %&) urn ukprn sen-setting))
              :summary-fn    (count-true? :issue-missing-sen2-estab)
              :summary-label "#rows"
              :action        "Specify urn|ukprn (with indicators) or sen-setting."}
             :issue-sen2-estab-indicator-not-set
             {:idx           514
              :label         "URN|UKPRN|SENsetting specified but SENU & RP indicators not set"
              :cols-required #{:urn :ukprn :sen-unit-indicator :resourced-provision-indicator :sen-setting}
              :col-fn        (fn [ds] (-> ds
                                          (tc/map-columns :issue [:urn :ukprn :sen-unit-indicator :resourced-provision-indicator :sen-setting]
                                                          #(and (not-every? nil?     [%1 %2 %5])
                                                                (not-every? boolean? [%3 %4])))
                                          :issue))
              :summary-fn    (count-true? :issue-sen2-estab-indicator-not-set)
              :summary-label "#rows"
              :action        "Specify sen-unit-indicator & resourced-provision-indicator."}
             :issue-invalid-sen-setting
             {:idx           592
              :label         "Invalid (non-nil) sen-setting"
              :cols-required #{:sen-setting}
              :col-fn        #(->> % :sen-setting (map (complement (partial contains? (conj sen-settings nil)))))
              :summary-fn    (count-true? :issue-invalid-sen-setting)
              :summary-label "#rows"
              :action        "Assign a recognised sen-setting."})
      (seq edubaseall-send-map) ; 52#: Add GIAS based checks for establishment type
      (assoc :issue-urn-for-unexpected-gfe-gias-establishment-type
             {:idx           522
              :label         "URN has GFE GIAS estab. type unexpected for SEND"
              :cols-required #{:urn}
              :col-fn        (fn [{:keys [urn]}]
                               (map (comp #{"Higher education institutions"
                                            "University technical college"}
                                          :type-of-establishment-name
                                          edubaseall-send-map)
                                    urn))
              :summary-fn    (comp count distinct :urn
                                   #(tc/select-rows % :issue-urn-for-unexpected-gfe-gias-establishment-type))
              :summary-label "#URNs"
              :action        "Confirm the placement-detail is correct or update."}
             :issue-urn-for-unexpected-othe-gias-establishment-type
             {:idx           524
              :label         "URN has OTHE GIAS estab. type unexpected for SEND"
              :cols-required #{:urn}
              :col-fn        (fn [{:keys [urn]}]
                               (map (comp #{"Online provider"
                                            "Service children's education"
                                            "Offshore schools"
                                            "British schools overseas"
                                            "Institution funded by other government department"
                                            "Miscellaneous"}
                                          :type-of-establishment-name
                                          edubaseall-send-map)
                                    urn))
              :summary-fn    (comp count distinct :urn
                                   #(tc/select-rows % :issue-urn-for-unexpected-othe-gias-establishment-type))
              :summary-label "#URNs"
              :action        "Confirm the placement-detail is correct or update."})
      (seq edubaseall-send-map) ; 53#: Add GIAS based checks for SENU & RP indicators
      (assoc :issue-senu-flagged-at-estab-without-one
             {:idx           532
              :label         "SEN unit flagged at estab. other than URNs GIAS says has them."
              :cols-required #{:urn :sen-unit-indicator}
              :col-fn        (fn [{:keys [urn sen-unit-indicator]}]
                               (map #(and (->> %1
                                               (get edubaseall-send-map)
                                               :sen-unit?
                                               boolean
                                               false?)
                                          (->> %2
                                               boolean
                                               true?))
                                    urn sen-unit-indicator))
              :summary-fn    (fn [ds] (-> ds
                                          (tc/select-rows :issue-senu-flagged-at-estab-without-one)
                                          (tc/unique-by [:urn :ukprn :sen-setting])
                                          tc/row-count))
              :summary-label "#Estabs"
              :action        "Review SEN unit flagging for these establishment(s) and correct if necessary."}
             :issue-resourced-provision-flagged-at-estab-without-one
             {:idx           534
              :label         "RP flagged at estab. other than URNs GIAS says has them."
              :cols-required #{:urn :resourced-provision-indicator}
              :col-fn        (fn [{:keys [urn resourced-provision-indicator]}]
                               (map #(and (->> %1
                                               (get edubaseall-send-map)
                                               :resourced-provision?
                                               boolean
                                               false?)
                                          (->> %2
                                               boolean
                                               true?))
                                    urn resourced-provision-indicator))
              :summary-fn    (fn [ds] (-> ds
                                          (tc/select-rows :issue-resourced-provision-flagged-at-estab-without-one)
                                          (tc/unique-by [:urn :ukprn :sen-setting])
                                          tc/row-count))
              :summary-label "#Estabs"
              :action        "Review resourced provision flagging for these establishment(s) and correct if necessary."}
             )
      (seq sen2-estab-min-expected-placed) ; 54#: Add checks have at least minimum numbers placed.
      (assoc :issue-less-placements-than-expected
             {:idx           542
              :label         "SEN2 estab. has fewer placed than expected"
              :cols-required #{:census-year :urn :ukprn :sen-unit-indicator :resourced-provision-indicator :sen-setting}
              :col-fn        (fn [ds]
                               (let [cy-sen2-estab-with-less-than-expected-placed
                                     (-> ds
                                         (tc/join-columns :sen2-estab sen2-estab-keys {:result-type :map})
                                         (tc/select-rows (comp (-> sen2-estab-min-expected-placed keys set) :sen2-estab))
                                         (tc/group-by [:census-year :sen2-estab])
                                         (tc/aggregate {:num-placed tc/row-count})
                                         (tc/select-rows #(< (% :num-placed)
                                                             (get sen2-estab-min-expected-placed (% :sen2-estab))))
                                         ((fn [ds] (zipmap (-> ds (tc/drop-columns [:num-placed]) (tc/rows :as-maps))
                                                           (-> ds :num-placed)))))]
                                 (-> ds
                                     (tc/join-columns :sen2-estab sen2-estab-keys {:result-type :map})
                                     (tc/join-columns :cy-sen2-estab [:census-year :sen2-estab] {:result-type :map})
                                     (tc/map-columns :num-placed [:cy-sen2-estab] cy-sen2-estab-with-less-than-expected-placed)
                                     :num-placed)))
              :summary-fn    (fn [ds] (-> ds
                                          (tc/select-rows :issue-less-placements-than-expected)
                                          (tc/unique-by sen2-estab-keys)
                                          tc/row-count))
              :summary-label "#SEN2-Estabs"
              :action        "Check not missing any placements or SENU|RP flagging."})
      true ; 6##: Define checks for sen-need
      (assoc :issue-missing-sen-type
             {:idx           612
              :label         "Missing sen-type (EHCP need)"
              :cols-required #{:sen-type}
              :col-fn        #(->> % :sen-type (map nil?))
              :summary-fn    (count-true? :issue-missing-sen-type)
              :summary-label "#rows"
              :action        "Get sen-type (or will be considered unknown)."}
             :issue-invalid-sen-type
             {:idx           614
              :label         "Invalid (non-nil) sen-type (EHCP need)"
              :cols-required #{:sen-type}
              :col-fn        #(->> % :sen-type (map (complement (partial contains? (conj sen-types nil)))))
              :summary-fn    (count-true? :issue-invalid-sen-type)
              :summary-label "#rows"
              :action        "Correct or consider as custom EHCP primary need."})
      (seq additional-checks) ; Add `additional-checks`
      (merge additional-checks)
      (seq checks-to-omit)
      (#(apply dissoc % checks-to-omit))
      true ; Put into sorted map in `:idx` order
      ((fn [m] (into (sorted-map-by (fn [k1 k2] (compare [(get-in m [k1 :idx]) k1]
                                                         [(get-in m [k2 :idx]) k2]))) m))))))

(def checks-totals
  "Additional summaries of #rows and #CYP that can be merged onto checks."
  {:issue-row-count {:idx           996
                     :label         "TOTAL number of records with issues flagged"
                     :summary-fn    #(-> %
                                         (tc/join-columns :issue? #"^:issue-.*$" {:result-type (partial some boolean)})
                                         (tc/select-rows :issue?)
                                         tc/row-count)
                     :summary-label "#rows"}
   :issue-num-cyp   {:idx           997
                     :label         "TOTAL number of CYP with issues flagged"
                     :summary-fn    #(-> %
                                         (tc/join-columns :issue? #"^:issue-.*$" {:result-type (partial some boolean)})
                                         (tc/select-rows :issue?)
                                         (tc/unique-by [:person-table-id])
                                         tc/row-count)
                     :summary-label "#CYP"}
   :total-row-count {:idx           998
                     :label         "TOTAL number of records"
                     :summary-fn    tc/row-count
                     :summary-label "#rows"
                     }
   :total-num-cyp   {:idx           999
                     :label         "TOTAL number of CYP"
                     :summary-fn    (comp count distinct :person-table-id)
                     :summary-label "#CYP"}})

(defn flag-issues
  "Run `checks` on dataset `ds`, adding issue flag columns."
  [ds checks]
  (as-> ds $
    (reduce-kv (fn [ds' k v]
                 (if (set/subset? (:cols-required v) (set (tc/column-names ds')))
                   (tc/add-column ds' k (:col-fn v))
                   ds'))
               $
               checks)
    (tc/set-dataset-name $ (str (tc/dataset-name $) "-issues-flagged"))))

(defn plans-placements-on-census-dates-issues-flagged-col-name->label
  "Column labels for display"
  [& {:keys [plans-placements-on-census-dates-col-name->label
             checks]}]
  (merge plans-placements-on-census-dates-col-name->label
         (update-vals checks :label)))

(def names-of-update-cols
  "Names of columns to add to issues dataset for updates."
  [:update-drop?
   :update-upn
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
   rows with issues flagged by `checks`,
   selected columns for review,
   and blank columns for manual updates."
  [plans-placements-on-census-dates-issues-flagged checks
   & {:keys [sen2-blade-module-cols-to-select]
      :or   {sen2-blade-module-cols-to-select sen2-blade-module-cols-to-select}}]
  (-> plans-placements-on-census-dates-issues-flagged
      ;; Select only rows with an issue flagged:
      (tc/select-rows #((apply some-fn (keys checks)) %))
      ;; Add empty columns for updated data:
      (tc/add-columns (zipmap names-of-update-cols (repeat nil)))
      ;; Select columns in sensible order for manual review of issues:
      (tc/select-columns (distinct (concat
                                    ;; Person ID columns
                                    [:person-table-id] (:person sen2-blade-module-cols-to-select)
                                    ;; Census year & date
                                    [:census-year :census-date]
                                    ;; Age & NCY
                                    [:age-at-start-of-school-year :ncy-nominal]
                                    ;; Request ID (as may vary by `:census-year`)
                                    [:requests-table-id]
                                    ;; Issue flags
                                    (keys checks)
                                    ;; Blank columns for updates
                                    names-of-update-cols
                                    ;; Plan info from SEN2 `named-plan` module
                                    [:named-plan?] (:named-plan sen2-blade-module-cols-to-select)
                                    ;; Active plan info from SEN2 `active-plans` module
                                    [:active-plans?] (:active-plans sen2-blade-module-cols-to-select)
                                    ;; Placement info from SEN2 `placement-detail` module
                                    [:placement-detail?] (:placement-detail sen2-blade-module-cols-to-select)
                                    sen2-estab-keys
                                    ;; SEN need info from SEN2 `sen-need` module
                                    [:sen-need?] (:sen-need sen2-blade-module-cols-to-select))))
      (tc/order-by [:person-table-id :census-date :requests-table-id])
      (tc/set-dataset-name "plans-placements-on-census-dates-issues")))

(defn plans-placements-on-census-dates-issues-col-name->label
  "Column labels for display."
  [& {:keys [plans-placements-on-census-dates-col-name->label
             checks]}]
  (merge plans-placements-on-census-dates-col-name->label
         (update-vals checks :label)
         (reduce (fn [m k] (assoc m k (-> k
                                          name
                                          (#(string/replace % "update-" ""))
                                          (#(plans-placements-on-census-dates-col-name->label (keyword %) %))
                                          ((partial str "Update: ")))))
                 {} names-of-update-cols)))

(defn issues->ds
  "Run `checks` on `plans-placements-on-census-dates` dataset, extracting
   rows with issues flagged by `checks'`,
   selected columns for review,
   and blank columns for manual updates."
  [plans-placements-on-census-dates checks & {:as opts}]
  (-> plans-placements-on-census-dates
      (flag-issues        checks)
      (flagged-issues->ds checks opts)))

(defn drop-falsey-issue-columns
  "Given issues dataset `ds` and (optional) column-selector for issue columns,
   returns dataset without issue columns for which all records have falsey values."
  ([ds]
   (drop-falsey-issue-columns ds #"^:issue-.+$"))
  ([ds issue-column-selector]
   (tc/drop-columns ds (->> (tc/select-columns ds issue-column-selector)
                            (filter (comp (partial every? (comp false? boolean)) last))
                            keys))))

(defn summarise-issues
  "Summarise issues flagged in `ds` as a result of running `checks`."
  [ds checks]
  (cond
    (= 0 (tc/row-count ds))
    (tc/dataset nil {:column-names ["key"
                                    "Index"
                                    "Issue | TOTAL"
                                    "Summary"]
                     :dataset-name "issue-summary"})
    :else
    (-> ds
        (tc/group-by [:census-date])
        ((fn [ds] (tc/aggregate ds (update-vals (select-keys checks (concat (tc/column-names ds)
                                                                            (keys checks-totals)))
                                                :summary-fn))))
        (tc/order-by [:census-date])
        (tc/update-columns [:census-date] (partial map #(.format % (java.time.format.DateTimeFormatter/ISO_LOCAL_DATE))))
        (tc/pivot->longer (complement #{:census-date}))
        (tc/pivot->wider :census-date :$value)
        (tc/rename-columns {:$column :issue-key})
        (tc/map-columns :idx           [:issue-key] (update-vals checks :idx))
        (tc/map-columns :label         [:issue-key] (update-vals checks :label))
        (tc/map-columns :summary-label [:issue-key] (update-vals checks :summary-label))
        (tc/order-by [:idx])
        (tc/reorder-columns [:issue-key :idx :label :summary-label])
        (tc/drop-columns [:issue-key])
        (tc/rename-columns {:issue-key     "key"
                            :idx           "Index"
                            :label         "Issue | TOTAL"
                            :summary-label "Summary"})
        (tc/set-dataset-name "issue-summary"))))



;;; # CSV file read
(def parser-fn
  "Parser function for reading plans-placements-on-census-dates from CSV files."
  {:sen2-table-id                     :string
   :person-table-id                   :string
   :requests-table-id                 :string
   :assessment-table-id               :string
   :named-plan-table-id               :string
   :active-plans-table-id             :string
   :placement-detail-table-id         :string
   :sen-need-table-id                 :string
   :census-year                       :int16
   :census-date                       :local-date
   :person?                           :boolean
   :person-order-seq-column           :int32
   :upn                               :string
   :unique-learner-number             :string
   :person-birth-date                 :local-date
   :age-at-start-of-school-year       [:int8 parse-long]
   :ncy-nominal                       [:int8 parse-long]
   :named-plan?                       :boolean
   :named-plan-order-seq-column       :int32
   :start-date                        :local-date
   :cease-date                        :local-date
   :cease-reason                      [:int8 parse-long]
   :active-plans?                     :boolean
   :active-plans-order-seq-column     :int32
   :transfer-la                       :string
   :placement-detail?                 :boolean
   :placement-detail-order-seq-column :int32
   :urn                               :string
   :ukprn                             :string
   :sen-setting-other                 :string
   :placement-rank                    [:int8 parse-long]
   :entry-date                        :local-date
   :leaving-date                      :local-date
   :sen-unit-indicator                :boolean
   :resourced-provision-indicator     :boolean
   :sen-setting                       :string
   :sen-need?                         :boolean
   :sen-need-order-seq-column         :int32
   :sen-type-rank                     [:int8 parse-long]
   :sen-type                          :string})

(defn csv-file->ds
  "Read columns required to construct census
   from CSV file of `plans-placements-on-census-dates` into a dataset."
  [filepath & {:keys [column-allowlist key-fn parser-fn]
               :or   {column-allowlist (map name cols-to-keep)
                      key-fn           keyword
                      parser-fn        parser-fn}}]
  (tc/dataset filepath
              {:column-allowlist column-allowlist
               :key-fn           key-fn
               :parser-fn        parser-fn}))

(def updates-ds-required-cols
  "Required columns for updates dataset."
  (concat [:person-table-id
           :census-date
           :requests-table-id]
          (filter (complement #{:update-notes}) names-of-update-cols)))

(defn updates-csv-file->ds
  "Read columns required to update columns for census
   from CSV file of `plans-placements-on-census-dates-issues`
   with update columns filled in into a dataset."
  [filepath & {:keys [column-allowlist key-fn parser-fn]
               :or   {column-allowlist (map name updates-ds-required-cols)
                      key-fn           keyword
                      parser-fn        (merge (select-keys parser-fn [:person-table-id
                                                                      :census-date
                                                                      :census-year
                                                                      :requests-table-id])
                                              {:update-drop? :boolean}
                                              (-> parser-fn
                                                  (select-keys [:ncy-nominal
                                                                :urn
                                                                :ukprn
                                                                :sen-unit-indicator
                                                                :resourced-provision-indicator
                                                                :sen-setting
                                                                :sen-type
                                                                :upn])
                                                  (update-keys (comp keyword (partial str "update-") name))))}}]
  (tc/dataset filepath
              {:column-allowlist column-allowlist
               :key-fn           key-fn
               :parser-fn        parser-fn}))



;;; # Updating
(defn summarise-updates
  "Summarise updates in `plans-placements-on-census-dates-updates` dataset."
  [plans-placements-on-census-dates-updates]
  (-> plans-placements-on-census-dates-updates
      (tc/map-columns :update-sen2-estab [:update-urn
                                          :update-ukprn
                                          :update-sen-unit-indicator
                                          :update-resourced-provision-indicator
                                          :update-sen-setting]
                      (fn [& args] (some some? args)))
      (tc/update-columns [:census-date] (partial map #(.format % (java.time.format.DateTimeFormatter/ISO_LOCAL_DATE))))
      (tc/update-columns [:update-drop?] (partial map #(if % "X" " ")))
      (tc/update-columns #"^:update-[^\?]*" (partial map #(if (some? %) "*" " ")))
      (tc/group-by [:census-date :update-drop? :update-upn :update-ncy-nominal :update-sen2-estab :update-sen-type])
      (tc/aggregate {:row-count tc/row-count})
      (tc/pivot->wider :census-date :row-count {:drop-missing? false})
      (as-> $ (tc/order-by $ (tc/column-names $ #"^:update-.+$") :desc))
      (tc/rename-columns {:update-drop?       "drop?"
                          :update-upn         "UPN"
                          :update-ncy-nominal "Nominal NCY"
                          :update-sen2-estab  "SEN2 Establishment"
                          :update-sen-type    "sen-type (need)"})))

(defn update-plans-placements-on-census-dates
  "Apply updates from `plans-placements-on-census-dates-updates'` to `plans-placements-on-census-dates'`.
   Trailing map/key-value arguments allow specification of columns to keep:
   - `:cols-to-keep` - seq of column names to keep (default `cols-to-keep`)
   - `:additional-placement-detail-cols-to-keep` - seq of additional columns to keep from placement-detail module:
      - Contents are `nil`led if other `placement-detail` columns are updated.
      - These columns are added to the `:cols-to-keep` parameter value (if not already present)."
  [plans-placements-on-census-dates'
   plans-placements-on-census-dates-updates'
   & {:keys [cols-to-keep
             additional-placement-detail-cols-to-keep]
      :or   {cols-to-keep cols-to-keep}}]
  (let [;; Filter `tc/column-names` to ensure cols-to-keep are present and in order
        cols-to-keep'                             (filter (into #{} cat [cols-to-keep
                                                                         additional-placement-detail-cols-to-keep])
                                                          (tc/column-names plans-placements-on-census-dates'))
        additional-placement-detail-cols-to-keep' (filter (set additional-placement-detail-cols-to-keep)
                                                          (tc/column-names plans-placements-on-census-dates'))]
    (-> plans-placements-on-census-dates'
        (tc/select-columns cols-to-keep')
        (tc/left-join (-> plans-placements-on-census-dates-updates'
                          (tc/select-columns updates-ds-required-cols)
                          (tc/set-dataset-name "update"))
                      [:person-table-id :census-date :requests-table-id])
        (tc/drop-columns #"^:update\..*$")
        ;; Drop records with `:update-drop?`
        (tc/drop-rows :update-drop?)
        ;; Update `:upn` if present and `:update-upn` is non-nil in update dataset
        ((fn [ds] (if (not-any? #{:upn} (tc/column-names ds))
                    ds
                    (tc/map-columns ds :upn
                                    [:update-upn :upn]
                                    #(if (some? %1) %1 %2)))))
        ;; Update `:ncy-nominal` if non-nil in update dataset
        (tc/map-rows (fn [{:keys [ncy-nominal update-ncy-nominal]}]
                       (if (some? update-ncy-nominal)
                         {:ncy-nominal update-ncy-nominal}
                         {:ncy-nominal ncy-nominal})))
        ;; Update all placement-detail module SEN2 Establishment columns if any are non nil in update dataset
        (tc/map-rows (fn [{:keys [urn                           update-urn
                                  ukprn                         update-ukprn
                                  sen-unit-indicator            update-sen-unit-indicator
                                  resourced-provision-indicator update-resourced-provision-indicator
                                  sen-setting                   update-sen-setting]
                           :as   r}]
                       (if (some some? [update-urn
                                        update-ukprn
                                        update-sen-unit-indicator
                                        update-resourced-provision-indicator
                                        update-sen-setting])
                         (merge (zipmap additional-placement-detail-cols-to-keep' (repeat nil))
                                {:urn                           update-urn
                                 :ukprn                         update-ukprn
                                 :sen-unit-indicator            (if (some? update-sen-unit-indicator)
                                                                  update-sen-unit-indicator
                                                                  false)
                                 :resourced-provision-indicator (if (some? update-resourced-provision-indicator)
                                                                  update-resourced-provision-indicator
                                                                  false)
                                 :sen-setting                   update-sen-setting})
                         (merge (select-keys r additional-placement-detail-cols-to-keep')
                                {:urn                           urn
                                 :ukprn                         ukprn
                                 :sen-unit-indicator            sen-unit-indicator
                                 :resourced-provision-indicator resourced-provision-indicator
                                 :sen-setting                   sen-setting}))))
        ;; Update `:sen-type` if non-nil in update dataset
        (tc/map-rows (fn [{:keys [sen-type update-sen-type]}]
                       (if (some? update-sen-type)
                         {:sen-type update-sen-type}
                         {:sen-type sen-type})))
        ;; Drop update columns
        (tc/drop-columns #"^:update-.*$")
        ;; Arrange dataset
        (tc/order-by [:person-table-id :census-date :requests-table-id])
        (tc/set-dataset-name "plans-placements-on-census-dates-updated"))))



;;; # Combining
(defn concat-plans-placements-on-census-dates
  "Given sequence `xs` of datasets of plans-placements-on-census-dates,
   each containing a `:census-year` column, adds column `:return-year`
   (as the maximum `:census-year`) and concatenates them."
  [xs & {:keys [person-id-col-name]
         :or   {person-id-col-name :person-table-id}}]
  (as-> xs $
    (map (fn [ds]
           (-> ds
               (tc/add-column :return-year #(->> % :census-year (reduce max)))
               (tc/reorder-columns [:return-year])))
         $)
    (reduce tc/concat-copying $)
    (tc/reorder-columns $ [person-id-col-name :census-year :census-date :return-year])
    (tc/order-by $ (tc/column-names $))))

(defn concatenated-plans-placements->side-by-side
  "Given dataset `ds` of concatenated plans-placements-on-census-dates
  (with column `:return-year` indicating the SEN2 return year each record was extracted from)
  returns a dataset keyed by [`person-id-col-name` `:census-year` `:census-date`]
  with the value columns joined into a map and placed in one of two columns:
   - `:census-year-return` for records with `:census-year`   matching the `:return-year`
   - `:next-year-return`   for records with `:census-year`+1 matching the `:return-year`.
  The default value columns are [`:sen-type``:ncy-nominal`
  `:urn` `:ukprn` `:sen-unit-indicator` `:resourced-provision-indicator` `:sen-setting`]
  (with the latter packed into map under :sen2-estab) may be added to
  using option `additional-val-cols`."
  [ds & {:keys [person-id-col-name
                additional-val-cols]
         :or   {person-id-col-name :person-table-id}}]
  (-> ds
      (tc/map-columns :val-col-name [:census-year :return-year] #(cond (= %1    %2   ) :census-year-return
                                                                       (= %1 (- %2 1)) :next-year-return
                                                                       :else           :other))
      (tc/join-columns :sen2-estab sen2-estab-keys {:result-type :map})
      (tc/join-columns :val-col
                       ((comp distinct concat) val-cols-to-keep [:sen2-estab] additional-val-cols)
                       {:result-type :map})
      (tc/select-columns [person-id-col-name :census-year :census-date :val-col-name :val-col])
      (tc/pivot->wider :val-col-name :val-col {:drop-missing? false})
      (tc/reorder-columns [person-id-col-name :census-year :census-date :census-year-return :next-year-return])
      (tc/order-by [person-id-col-name :census-year])))

