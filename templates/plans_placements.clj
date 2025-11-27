(ns plans-placements
  "Extract & check plans & placements on census dates from SEN2 Blade."
  (:require [tablecloth.api :as tc]
            [witan.gias :as gias]
            [witan.sen2 :as sen2]
            [witan.sen2.return.person-level.blade.plans-placements :as sen2-blade-plans-placements]
            [sen2-blade-csv :as sen2-blade] ; <- replace with workpackage specific version
            ))


;;; # Parameters
;;; ## Output directory
(def out-dir
  "Output directory"
  "./tmp/")

(def census-years
  "Years of SEN2 census dates on which to extract plans & placements."
  (-> @sen2-blade/return-year sen2/return-year->census-years))

(def sen2-blade-suffix
  "Suffix (to identify source SEN2 blade) to append to dataset and output file names."
  (str "-" @sen2-blade/return-year))


;;; ## Census dates
(def census-dates-ds
  "Dataset with column `:census-date` of dates to extract open plans & placements on."
  (sen2/census-years->census-dates-ds census-years))



;;; # Extract plans & placements on census dates
(def sen2-blade-module-cols-to-select
  "Map of SEN2 Blade columns to select for inclusion from each module (in addition to `:*-table-id` cols)."
  (-> sen2-blade-plans-placements/sen2-blade-module-cols-to-select
      ;; Add `:res` & `:wbp` from `placement-detail` module
      (update :placement-detail #(conj % :res :wbp))))

(def plans-placements-on-census-dates
  "Plans & placements on census dates (with person information)."
  (delay (-> @sen2-blade/ds-map
             (sen2-blade-plans-placements/plans-placements-on-census-dates
              census-dates-ds
              {:sen2-blade-module-cols-to-select sen2-blade-module-cols-to-select})
             (as-> $ (tc/set-dataset-name $ (str (tc/dataset-name $) sen2-blade-suffix))))))

(def plans-placements-on-census-dates-col-name->label
  "Column labels for display."
  (delay (sen2-blade-plans-placements/plans-placements-on-census-dates-col-name->label
          :census-dates sen2/census-dates-col-name->label
          sen2-blade/module-col-name->label)))


(comment ;; Write plans & placements file (and column labels file)
  (do (-> @plans-placements-on-census-dates
          (#(tc/write! % (str out-dir (tc/dataset-name %) ".csv"))))
      (-> @plans-placements-on-census-dates
          (sen2-blade-plans-placements/csv-col-labels-dataset
           @plans-placements-on-census-dates-col-name->label)
          (#(tc/write! % (str out-dir (tc/dataset-name %) ".csv")))))
  )



;;; # Check for issues
;;; ## Checks
(def edubaseall-send-map
  "GIAS SEND map to use (if not using the default)"
  (delay (gias/edubaseall-send->map)))

(def sen2-estab-min-expected-placed
  "Map of issue thresholds for minimum number of placements expected for selected `sen2-estab`s."
  (delay
    (->
     ;; Replace the following template dataset with one containing capacities to consider
     (tc/dataset [{:urn                           "000000"
                   :ukprn                         nil
                   :sen-unit-indicator            false
                   :resourced-provision-indicator false
                   :sen-setting                   nil
                   :capacity                      00}])
     ;; Set threshold to flag if LA usage is below 75% of the capacity
     (tc/map-columns :num-placed-check-threshold [:capacity] #(-> % (* 0.75) Math/floor int))
     ;; Turn into a map
     (->> ((juxt #(-> %
                      (tc/select-columns sen2-blade-plans-placements/sen2-estab-keys)
                      (tc/rows :as-maps))
                 :num-placed-check-threshold))
          (apply zipmap)))))

(def plans-placements-checks-to-omit
  "Checks to omit"
  [:issue-missing-census-year                  ; Guaranteed non-missing by derivation.
   :issue-missing-census-date                  ; Guaranteed non-missing by derivation.
   :issue-unknown-age-at-start-of-school-year  ; Covered by check `:issue-missing-ncy-nominal`
   :issue-not-send-age                         ; Covered by check `:issue-invalid-ncy-nominal`
   :issue-placement-detail-missing-sen2-estab  ; Covered by check `:issue-missing-sen2-estab`
   ])

(def checks
  "Definitions of checks for issues in dataset of plans & placements on census dates."
  (delay (sen2-blade-plans-placements/checks
          {:edubaseall-send-map            @edubaseall-send-map
           :sen2-estab-min-expected-placed @sen2-estab-min-expected-placed
           :checks-to-omit                 plans-placements-checks-to-omit})))


;;; ## Run checks
(def plans-placements-on-census-dates-issues
  "Selected columns of the `plans-placements-on-census-dates` dataset,
   for rows with issues flagged by `checks`,
   with issue flag columns,
   and blank columns for manual updates."
  (delay (-> @plans-placements-on-census-dates
             (sen2-blade-plans-placements/issues->ds
              @checks
              {:sen2-blade-module-cols-to-select sen2-blade-module-cols-to-select})
             (as-> $ (tc/set-dataset-name $ (str (tc/dataset-name $) sen2-blade-suffix))))))

(def plans-placements-on-census-dates-issues-col-name->label
  "Column labels for display."
  (delay (sen2-blade-plans-placements/plans-placements-on-census-dates-issues-col-name->label
          {:plans-placements-on-census-dates-col-name->label @plans-placements-on-census-dates-col-name->label
           :checks                                           @checks})))

(comment ;; EDA: Summary of issues with plans & placements
  (-> @plans-placements-on-census-dates-issues
      sen2-blade-plans-placements/drop-falsey-issue-columns
      (sen2-blade-plans-placements/summarise-issues (merge @checks sen2-blade-plans-placements/checks-total-issues))
      (vary-meta assoc :print-index-range 1000))

  )

(comment ;; Write issues file (and column labels file)
  (do (-> @plans-placements-on-census-dates-issues
          (#(tc/write! % (str out-dir (tc/dataset-name %) ".csv"))))
      (-> @plans-placements-on-census-dates-issues
          (sen2-blade-plans-placements/csv-col-labels-dataset
           @plans-placements-on-census-dates-issues-col-name->label)
          (#(tc/write! % (str out-dir (tc/dataset-name %) ".csv")))))
  )

