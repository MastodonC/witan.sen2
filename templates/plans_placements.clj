(ns plans-placements
  "Extract & check plans & placements on census dates from SEN2 Blade."
  (:require [tablecloth.api :as tc]
            [witan.sen2 :as sen2]
            [sen2-blade :as sen2-blade] ; <- replace with workpackage specific version
            [witan.sen2.return.person-level.blade.plans-placements :as sen2-blade-plans-placements]))


;;; # Parameters
;;; ## Output directory
(def out-dir
  "Output directory"
  "./tmp/")


;;; ## Census dates
(def census-dates-ds
  "Dataset with column `:census-date` of dates to extract open plans & placements on."
  (sen2/census-years->census-dates-ds [2022 2023]))



;;; # Extract plans & placements on census dates
(def plans-placements-on-census-dates
  "Plans & placements on census dates (with person information)."
  (delay (sen2-blade-plans-placements/plans-placements-on-census-dates @sen2-blade/ds-map
                                                                       census-dates-ds)))

(def plans-placements-on-census-dates-col-name->label
  "Column labels for display."
  (delay (sen2-blade-plans-placements/plans-placements-on-census-dates-col-name->label
          :census-dates sen2/census-dates-col-name->label
          sen2-blade/module-col-name->label)))


;;; ## Write plans & placements file
(comment
  (let [ds              @plans-placements-on-census-dates
        file-name-stem  (tc/dataset-name ds)
        col-name->label @plans-placements-on-census-dates-col-name->label]
    (tc/write! (tc/dataset {:column-number (iterate inc 1)
                            :column-name   (map name   (tc/column-names ds))
                            :column-label  (map col-name->label (tc/column-names ds))})
               (str out-dir file-name-stem "-col-labels.csv"))
    (tc/write! ds
               (str out-dir file-name-stem ".csv")))
  )



;;; # Check for issues


;;; ## Checks
(def checks
  "Definitions for checks for issues in dataset of plans & placements on census dates."
  (sen2-blade-plans-placements/checks))


;;; ## Run checks
(def plans-placements-on-census-dates-issues
  "Selected columns of the `plans-placements-on-census-dates` dataset,
   for rows with issues flagged by `checks`,
   with issue flag columns,
   and blank columns for manual updates."
  (delay (sen2-blade-plans-placements/issues->ds @plans-placements-on-census-dates
                                                 checks)))

(def plans-placements-on-census-dates-issues-col-name->label
  "Column labels for display."
  (delay (sen2-blade-plans-placements/plans-placements-on-census-dates-issues-col-name->label
          {:plans-placements-on-census-dates-col-name->label @plans-placements-on-census-dates-col-name->label
           :checks                                           checks})))


;;; ## Write issues file
(comment
  (let [ds              @plans-placements-on-census-dates-issues
        file-name-stem  (tc/dataset-name ds)
        col-name->label @plans-placements-on-census-dates-issues-col-name->label]
    (tc/write! (tc/dataset {:column-number (iterate inc 1)
                            :column-name   (map name   (tc/column-names ds))
                            :column-label  (map col-name->label (tc/column-names ds))})
               (str out-dir file-name-stem "-col-labels.csv"))
    (tc/write! ds
               (str out-dir file-name-stem ".csv")))
  )

