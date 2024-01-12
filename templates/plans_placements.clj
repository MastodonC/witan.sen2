(ns plans-placements
  "Extract & check plans & placements on census dates from SEN2 Blade."
  (:require [tablecloth.api :as tc]
            [witan.sen2 :as sen2]
            [witan.sen2.return.person-level.blade-export.csv :as sen2-blade-csv]
            [witan.sen2.return.person-level.blade-export.plans-placements :as sen2-blade-plans-placements]))


;;; # Parameters
;;; ## Output directory
(def out-dir
  "Output directory"
  "./tmp/")


;;; ## Census dates
(def census-dates-ds
  "Dataset with column `:census-date` of dates to extract open plans & placements on"
  (sen2/census-years->census-dates-ds [2022 2023]))


;;; ## SEN2 Blade
(def sen2-blade-export-dir
  "Directory containing SEN2 Blade export files"
  "./data/example-sen2-blade-csv-export/")

(def sen2-blade-export-date-string
  "Date (string) of COLLECT `Blade-Export`"
  "31-03-2023")



;;; # Read CSV files
(def sen2-blade-csv-file-paths
  "Map of the SEN2 Blade export CSV file paths."
  (sen2-blade-csv/file-paths sen2-blade-export-dir
                             sen2-blade-export-date-string))

(def sen2-blade-csv-ds-map
  "Map of SEN2 Blade export CSV datasets."
  (delay (sen2-blade-csv/file-paths->ds-map sen2-blade-csv-file-paths)))



;;; # Extract plans & placements on census dates
(def plans-placements-on-census-dates
  "Plans & placements on census dates (with person information)."
  (delay (sen2-blade-plans-placements/plans-placements-on-census-dates @sen2-blade-csv-ds-map
                                                                       census-dates-ds)))

(def plans-placements-on-census-dates-col-name->label
  "Column labels for display."
  sen2-blade-plans-placements/plans-placements-on-census-dates-col-name->label)

;;; ## Write plans & placements file
(comment
  (let [ds              @plans-placements-on-census-dates
        file-name-stem  (tc/dataset-name ds)
        col-name->label plans-placements-on-census-dates-col-name->label]
    (tc/write! (tc/dataset {:column-number (iterate inc 1)
                            :column-name   (map name   (tc/column-names ds))
                            :column-label  (map col-name->label (tc/column-names ds))})
               (str out-dir file-name-stem "-col-labels.csv"))
    (tc/write! ds
               (str out-dir file-name-stem ".csv")))
  )



;;; # Check for issues
(def checks
  "Definitions for checks for issues in dataset of plans & placements on census dates."
  sen2-blade-plans-placements/checks)

(def plans-placements-on-census-dates-issues
  "Selected columns of the `plans-placements-on-census-dates` dataset,
   for rows with issues flagged by `checks`,
   with issue flag columns,
   and blank columns for manual updates."
  (delay (sen2-blade-plans-placements/issues->ds @plans-placements-on-census-dates
                                                 checks)))

(def plans-placements-on-census-dates-issues-col-name->label
  "Column labels for display."
  (sen2-blade-plans-placements/plans-placements-on-census-dates-issues-col-name->label
   checks))


;;; ### Write issues file
(comment
  (let [ds              @plans-placements-on-census-dates-issues
        file-name-stem  (tc/dataset-name ds)
        col-name->label plans-placements-on-census-dates-issues-col-name->label]
    (tc/write! (tc/dataset {:column-number (iterate inc 1)
                            :column-name   (map name   (tc/column-names ds))
                            :column-label  (map col-name->label (tc/column-names ds))})
               (str out-dir file-name-stem "-col-labels.csv"))
    (tc/write! ds
               (str out-dir file-name-stem ".csv")))
  )

