(ns plans-placements
  "Extract & check plans & placements on census dates from SEN2 return Blade CSV Export."
  (:require [tablecloth.api :as tc]
            [witan.sen2 :as sen2]
            [witan.sen2.return.person-level.blade-export.csv :as sen2-blade-csv]
            [witan.sen2.return.person-level.blade-export.csv.plans-placements :as sen2-blade-csv-plans-placements]))


;;; # Parameters
;;; ## Output directory
(def out-dir
  "Output directory"
  "./tmp/")


;;; ## Census dates
(def census-dates-ds
  "Dataset with column `:census-date` of dates to extract open plans & placements on"
  (sen2/census-years->census-dates-ds [2022 2023]))


;;; ## SEN2 Blade CSV Export
(def sen2-blade-csv-dir
  "Directory containing SEN2 blade export CSV files"
  "./data/example-sen2-blade-csv-export/")

(def sen2-blade-export-date-string
  "Date (string) of COLLECT `Blade-Export`"
  "31-03-2023")



;;; # Read CSV files
(def sen2-blade-csv-file-names
  "Map of the SEN2 Blade CSV file names"
  (sen2-blade-csv/file-names sen2-blade-export-date-string))

(def sen2-blade-csv-ds-map
  "Map of SEN2 Blade CSV Export datasets."
  (delay (sen2-blade-csv/->ds-map sen2-blade-csv-dir
                                  sen2-blade-csv-file-names)))



;;; # Extract plans & placements on census dates
(def plans-placements-on-census-dates
  "Plans & placements on census dates (with person information)
   from `sen2-blade-ds-map` for `:census-dates` in `census-dates-ds`."
  (delay (sen2-blade-csv-plans-placements/plans-placements-on-census-dates @sen2-blade-csv-ds-map
                                                                           census-dates-ds)))


;;; ## Write plans & placements file
(comment
  (let [ds              @plans-placements-on-census-dates
        file-name-stem  (tc/dataset-name ds)
        col-name->label sen2-blade-csv-plans-placements/plans-placements-on-census-dates-col-name->label]
    (tc/write! (tc/dataset {:column-number (iterate inc 1)
                            :column-name   (map name   (tc/column-names ds))
                            :column-label  (map col-name->label (tc/column-names ds))})
               (str out-dir file-name-stem "-col-labels.csv"))
    (tc/write! ds
               (str out-dir file-name-stem ".csv")))
  )



;;; # Check for issues
(def plans-placements-on-census-dates-issues
  "Selected columns of the `plans-placements-on-census-dates` dataset,
   for rows with issues flagged by `checks`,
   with issue flag columns,
  and blank columns for manual updates."
  (delay (sen2-blade-csv-plans-placements/issues->ds @plans-placements-on-census-dates
                                                     sen2-blade-csv-plans-placements/checks)))

;;; ### Write issues file
(comment
  (let [ds              @plans-placements-on-census-dates-issues
        file-name-stem  (tc/dataset-name ds)
        col-name->label (sen2-blade-csv-plans-placements/plans-placements-on-census-dates-issues-col-name->label
                         sen2-blade-csv-plans-placements/checks)]
    (tc/write! (tc/dataset {:column-number (iterate inc 1)
                            :column-name   (map name   (tc/column-names ds))
                            :column-label  (map col-name->label (tc/column-names ds))})
               (str out-dir file-name-stem "-col-labels.csv"))
    (tc/write! ds
               (str out-dir file-name-stem ".csv")))
  )

