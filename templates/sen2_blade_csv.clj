(ns sen2-blade-csv
  "SEN2 Blade read from COLLECT Blade CSV export."
  (:require [witan.sen2.return.person-level.blade.csv :as sen2-blade-csv]))



;;; # Parameters
;;; ## SEN2 Blade
(def data-dir
  "Directory containing SEN2 Blade export files"
  #_"./data/example-sen2-blade-csv-export-2023/"
  "./data/example-sen2-blade-csv-export-2024/"
  #_"./data/example-sen2-blade-csv-export-2025/")

(def export-date-string
  "Date (string) of COLLECT `Blade-Export`"
  #_"31-03-2023"
  "18-04-2024"
  #_"07-03-2025")



;;; # Read SEN2 Blade from CSV files
(def file-names
  "Map of the SEN2 Blade export CSV file names."
  (sen2-blade-csv/make-file-names export-date-string))

(def file-paths
  "Map of the SEN2 Blade export CSV file paths."
  (sen2-blade-csv/make-file-paths export-date-string
                                  data-dir))

(def ds-map
  "Map of SEN2 Blade datasets."
  (delay (sen2-blade-csv/file-paths->ds-map file-paths)))


;;; ## Bring in defs required for EDA/documentation
;; Not required if not doing a sen2-blade-eda.
(def table-id-ds
  "Dataset of `:*table-id` key relationships."
  (delay (sen2-blade-csv/ds-map->table-id-ds @ds-map)))

(def module-titles
  sen2-blade-csv/module-titles)

(def module-col-name->label
  sen2-blade-csv/module-col-name->label)

(def module-src-col-name->col-name
  sen2-blade-csv/module-src-col-name->col-name)

