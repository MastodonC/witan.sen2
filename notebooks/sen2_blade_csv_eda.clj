(ns sen2-blade-csv-eda
  "Clerk notebook to read and document SEN2 return Blade CSV Export."
  {:nextjournal.clerk/toc                  true
   :nextjournal.clerk/visibility           {:code   :fold
                                            :result :show}
   :nextjournal.clerk/page-size            nil
   :nextjournal.clerk/auto-expand-results? true
   :nextjournal.clerk/budget               nil}
  (:require [clojure.java.io :as io]
            [nextjournal.clerk :as clerk]
            [tablecloth.api :as tc]
            [witan.sen2.return.person-level.blade-export.csv :as sen2-blade-csv]
            [witan.sen2.return.person-level.blade-export.csv.eda :as sen2-blade-csv-eda]))

^::clerk/no-cache
(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# SEN2 Person Level Return Blade CSV Export"
               (format "  \n`%s`  \n" *ns*)
               ((comp :doc meta) *ns*)))




;;; # Raw data EDA
;; Illustrates use of:
;; - `witan.sen2.return.person-level.blade-export.csv`  
;;   (aliased as `sen2-blade-csv` here)
;; - `witan.sen2.return.person-level.blade-export.csv.eda`  
;;   (aliased as `sen2-blade-csv-eda` here)



;;; ## Parameters
;;; ### SEN2 Blade CSV Export
;; Specify the folder containing the SEN2 Blade CSV files:
^{::clerk/visibility {:code   :show
                      :result :hide}
  ::clerk/viewer clerk/md}
(def sen2-blade-csv-dir
  "Directory containing SEN2 blade export CSV files"
  "./data/example-sen2-blade-csv-export/")

;; Make a map of the SEN2 Blade CSV file names:
^{::clerk/visibility {:code   :show
                      :result :hide}}
(def sen2-blade-csv-file-names
  (sen2-blade-csv/file-names "31-03-2023"))

(clerk/table {::clerk/width :prose}
             (into [["Key" "File Name" "Exists?"]]
                   (map (fn [[k v]]
                          (let [path (str sen2-blade-csv-dir v)]
                            [k v (if (.exists (io/file path)) "✅" "❌")])))
                   sen2-blade-csv-file-names))



;;; ## Read CSV files
;; Read the CSV files into a map with datasets as values:
^{::clerk/visibility {:code   :show
                      :result :hide}}
(def sen2-blade-csv-ds-map
  "Map of SEN2 Blade CSV Export datasets."
  (sen2-blade-csv/->ds-map sen2-blade-csv-dir
                           sen2-blade-csv-file-names))



;;; ## Dataset structure & categorical values
(sen2-blade-csv-eda/report-csv-ds-map-info-all sen2-blade-csv-ds-map)



;;; ## Database structure
(sen2-blade-csv-eda/report-expected-schema)
(sen2-blade-csv-eda/report-table-keys)



;;; ## `*-table-id` Key relationships
;; The hierarchy is proper (due to COLLECT):
;; - primary keys are unique
;; - all foreign keys in child are contained in parent
;; - not all parent records have children
;; - some parents have multiple children
(sen2-blade-csv-eda/report-key-relationships sen2-blade-csv-ds-map)



;;; ## `table-id-ds`
^{::clerk/visibility {:result :hide}}
(def sen2-blade-csv-table-id-ds
  "Dataset of `:*table-id` key relationships."
  (sen2-blade-csv/ds-map->table-id-ds sen2-blade-csv-ds-map))

(sen2-blade-csv-eda/report-table-id-ds sen2-blade-csv-table-id-ds)



;;; ## Composite keys
;; Note: OK if not a unique key without `requests-table-id`,
(sen2-blade-csv-eda/report-composite-keys sen2-blade-csv-ds-map)

