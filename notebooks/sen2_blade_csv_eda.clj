(ns sen2-blade-csv-eda
  "Clerk notebook to read and document SEN2 return Blade CSV Export."
  {:nextjournal.clerk/toc                  true
   :nextjournal.clerk/visibility           {:code   :hide
                                            :result :show}
   :nextjournal.clerk/page-size            nil
   :nextjournal.clerk/auto-expand-results? true
   :nextjournal.clerk/budget               nil}
  (:require [clojure.string :as string]
            [clojure.java.io :as io]
            [nextjournal.clerk :as clerk]
            [tablecloth.api :as tc]
            [witan.sen2.return.person-level.blade-export.csv :as sen2-blade-csv]
            [witan.sen2.return.person-level.blade-export.csv.eda :as sen2-blade-csv-eda]))

^::clerk/no-cache
(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# SEN2 Person Level Return Blade CSV Export"
               (format "  \n`%s`  \n" *ns*)
               ((comp :doc meta) *ns*)
               "  \nTimeStamp: " (.format (java.time.LocalDateTime/now)
                                          (java.time.format.DateTimeFormatter/ISO_LOCAL_DATE_TIME))))
{::clerk/visibility {:code :fold}}




;;; # Raw data EDA
;; This notebook illustrates use of:
;; - `witan.sen2.return.person-level.blade-export.csv`  
;;   (aliased as `sen2-blade-csv` here)
;; - `witan.sen2.return.person-level.blade-export.csv.eda`  
;;   (aliased as `sen2-blade-csv-eda` here)
;; to read and document the SEN2 return Blade export CSV files.
;;
;; Notebook `sen2-blade-csv-plans-placements` describes all the steps
;; necessary to extract plans & placements on census dates (from which
;; one can derive a census for modelling).
;;
;; This notebook does not write any files, and is not a prerequisite for
;; `sen2-blade-csv-plans-placements`.  It is provided to assist in
;; understanding the SEN2 return Blade CSV export.



;;; ## Parameters
;;; ### Working directory
;;; (Only used in last comment as destination for built HTML notebook.)
^{::clerk/visibility {:code :show, :result :hide}
  ::clerk/viewer clerk/md}
(def wk-dir "./tmp/")


;;; ### SEN2 Blade CSV Export
;; Specify the folder containing the SEN2 Blade CSV files:
^{::clerk/visibility {:code :show, :result :hide}
  ::clerk/viewer clerk/md}
(def sen2-blade-csv-dir
  "Directory containing SEN2 blade export CSV files"
  "./data/example-sen2-blade-csv-export/")

;; Make a map of the SEN2 Blade CSV file names:
^{::clerk/visibility {:code :show, :result :hide}}
(def sen2-blade-csv-file-names
  (sen2-blade-csv/file-names "31-03-2023"))

;; Check the files exist:
^{::clerk/visibility {:code :fold}
  ::clerk/viewer (partial clerk/table {::clerk/width :prose})}
(into [["Key" "File Name" "Exists?"]]
      (map (fn [[k v]]
             (let [path (str sen2-blade-csv-dir v)]
               [k v (if (.exists (io/file path)) "✅" "❌")])))
      sen2-blade-csv-file-names)

;; NOTE: The `person` module should be de-identified as follows:
;; - [x] Contents of the `surname` field deleted.
;; - [x] Contents of the `forename` field deleted.
;; - [x] Contents of `personbirthdate`
;;       (which were of the form "YYYY-Mmm-DD 00:00:00")
;;       edited to first day of the month
;;       (so of the form "YYYY-Mmm-01 00:00:00").
;; - [x] Contents of the `postcode` field deleted.



;;; ## Read CSV files
;; Read the CSV files into a map with datasets as values:
^{::clerk/visibility {:code :show, :result :hide}}
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




^{::clerk/visibility {:code :hide, :result :hide}}
(comment ;; clerk build
  (let [in-path            (str "notebooks/" (clojure.string/replace (str *ns*) #"\.|-" {"." "/" "-" "_"}) ".clj")
        out-path           (str wk-dir (clojure.string/replace (str *ns*) #"^.*\." "") ".html")]
    (clerk/build! {:paths    [in-path]
                   :ssr      true
                   :bundle   true
                   :out-path "."})
    (.renameTo (io/file "./index.html") (io/file out-path)))
  )
