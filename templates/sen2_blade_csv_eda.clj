(ns sen2-blade-csv-eda
  "Template notebook to read and document a SEN2 return COLLECT Blade CSV Export."
  {:nextjournal.clerk/toc                  true
   :nextjournal.clerk/visibility           {:code   :hide
                                            :result :hide}
   :nextjournal.clerk/page-size            nil
   :nextjournal.clerk/auto-expand-results? true
   :nextjournal.clerk/budget               nil}
  (:require [clojure.string :as string]
            [clojure.java.io :as io]
            [nextjournal.clerk :as clerk]
            [tablecloth.api :as tc]
            [witan.sen2.return.person-level.blade-export.csv :as sen2-blade-csv]
            [witan.sen2.return.person-level.blade-export.csv.eda :as sen2-blade-csv-eda]))

^{;; Notebook header
  ::clerk/no-cache   true
  ::clerk/visibility {:result :show}}
(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# SEN2 Person Level Return COLLECT Blade CSV Export"
               (format "  \n`%s`  \n" *ns*)
               ((comp :doc meta) *ns*)
               "  \nTimeStamp: " (.format (java.time.LocalDateTime/now)
                                          (java.time.format.DateTimeFormatter/ISO_LOCAL_DATE_TIME))))



;;; # SEN2 Blade CSV Export EDA
;;; ## Parameters
;;; ### Output directory
(def out-dir
  "Output directory"
  "./tmp/")
^{::clerk/visibility {:result :show}, ::clerk/viewer clerk/md}
(format "%s: `%s`." ((comp :doc meta) #'out-dir) out-dir)


;;; ### SEN2 Blade CSV Export
(def sen2-blade-csv-dir
  "Directory containing SEN2 blade export CSV files"
  "./data/example-sen2-blade-csv-export/")
^{::clerk/visibility {:result :show}, ::clerk/viewer clerk/md}
(format "%s:  \n`%s`." ((comp :doc meta) #'sen2-blade-csv-dir) sen2-blade-csv-dir)

(def sen2-blade-export-date-string
  "Date (string) of COLLECT `Blade-Export`"
  "31-03-2023")
^{::clerk/visibility {:result :show}, ::clerk/viewer clerk/md}
(format "%s: `%s`." ((comp :doc meta) #'sen2-blade-export-date-string) sen2-blade-export-date-string)

;; NOTE: The `person` module should be de-identified as follows:
;; - [x] Contents of the `surname` field deleted.
;; - [x] Contents of the `forename` field deleted.
;; - [x] Contents of `personbirthdate`
;;       (which were of the form "YYYY-Mmm-DD 00:00:00")
;;       edited to first day of the month
;;       (so of the form "YYYY-Mmm-01 00:00:00").
;; - [x] Contents of the `postcode` field deleted.



;;; ## Read CSV files
(def sen2-blade-csv-file-names
  "Map of the SEN2 Blade CSV file names"
  (sen2-blade-csv/file-names sen2-blade-export-date-string))

^{::clerk/visibility {:result :show}
  ::clerk/viewer     (partial clerk/table {::clerk/width :prose})}
(into [["Key" "File Name" "Exists?"]]
      (map (fn [[k v]]
             (let [path (str sen2-blade-csv-dir v)]
               [k v (if (.exists (io/file path)) "✅" "❌")])))
      sen2-blade-csv-file-names)

(def sen2-blade-csv-ds-map
  "Map of SEN2 Blade CSV Export datasets."
  (sen2-blade-csv/->ds-map sen2-blade-csv-dir
                           sen2-blade-csv-file-names))



;;; ## Dataset structure & categorical values
^{::clerk/visibility {:result :show}}
(sen2-blade-csv-eda/report-csv-ds-map-info-all sen2-blade-csv-ds-map)



;;; ## Database structure
^{::clerk/visibility {:result :show}}
(sen2-blade-csv-eda/report-expected-schema)
^{::clerk/visibility {:result :show}}
(sen2-blade-csv-eda/report-table-keys)



;;; ## `*-table-id` Key relationships
;; The hierarchy is proper (due to COLLECT):
;; - primary keys are unique
;; - all foreign keys in child are contained in parent
;; - not all parent records have children
;; - some parents have multiple children
^{::clerk/visibility {:result :show}}
(sen2-blade-csv-eda/report-key-relationships sen2-blade-csv-ds-map)



;;; ## `table-id-ds`
(def sen2-blade-csv-table-id-ds
  "Dataset of `:*table-id` key relationships."
  (sen2-blade-csv/ds-map->table-id-ds sen2-blade-csv-ds-map))

^{::clerk/visibility {:result :show}}
(sen2-blade-csv-eda/report-table-id-ds sen2-blade-csv-table-id-ds)



;;; ## Composite keys
;; Note: OK if not a unique key without `requests-table-id`,
^{::clerk/visibility {:result :show}}
(sen2-blade-csv-eda/report-composite-keys sen2-blade-csv-ds-map)




^{::clerk/visibility {:code :hide, :result :hide}}
(comment ;; clerk build
  (let [in-path            (str "notebooks/" (clojure.string/replace (str *ns*) #"\.|-" {"." "/" "-" "_"}) ".clj")
        out-path           (str out-dir (clojure.string/replace (str *ns*) #"^.*\." "") ".html")]
    (clerk/build! {:paths    [in-path]
                   :ssr      true
                   :bundle   true
                   :out-path "."})
    (.renameTo (io/file "./index.html") (io/file out-path)))
  )
