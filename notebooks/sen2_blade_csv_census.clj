(ns sen2-blade-csv-census
  "Clerk notebook to extract raw census of plans & placements open on census dates from SEN2 return Blade CSV Export."
  {:nextjournal.clerk/toc                  true
   :nextjournal.clerk/visibility           {:code   :fold
                                            :result :show}
   :nextjournal.clerk/page-size            nil
   :nextjournal.clerk/auto-expand-results? true
   :nextjournal.clerk/budget               nil}
  (:require [clojure.java.io :as io]
            [nextjournal.clerk :as clerk]
            [tablecloth.api :as tc]
            [witan.sen2 :as sen2]
            [witan.sen2.return.person-level.blade-export.csv :as sen2-blade-csv]
            [witan.sen2.return.person-level.blade-export.csv.census :as sen2-blade-csv-census]))

(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# Raw Census from SEN2 Person Level Return Blade CSV Export"
               (format "  \n`%s`  \n" *ns*)
               ((comp :doc meta) *ns*)))




;;; # SEN2 raw census
;; Illustrates use of:
;; - `witan.sen2.return.person-level.blade-export.csv`  
;;   (aliased as `sen2-blade-csv` here)
;; - `witan.sen2.return.person-level.blade-export.csv.census`  
;;   (aliased as `sen2-blade-csv-census` here)



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
             (into [["Key" "Value" "File exists?"]]
                   (map (fn [[k v]]
                          (let [path (str sen2-blade-csv-dir v)]
                            [k v (if (.exists (io/file path)) "✅" "❌")])))
                   sen2-blade-csv-file-names))

;; NOTE: The `person` module should be de-identified as follows:
;; - [x] Contents of the `surname` field deleted.
;; - [x] Contents of the `forename` field deleted.
;; - [x] Contents of `personbirthdate` (which were of the form "YYYY-Mmm-DD 00:00:00")
;;       edited to first day of the month (so of the form "YYYY-Mmm-01 00:00:00").
;; - [x] Contents of the `postcode` field deleted.




;;; ### Census dates
;; Make a dataset with column `:census-date` of dates to create the census on:
^{::clerk/visibility {:code   :show
                      :result :show}
  ::clerk/viewer clerk/table}
(def census-dates-ds
  (sen2/census-years->census-dates-ds [2022 2023]))

;; Note:
;; - The `census-dates-ds` can contain other columns which are
;;   carried through to the raw census dataset. (Here `:census-year`.)
;; - The `:census-dates` must be unique.
;; - Multiple `:census-dates` can be specified (even within the same SEN2 `:census-year`).




;;; ## Read CSV files
;; Read the CSV files into a map with datasets as values:
^{::clerk/visibility {:code   :show
                      :result :hide}}
(def sen2-blade-csv-ds-map
  "Map of SEN2 Blade CSV Export datasets."
  (sen2-blade-csv/->ds-map sen2-blade-csv-dir
                           sen2-blade-csv-file-names))




;;; ## Raw sen2 census
^{::clerk/viewer clerk/md ::clerk/no-cache true}
((comp :doc meta) #'sen2-blade-csv-census/sen2-census-raw)

;; Extract raw sen2 census from `sen2-blade-ds-map` for `:census-dates` in `census-dates-ds` using:
^{::clerk/visibility {:result :hide
                      :code   :show}}
(def sen2-census-raw
  (sen2-blade-csv-census/sen2-census-raw sen2-blade-csv-ds-map
                                         census-dates-ds))

;; This returns a `sen2-census-raw` dataset with the following structure:
^{::clerk/visibility {:result :hide}}
(defn- column-info-with-labels
  "Selected column info with labels."
  [ds ds-col-name->label]
  (-> ds
      tc/info
      (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max])
      (tc/map-columns :col-label [:col-name] ds-col-name->label)
      (tc/reorder-columns [:col-name :col-label])
      (tc/rename-columns {:col-name  "Column Name"
                          :col-label "Column Label"
                          :datatype  "Data Type"
                          :n-valid   "# Valid"
                          :n-missing "# Missing"
                          :min       "Min"
                          :max       "Max"})))

^{::clerk/viewer (partial clerk/table {::clerk/width :full})}
(column-info-with-labels sen2-census-raw
                         (sen2-blade-csv-census/sen2-census-raw-col-name->label sen2-census-raw))

