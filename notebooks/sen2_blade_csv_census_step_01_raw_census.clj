(ns sen2-blade-csv-census-step-01-raw-census
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
            [witan.sen2.return.person-level.blade-export.csv-census :as sen2-blade-csv-census]

            [tick.core :as t]
            ))

(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# Raw Census from SEN2 Person Level Return Blade CSV Export"
               (format "  \n`%s`  \n" *ns*)
               ((comp :doc meta) *ns*)))




;;; # SEN2 raw census
;; Illustrates use of:
;; - `witan.sen2.return.person-level.blade-export.csv`
;; - `witan.sen2.return.person-level.blade-export.csv-census`



;;; ## Parameters
;;; ### SEN2 Blade CSV Export
;; SEN2 Blade CSV folder:
^{::clerk/viewer clerk/md}
(def sen2-blade-csv-dir
  "Directory containing SEN2 blade export CSV files"
  "./data/example-sen2-blade-csv-export/")

;; SEN2 Blade CSV files:
^{::clerk/visibility {:result :hide}}
(def sen2-blade-csv-file-names
  (sen2-blade-csv/file-names "31-03-2023"))

(clerk/table {::clerk/width :prose}
             (into [["Key" "File Name" "Exists?"]]
                   (map (fn [[k v]]
                          (let [path (str sen2-blade-csv-dir v)]
                            [k v (if (.exists (io/file path)) "✅" "❌")])))
                   sen2-blade-csv-file-names))

;; NOTE: The `person` module has been de-identified:
;; - [x] Contents of the `surname` field deleted.
;; - [x] Contents of the `forename` field deleted.
;; - [x] Contents of `personbirthdate` (which were of the form "YYYY-Mmm-DD 00:00:00")
;;       edited to first day of the month (so of the form "YYYY-Mmm-01 00:00:00").
;; - [x] Contents of the `postcode` field have been deleted.



;;; Read CSV files
;; into a map with datasets as values:
^{::clerk/visibility {:code   :show
                      :result :hide}}
(def sen2-blade-csv-ds-map
  "Map of SEN2 Blade CSV Export datasets."
  (sen2-blade-csv/->ds-map sen2-blade-csv-dir
                           sen2-blade-csv-file-names))



;;; ### Census dates
^{::clerk/visibility {:result :hide}}
(def census-years [2022 2023])
^{::clerk/viewer clerk/table}
(def census-dates-ds
  (sen2-blade-csv-census/census-years->census-dates-ds census-years))




;;; ## Utility functions
(defn- clerk-table-or-msg [ds s]
  (if (zero? (tc/row-count ds))
    (clerk/md s)
    (clerk/table {::clerk/width :prose} ds)))

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




;;; ## Extract plans & placements open on census-dates
;;; ### Person information
;; …with age at start of school year & nominal NCY for census-dates

^{::clerk/viewer clerk/md}
(format "NOTE: There are **%,d** CYP with missing `:person-birth-date` in table `person`."
        (-> (:person sen2-blade-csv-ds-map)
            (tc/select-missing [:person-birth-date])
            (tc/row-count)))


^{::clerk/visibility {:result :hide}}
(def person-on-census-dates
  (sen2-blade-csv-census/person-on-census-dates sen2-blade-csv-ds-map
                                                #_{:census-years census-years}
                                                {:census-dates-ds census-dates-ds}
                                                ))
^{::clerk/visibility {:result :hide}}
(def person-on-census-dates-col-name->label
  (sen2-blade-csv-census/person-on-census-dates-col-name->label person-on-census-dates))

;; Dataset `person-on-census-dates`:
^{::clerk/viewer (partial clerk/table {::clerk/width :full})}
(column-info-with-labels person-on-census-dates
                         person-on-census-dates-col-name->label)

;; Ages on at start of school year (on 31-AUG) containing the `:census-date` and corresponding nominal NCY for all CYP in `person` table:
^{::clerk/viewer (partial clerk/table {::clerk/width :prose})}
(-> person-on-census-dates
    (tc/crosstab [:age-at-start-of-school-year :nominal-ncy] [:census-year])
    (tc/order-by ["rows/cols"])
    (tc/add-columns {:age-at-start-of-school-year #(map first (% "rows/cols"))
                     :nominal-ncy #(map second (% "rows/cols"))})
    (tc/drop-columns "rows/cols")
    (tc/reorder-columns [:age-at-start-of-school-year :nominal-ncy])
    (tc/rename-columns person-on-census-dates-col-name->label))
