(ns sen2-blade-csv-plans-placements
  "Clerk notebook illustrating extraction of plans & placements open on census dates from SEN2 return Blade CSV export."
  {:nextjournal.clerk/toc                  true
   :nextjournal.clerk/visibility           {:code   :hide
                                            :result :show}
   :nextjournal.clerk/page-size            nil
   :nextjournal.clerk/auto-expand-results? true
   :nextjournal.clerk/budget               nil}
  (:require [clojure.java.io :as io]
            [nextjournal.clerk :as clerk]
            [tablecloth.api :as tc]
            [witan.sen2 :as sen2]
            [witan.sen2.return.person-level.blade-export.csv :as sen2-blade-csv]
            [witan.sen2.return.person-level.blade-export.csv.plans-placements :as sen2-blade-csv-plans-placements]))

^::clerk/no-cache
(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# Plans & placements open on census dates"
               (format "  \n`%s`  \n" *ns*)
               ((comp :doc meta) *ns*)))
{::clerk/visibility {:code :show}}




;;; # Plans & Placements
;; Illustrates use of:
;; - `witan.sen2.return.person-level.blade-export.csv`  
;;   (aliased as `sen2-blade-csv` here)
;; - `witan.sen2.return.person-level.blade-export.csv.plans-placements`  
;;   (aliased as `sen2-blade-csv-plans-placements` here)



;;; ## Parameters
;;; ### SEN2 Blade CSV Export
;; Specify the folder containing the SEN2 Blade CSV files:
^{::clerk/visibility {:result :hide}
  ::clerk/viewer clerk/md}
(def sen2-blade-csv-dir
  "Directory containing SEN2 blade export CSV files"
  "./data/example-sen2-blade-csv-export/")

;; Make a map of the SEN2 Blade CSV file names:
^{::clerk/visibility {:result :hide}}
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


;;; ### Census dates
;; Make a dataset with column `:census-date` of dates to extract open plans & placements on:
^{::clerk/visibility {:result :show}
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



;;; ## Extract plans & placements on census dates
^{::clerk/visibility {:code :hide}
  ::clerk/viewer clerk/md ::clerk/no-cache true}
((comp :doc meta) #'sen2-blade-csv-plans-placements/plans-placements-on-census-dates)

;; Extract plans & placements on census dates (with person information)
;; from `sen2-blade-ds-map` for `:census-dates` in `census-dates-ds` using:
^{::clerk/visibility {:code   :show
                      :result :hide}}
(def plans-placements-on-census-dates
  (sen2-blade-csv-plans-placements/plans-placements-on-census-dates sen2-blade-csv-ds-map
                                                                    census-dates-ds))

;; This returns a `plans-placements-on-census-dates` dataset with the following structure:
^{::clerk/visibility {:code :hide
                      :result :hide}}
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

^{::clerk/visibility {:code :hide}
  ::clerk/viewer (partial clerk/table {::clerk/width :full})}
(column-info-with-labels plans-placements-on-census-dates
                         sen2-blade-csv-plans-placements/plans-placements-on-census-dates-col-name->label)



;;; ## Check for issues
;;; ### Issues dataset
;; Apply `checks` to `plans-placements-on-census-dates` to obtain
;; dataset of issues for manual review and updates:
^{::clerk/visibility {:code   :show
                      :result :hide}}
(def plans-placements-on-census-dates-issues
  (sen2-blade-csv-plans-placements/issues->ds plans-placements-on-census-dates
                                              sen2-blade-csv-plans-placements/checks))

;; This returns a `plans-placements-on-census-dates-issues` dataset
;; containing key columns of the `plans-placements-on-census-dates` dataset,
;; for rows with issues flagged by `checks`,
;; with issue flag columns,
;; and blank columns for manual updates,
;; with the following structure:
^{::clerk/visibility {:code :hide}
  ::clerk/viewer (partial clerk/table {::clerk/width :full})}
(column-info-with-labels plans-placements-on-census-dates-issues
                         (sen2-blade-csv-plans-placements/plans-placements-on-census-dates-issues-col-name->label
                          sen2-blade-csv-plans-placements/checks))

;;; ### Issues summary
;; Summary of issues (& numbers of CYP & records) by `:census-date`:
^{::clerk/viewer (partial clerk/table {::clerk/width :full})}
(sen2-blade-csv-plans-placements/summarise-issues plans-placements-on-census-dates-issues
                                                  sen2-blade-csv-plans-placements/checks)
