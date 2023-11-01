(ns sen2-blade-csv-plans-placements
  "Clerk notebook illustrating extraction of plans & placements open on census dates from SEN2 return Blade CSV export."
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
            [witan.sen2 :as sen2]
            [witan.sen2.return.person-level.blade-export.csv :as sen2-blade-csv]
            [witan.sen2.return.person-level.blade-export.csv.plans-placements :as sen2-blade-csv-plans-placements]))

^::clerk/no-cache
(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# Plans & placements open on census dates"
               (format "  \n`%s`  \n" *ns*)
               ((comp :doc meta) *ns*)
               "  \nTimeStamp: " (.format (java.time.LocalDateTime/now)
                                          (java.time.format.DateTimeFormatter/ISO_LOCAL_DATE_TIME))))
{::clerk/visibility {:code :show}}




;;; # Plans & Placements
;; Illustrates use of:
;; - `witan.sen2.return.person-level.blade-export.csv`  
;;   (aliased as `sen2-blade-csv` here)
;; - `witan.sen2.return.person-level.blade-export.csv.plans-placements`  
;;   (aliased as `sen2-blade-csv-plans-placements` here)
;;
;; to:
;; 1. Read the CSV files from a SEN2 return Blade exported from COLLECT.
;; 2. Extract plans & placements on census dates.
;; 3. Identify issues in the dataset of plans & placements and create an
;;    issues CSV file for review and entry of updates.
;; 4. Apply updates from an updates file to address issues.
;;
;; In practice this would be split into two separate namespaces: The
;; first would end after #3 so the issues file can be reviewed with the
;; client and updates to address the issues agreed. Step #4 would then
;; follow, likely as the first part of a namespace progressing towards a census.


;;; ## Parameters
;;; ### Working directory
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



;;; ## 1. Read CSV files
;; Read the CSV files into a map with datasets as values:
^{::clerk/visibility {:code :show, :result :hide}}
(def sen2-blade-csv-ds-map
  "Map of SEN2 Blade CSV Export datasets."
  (sen2-blade-csv/->ds-map sen2-blade-csv-dir
                           sen2-blade-csv-file-names))



;;; ## 2. Extract plans & placements on census dates
^{::clerk/visibility {:code :hide}
  ::clerk/viewer clerk/md ::clerk/no-cache true}
((comp :doc meta) #'sen2-blade-csv-plans-placements/plans-placements-on-census-dates)

;; Extract plans & placements on census dates (with person information)
;; from `sen2-blade-ds-map` for `:census-dates` in `census-dates-ds` using:
^{::clerk/visibility {:code :show, :result :hide}}
(def plans-placements-on-census-dates
  (sen2-blade-csv-plans-placements/plans-placements-on-census-dates sen2-blade-csv-ds-map
                                                                    census-dates-ds))

;; This returns a `plans-placements-on-census-dates` dataset with the following structure:
^{::clerk/visibility {:code :fold, :result :hide}}
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


;;; ### Write plans & placements file
^{::clerk/visibility {:code :hide}}
(clerk/md (format "Write `%s`  \nto working directory: %s:" (tc/dataset-name plans-placements-on-census-dates) wk-dir))
^{::clerk/visibility {:code :show, :result :hide}}
(comment
  (let [ds              plans-placements-on-census-dates
        file-name-stem  (tc/dataset-name ds)
        col-name->label sen2-blade-csv-plans-placements/plans-placements-on-census-dates-col-name->label]
    (tc/write! (tc/dataset {:column-number (iterate inc 1)
                            :column-name   (map name   (tc/column-names ds))
                            :column-label  (map col-name->label (tc/column-names ds))})
               (str wk-dir file-name-stem "-col-labels.csv"))
    (tc/write! ds
               (str wk-dir file-name-stem ".csv")))
  )



;;; ## 3. Check for issues
;;; ### Issues dataset
;; Apply `checks` to `plans-placements-on-census-dates` to obtain
;; dataset of issues for manual review and entry of updates:
^{::clerk/visibility {:code :show, :result :hide}}
(def plans-placements-on-census-dates-issues
  (sen2-blade-csv-plans-placements/issues->ds plans-placements-on-census-dates
                                              sen2-blade-csv-plans-placements/checks))

;; This returns a `plans-placements-on-census-dates-issues` dataset containing
;; selected columns of the `plans-placements-on-census-dates` dataset,
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


;;; ### Write issues file
(clerk/md (format "Write `%s`  \nto working directory: %s:" (tc/dataset-name plans-placements-on-census-dates-issues) wk-dir))
^{::clerk/visibility {:code :show, :result :hide}}
(comment
  (let [ds              plans-placements-on-census-dates-issues
        file-name-stem  (tc/dataset-name ds)
        col-name->label (sen2-blade-csv-plans-placements/plans-placements-on-census-dates-issues-col-name->label
                         sen2-blade-csv-plans-placements/checks)]
    (tc/write! (tc/dataset {:column-number (iterate inc 1)
                            :column-name   (map name   (tc/column-names ds))
                            :column-label  (map col-name->label (tc/column-names ds))})
               (str wk-dir file-name-stem "-col-labels.csv"))
    (tc/write! ds
               (str wk-dir file-name-stem ".csv")))
  )



;; ## 4. Update
;; Apply updates from `plans-placements-on-census-dates-updates` CSV file
;; (essentially the issues file with the #"`:update-.*`" columns filled in)
;; to address issues in the raw `plans-placements-on-census-dates`.
;;
;; In practice this step would be in a separate namespace, since the
;; issues identified in step #3 above would need to be reviewed, updates
;; agreed, and updates entered in to the #"`:update-.*`" columns of the
;; issues file before this step could be completed.  It is included here
;; to illustrate the complete process of obtaining clean
;; `plans-plaements-on-census-dates`.
;;
;; This step would likely be the first step of a namespace progressing
;; towards a census, which may also include programmatic updates for systematic
;; issues as well as implementing updates entered in the #"`:update-.*`" columns of
;; the issues file using the code below.

;; Note that only the columns required to construct a census are retained:
^{::clerk/visibility {:code :fold, :result :show}}
sen2-blade-csv-plans-placements/cols-for-census


;;; ### Read CSV of plans & placements
;; If doing these updates in a separate namespace,
;; then will have to retrieve the required columns from the
;; `plans-placements-on-census-dates` CSV
;; (with correct parsing and datatypes) using the code in this section.

;; Filepath of CSV file containing `plans-placements-on-census-dates`:
^{::clerk/visibility {:code :show, :result :hide}
  ::clerk/viewer clerk/md}
(def plans-placements-on-census-dates-filepath
  (str wk-dir "plans-placements-on-census-dates.csv"))

;; Get columns from `plans-placements-on-census-dates` required to construct census:
^{::clerk/visibility {:code :show, :result :hide}
  ::clerk/viewer clerk/md}
(def plans-placements-on-census-dates-cols4census
  (sen2-blade-csv-plans-placements/csv-file->ds plans-placements-on-census-dates-filepath))

;; `plans-placements-on-census-dates-cols4census` dataset structure:
^{::clerk/visibility {:code :hide, :result :show}
  ::clerk/viewer (partial clerk/table {::clerk/width :prose})}
(-> plans-placements-on-census-dates-cols4census
    tc/info
    (tc/select-columns [:col-name :datatype :n-valid :n-missing]))


;;; ### Updates file
;; Filepath of CSV file of updates to apply to address issues:
^{::clerk/viewer clerk/md}
(def plans-placements-on-census-dates-updates-filepath
  (str wk-dir "plans-placements-on-census-dates-updates.csv"))

;; Read columns required into `plans-placements-on-census-dates-updates`:
^{::clerk/visibility {:code :show, :result :hide}}
(def plans-placements-on-census-dates-updates
  (sen2-blade-csv-plans-placements/updates-csv-file->ds plans-placements-on-census-dates-updates-filepath))

;; This returns a `plans-placements-on-census-dates-updates` dataset with structure:
^{::clerk/visibility {:code :hide}
  ::clerk/viewer (partial clerk/table {::clerk/width :full})}
(column-info-with-labels plans-placements-on-census-dates-updates
                         (sen2-blade-csv-plans-placements/plans-placements-on-census-dates-issues-col-name->label
                          sen2-blade-csv-plans-placements/checks))


;;; ### Summarise updates
;; Summary of updates: dropping (✓) and updating (Δ) records as follows:
^{::clerk/viewer (partial clerk/table {::clerk/width :full})}
(sen2-blade-csv-plans-placements/summarise-updates plans-placements-on-census-dates-updates)


;;; ### Apply updates
^{::clerk/visibility {:result :hide}}
(def plans-placements-on-census-dates-updated
  (sen2-blade-csv-plans-placements/update-plans-placements-on-census-dates plans-placements-on-census-dates-cols4census
                                                                           plans-placements-on-census-dates-updates))

;; Giving updated `plans-placements-on-census-dates-updated` dataset
;; (retaining only the columns required to construct a census) with structure:
^{::clerk/visibility {:code :hide}
  ::clerk/viewer (partial clerk/table {::clerk/width :full})}
(column-info-with-labels plans-placements-on-census-dates-updated
                         sen2-blade-csv-plans-placements/plans-placements-on-census-dates-col-name->label)


;;; ### Check updated dataset
;; TODO: Check no issues with updated dataset.


;;; ### Write updated file
^{::clerk/visibility {:code :hide}}
(clerk/md (format "Write `%s`  \nto working directory: %s:" (tc/dataset-name plans-placements-on-census-dates-updated) wk-dir))
^{::clerk/visibility {:code :show, :result :hide}}
(comment
  (let [ds              plans-placements-on-census-dates-updated
        file-name-stem  (tc/dataset-name ds)
        col-name->label sen2-blade-csv-plans-placements/plans-placements-on-census-dates-col-name->label]
    (tc/write! (tc/dataset {:column-number (iterate inc 1)
                            :column-name   (map name   (tc/column-names ds))
                            :column-label  (map col-name->label (tc/column-names ds))})
               (str wk-dir file-name-stem "-col-labels.csv"))
    (tc/write! ds
               (str wk-dir file-name-stem ".csv")))
  )





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
