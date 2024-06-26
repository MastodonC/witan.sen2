(ns plans-placements-eda
  "Report on plans & placements on census dates extracted from SEN2 Blade."
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
	    [witan.send.adroddiad.tablecloth-utils :as tc-utils]
            [witan.sen2.return.person-level.blade.plans-placements :as sen2-blade-plans-placements]
            [sen2-blade :as sen2-blade] ; <- replace with workpackage specific version
            [plans-placements :as plans-placements] ; <- replace with workpackage specific version
            [witan.sen2.ehcp-stats :as ehcp-stats]))


^{;; Notebook header
  ::clerk/no-cache true}
(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# witan.sen2"
               (format "  \n`%s`  \n" *ns*)
               ((comp :doc meta) *ns*)
               "  \nTimeStamp: " (.format (java.time.LocalDateTime/now)
                                          (java.time.format.DateTimeFormatter/ISO_LOCAL_DATE_TIME))))




;;; # Plans & Placements EDA
;; 1. Get the SEN2 Blade.
;; 2. Extract plans & placements on census dates.
;; 3. Identify issues in the dataset of plans & placements and create an
;;    issues CSV file for review and entry of updates.
;; 4. Compare Totals with DfE Caseload



;;; ## Parameters
;;; ### Output directory
^{::clerk/viewer clerk/md}
(def out-dir "./tmp/")

;;; ### LA
^{::clerk/viewer clerk/md}
(def la-name "Plymouth")



;;; ## 1. SEN2 Blade
^{::clerk/viewer clerk/md}
(format "Read from: `%s`:" sen2-blade/data-dir)
^{::clerk/viewer (partial clerk/table {::clerk/width :prose})}
(-> sen2-blade/file-names
    ((fn [m] (tc/dataset {"Module key" (keys m)
                          "File Name"  (vals m)}))))



;;; ## 2. Plans & placements on census dates
^{::clerk/viewer   clerk/md
  ::clerk/no-cache true}
((comp :doc meta) #'sen2-blade-plans-placements/plans-placements-on-census-dates)


;;; ### Census dates
^{::clerk/viewer (partial clerk/table {::clerk/width :prose})}
plans-placements/census-dates-ds


;;; ### `plans-placements-on-census-dates` dataset 
;; `plans-placements-on-census-dates` dataset structure:
^{::clerk/viewer (partial clerk/table {::clerk/width :full})}
(tc-utils/column-info-with-labels @plans-placements/plans-placements-on-census-dates
                                  @plans-placements/plans-placements-on-census-dates-col-name->label)

^{::clerk/viewer clerk/md}
(format "Wrote `%s`  \nto working directory: `%s`:"
        (tc/dataset-name @plans-placements/plans-placements-on-census-dates)
        plans-placements/out-dir)



;;; ## 3. Issues
;;; ### Issues dataset
;; `plans-placements-on-census-dates-issues` dataset structure:
^{::clerk/viewer (partial clerk/table {::clerk/width :full})}
(tc-utils/column-info-with-labels @plans-placements/plans-placements-on-census-dates-issues
                                  @plans-placements/plans-placements-on-census-dates-issues-col-name->label)

^{::clerk/viewer clerk/md}
(format "Wrote `%s`  \nto working directory: `%s`:"
        (tc/dataset-name @plans-placements/plans-placements-on-census-dates-issues)
        plans-placements/out-dir)


;;; ### Issues summary
;; Summary of issues (& numbers of CYP & records) by `:census-date`:
^{::clerk/viewer (partial clerk/table {::clerk/width :full})}
(sen2-blade-plans-placements/summarise-issues @plans-placements/plans-placements-on-census-dates-issues
                                              plans-placements/checks)


;;; ## 4. Compare Totals with DfE Caseload
;;; ### #CYP with plan and/or placement record on census-date
;; …and discrepancy in # with both a plan & a placement record open on the census-date
;; with the EHCP caseload published by the DfE on [explore-education-statistics.service.gov.uk](https://explore-education-statistics.service.gov.uk/find-statistics/education-health-and-care-plans):
^{::clerk/viewer (partial clerk/table {::clerk/width :full})}
(-> plans-placements/census-dates-ds
    (tc/map-columns "Census Date" [:census-date]
                    #(.format % (java.time.format.DateTimeFormatter/ofPattern "dd-MMM-uuuu"
                                                                              (java.util.Locale. "en_GB"))))
    (tc/drop-columns [:census-date])
    (tc/left-join (-> @plans-placements/plans-placements-on-census-dates
                      (tc/select-rows :named-plan?)
                      (tc/group-by [:census-year])
                      (tc/aggregate {"#plans" tc/row-count}))
                  [:census-year])
    (tc/drop-columns #"^:right\..+$")
    (tc/left-join (-> @plans-placements/plans-placements-on-census-dates
                      (tc/select-rows :placement-detail?)
                      (tc/group-by [:census-year])
                      (tc/aggregate {"#placements" tc/row-count}))
                  [:census-year])
    (tc/drop-columns #"^:right\..+$")
    (tc/left-join (-> @plans-placements/plans-placements-on-census-dates
                      (tc/group-by [:census-year])
                      (tc/aggregate {"#plan|placement" tc/row-count}))
                  [:census-year])
    (tc/drop-columns #"^:right\..+$")
    (tc/left-join (-> @plans-placements/plans-placements-on-census-dates
                      (tc/select-rows :named-plan?)
                      (tc/select-rows :placement-detail?)
                      (tc/group-by [:census-year])
                      (tc/aggregate {"#plan&placement" tc/row-count}))
                  [:census-year])
    (tc/drop-columns #"^:right\..+$")
    (tc/left-join (-> @ehcp-stats/caseload
                      (tc/select-rows (comp #{la-name} :la-name))
                      (tc/select-rows (comp (apply hash-set (:census-year plans-placements/census-dates-ds)) :time-period))
                      (tc/select-columns [:time-period :num-caseload])
                      (tc/rename-columns {:time-period  :census-year
                                          :num-caseload "EHCP Caseload"})
                      (tc/set-dataset-name "right"))
                  [:census-year])
    (tc/drop-columns #"^:right\..+$")
    (tc/map-columns "Discrepancy" ["#plan&placement" "EHCP Caseload"] -)
    (tc/order-by [:census-year])
    (tc/rename-columns {:census-year "Census year"}))



^{::clerk/visibility {:result :hide}}
(comment ;; clerk build
  (let [in-path            (str "templates/" (clojure.string/replace (str *ns*) #"\.|-" {"." "/" "-" "_"}) ".clj")
        out-path           (str out-dir (clojure.string/replace (str *ns*) #"^.*\." "") ".html")]
    (clerk/build! {:paths    [in-path]
                   :ssr      true
                   :bundle   true
                   :out-path "."})
    (.renameTo (io/file "./index.html") (io/file out-path)))
  )
