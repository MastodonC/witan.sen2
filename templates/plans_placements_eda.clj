(ns plans-placements-eda
  "Report on plans & placements on census dates extracted from SEN2 Blade."
  #:nextjournal.clerk{:toc                  true
                      :visibility           {:code   :hide
                                             :result :hide}
                      :page-size            nil
                      :auto-expand-results? true
                      :budget               nil}
  (:require [nextjournal.clerk :as clerk]
            [tablecloth.api :as tc]
            [witan.send.adroddiad.tablecloth-utils :as tc-utils]
            [witan.sen2.return.person-level.dictionary :as sen2-dictionary]
            [witan.sen2.return.person-level.blade.plans-placements :as sen2-blade-plans-placements]
            [sen2-blade-csv :as sen2-blade] ; <- replace with workpackage specific version
            [plans-placements :as plans-placements] ; <- replace with workpackage specific version
            )
  (:import [java.time LocalDateTime]
           [java.time.format DateTimeFormatter]))


(def client-name      "Mastodon C")
(def workpackage-name "witan.sen2")
(def out-dir "Output directory" "./tmp/")

^#::clerk{:visibility {:result :show},:viewer clerk/md, :no-cache true} ; Notebook header
(str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
     (format "# %s SEND %s  \n" client-name workpackage-name)
     (format "`%s`\n\n" *ns*)
     (format "%s\n\n" ((comp :doc meta) *ns*))
     (format "Produced: `%s`\n\n"  (.format (LocalDateTime/now)
                                            (DateTimeFormatter/ofPattern "dd-MMM-uuuu HH:mm:ss"
                                                                         (java.util.Locale. "en_GB")))))

(defn doc-var [v] (format "%s:  \n`%s`." (-> v meta :doc) (var-get v)))
{::clerk/visibility {:result :show}}




;;; # Plans & Placements EDA
;; 1. Get the SEN2 Blade.
;; 2. Extract plans & placements on census dates.
;; 3. Identify issues in the dataset of plans & placements and create an
;;    issues CSV file for review and entry of updates.
;; 4. Compare Totals with DfE Caseload
;; 5. Additional EDA



;;; ## Parameters
;;; ### Output directory
^#::clerk{:viewer clerk/md}
(doc-var #'out-dir)


;;; ### LA
^{::clerk/viewer clerk/md}
(def la-name "South Tyneside")



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
(-> plans-placements/census-dates-ds
    (tc/map-columns :census-date #(.toString %)))


;;; ### `plans-placements-on-census-dates` dataset 
^{::clerk/viewer clerk/md}
(format "Wrote `%s`  \nto working directory: `%s`:"
        (tc/dataset-name @plans-placements/plans-placements-on-census-dates)
        plans-placements/out-dir)

;; dataset structure:
^{::clerk/viewer (partial clerk/table {::clerk/width :full})}
(tc-utils/column-info-with-labels @plans-placements/plans-placements-on-census-dates
                                  @plans-placements/plans-placements-on-census-dates-col-name->label)



;;; ## 3. Issues
;;; ### Issues summary
;; Summary of issues (& numbers of CYP & records with issues) by `:census-date`:
^{::clerk/viewer (partial clerk/table {::clerk/width :full})}
(-> @plans-placements/plans-placements-on-census-dates-issues
    sen2-blade-plans-placements/drop-falsey-issue-columns
    (sen2-blade-plans-placements/summarise-issues (merge @plans-placements/checks
                                                         sen2-blade-plans-placements/checks-total-issues)))

;;; ### Establishment issues
;;; #### URNs with unexpected GIAS Establishment Types for a SEND Placement
;; To check URNs and appropriate settings:
^#::clerk{:viewer (partial clerk/table {::clerk/width :wide})}
(-> @plans-placements/plans-placements-on-census-dates-issues
    (tc/select-rows (some-fn :issue-urn-for-unexpected-gfe-gias-establishment-type
                             :issue-urn-for-unexpected-othe-gias-establishment-type))
    (tc/select-columns [:urn :census-year])
    (tc/fold-by [:urn] frequencies)
    (tc/map-rows (fn [{:keys [urn]}]
                   (-> urn
                       (@plans-placements/edubaseall-send-map)
                       (select-keys [:establishment-name :type-of-establishment-name]))))
    (tc/reorder-columns [:establishment-name :urn :type-of-establishment-name :census-year])
    (tc/order-by [:establishment-name]))

;;; #### SEN Unit | RP flagged at estab. other than URNs GIAS says has them
;; To check flagging of SENU & RP:
^#::clerk{:viewer (partial clerk/table {::clerk/width :full})}
(-> @plans-placements/plans-placements-on-census-dates-issues
    (tc/select-rows (some-fn :issue-senu-flagged-at-estab-without-one
                             :issue-resourced-provision-flagged-at-estab-without-one))
    (tc/select-columns (conj sen2-blade-plans-placements/sen2-estab-keys :census-year))
    (tc/fold-by sen2-blade-plans-placements/sen2-estab-keys frequencies)
    (tc/map-rows (fn [{:keys [urn]}]
                   (-> urn
                       (@plans-placements/edubaseall-send-map)
                       (select-keys [:establishment-name :sen-unit? :resourced-provision?]))))
    (tc/reorder-columns [:establishment-name :sen-unit? :resourced-provision?])
    (tc/order-by [:establishment-name])
    (tc/update-columns [:sen-unit? :resourced-provision? :sen-unit-indicator :resourced-provision-indicator]
                       (partial map {false "×", true "✅"}))
    (tc/convert-types {:ukprn :string, :sen-setting :string})
    (#(tc/replace-missing % (tc/column-names % #{:string} :datatype) :value " ")))

;;; #### SEN2 Establishments with fewer placed than expected
;; Flagging Specialist Provision with less 75% of the places taken by LAs CYP,
;; in particular to check SENU & RP flagging:
^#::clerk{:viewer (partial clerk/table {::clerk/width :wide})}
(-> @plans-placements/plans-placements-on-census-dates-issues
    (tc/select-rows :issue-less-placements-than-expected)
    (tc/select-columns (conj sen2-blade-plans-placements/sen2-estab-keys :census-year :issue-less-placements-than-expected))
    tc/unique-by
    (tc/order-by [:census-year])
    (tc/pivot->wider :census-year :issue-less-placements-than-expected {:drop-missing? false})
    ((fn [ds] (tc/update-columns ds (tc/column-names ds :type/numerical) (partial map #(if % (str %) "≥")))))
    (tc/map-columns :establishment-name [:urn] (comp :establishment-name @plans-placements/edubaseall-send-map))
    (tc/reorder-columns [:establishment-name])
    (tc/order-by [:establishment-name])
    (tc/update-columns [:sen-unit-indicator :resourced-provision-indicator]
                       (partial map {nil " ", false "×", true "✅"}))
    (tc/convert-types {:ukprn :string, :sen-setting :string})
    (#(tc/replace-missing % (tc/column-names % #{:string} :datatype) :value " ")))

;; Note: "≥" in the census year column indicates the number placed was above the check threshold.


;;; ### Issues dataset
^{::clerk/viewer clerk/md}
(format "Wrote `%s`  \nto working directory: `%s`:"
        (tc/dataset-name @plans-placements/plans-placements-on-census-dates-issues)
        plans-placements/out-dir)

;; dataset structure:
^{::clerk/viewer (partial clerk/table {::clerk/width :full})}
(tc-utils/column-info-with-labels @plans-placements/plans-placements-on-census-dates-issues
                                  @plans-placements/plans-placements-on-census-dates-issues-col-name->label)





;;; ## 4. Compare Totals with DfE Caseload
;;; ### #CYP with plan and/or placement record on census-date
;; With the EHCP caseload as published by the DfE at  
;; [explore-education-statistics.service.gov.uk](https://explore-education-statistics.service.gov.uk/find-statistics/education-health-and-care-plans):
^{::clerk/viewer (partial clerk/table {::clerk/width :full})}
(-> @plans-placements/plans-placements-on-census-dates
    (sen2-blade-plans-placements/plan-placement-stats la-name)
    (tc/update-columns :census-date (partial map #(.format % (DateTimeFormatter/ofPattern
                                                              "dd-MMM-uuuu"
                                                              (java.util.Locale. "en_GB")))))
    (tc/rename-columns sen2-blade-plans-placements/plan-placement-stats-col-name->label))



;;; ## 5. Additional EDA
;;; ### `sen-settings`
^#::clerk{:viewer (partial clerk/table {:clerk/width :prose})}
(-> @plans-placements/plans-placements-on-census-dates
    (tc/select-columns [:sen-setting :census-year])
    (tc/order-by [:census-year])
    (tc/fold-by [:sen-setting] frequencies)
    (tc/order-by [(comp :order sen2-dictionary/sen-setting :sen-setting)])
    (tc/update-columns [:sen-setting] (partial map #(when % (str "\"" % "\"")))))

;;; ### `sen-setting-other` strings
^#::clerk{:viewer (partial clerk/table {:clerk/width :prose})}
(-> @plans-placements/plans-placements-on-census-dates
    (tc/select-columns [:sen-setting :sen-setting-other :census-year])
    (tc/order-by [:census-year])
    (tc/fold-by [:sen-setting :sen-setting-other] frequencies)
    (tc/order-by [(comp :order sen2-dictionary/sen-setting :sen-setting)
                  :sen-setting-other])
    (tc/update-columns [:sen-setting] (partial map #(when % (str "\"" % "\"")))))
