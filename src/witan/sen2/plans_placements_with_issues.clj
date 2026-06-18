(ns witan.sen2.plans-placements-with-issues
  (:require [witan.sen2 :as sen2]
            [witan.sen2.return.person-level.blade.csv :as sen2-blade-csv]
            [witan.sen2.return.person-level.blade.plans-placements :as plans]
            [witan.sen2.return.person-level.blade.pre-submission :as pre]
            [tablecloth.api :as tc]
            [kixi.large :as large]))

(defn raw-sen2->plans-placements-with-issues
  [{:keys [census-years data-path export-date module-read-cfg
           updates-file manually-updated-sen2 checks]
    :or {module-read-cfg sen2-blade-csv/module-read-cfg
         checks (plans/checks)}}]
  "Process raw SEN2 data or apply updates/fixes to SEN2 data. Expects a map with required and optional keys.
   Required key-values:
   - `:census-years`: vector of calendar years covered in this SEN2 data
   - `:data-path`: location of SEN2 data files
   - `:export-date`: date COLLECT data generated (in \"DD-MM-YYYY\" format)
   Optional extra key-values:
   - `:updates-file`: filepath of file with fixes/updates to records - usually created by manually updating
      the output of `write-out-issues`
   - `:manually-updated-sen2`: an updated SEN2 ds to be used where fixes are too complicated to be applied
      via an updates files (e.g. blanket fixes to RP/SENU labelling)
   - `module-read-cfg`: see `sen2-blade-csv/module-read-cfg` for applying bespoke SEN2 module parsing, otherwise defaults
   - `:checks`: see `plans/checks` for applying bespoke SEN2 issue checks, otherwise defaults"
  (let [sen2-blade-csv-ds-map (sen2-blade-csv/file-paths->ds-map (sen2-blade-csv/make-file-paths export-date data-path)
                                                                 module-read-cfg)
        census-dates-ds (sen2/census-years->census-dates-ds census-years)
        sen2-census-raw (if manually-updated-sen2
                          manually-updated-sen2
                          (plans/plans-placements-on-census-dates sen2-blade-csv-ds-map
                                                                  census-dates-ds))]
    (as-> {:blade-csv-ds-map sen2-blade-csv-ds-map
           :census-raw (if updates-file
                         (as-> updates-file $
                           (plans/updates-csv-file->ds $)
                           (plans/update-plans-placements-on-census-dates sen2-census-raw $))
                         sen2-census-raw)
           :checks checks
           :census-dates census-dates-ds} $
      (assoc $ :issues (plans/issues->ds (:census-raw $) checks))
      (assoc $ :issues-summary (-> (:issues $)
                                   (plans/issues->ds checks)
                                   (plans/summarise-issues checks))))))

(defn raw-sen2-pre-submission->plans-placements-with-issues
  [{:keys [census-years data-path module-read-cfg
           updates-file manually-updated-sen2 checks
           sheet-names]
    :or {module-read-cfg sen2-blade-csv/module-read-cfg
         checks (plans/checks)}}]
  "Process raw SEN2 data or apply updates/fixes to SEN2 data. Expects a map with required and optional keys.
   Required key-values:
   - `:census-years`: vector of calendar years covered in this SEN2 data
   - `:data-path`: location of SEN2 xlsx pre-submission file
   - `:sheet-names`: map of required dataset keywords to sheet/tab names in workbook
   Optional extra key-values:
   - `:updates-file`: filepath of file with fixes/updates to records - usually created by manually updating
      the output of `write-out-issues`
   - `:manually-updated-sen2`: an updated SEN2 ds to be used where fixes are too complicated to be applied
      via an updates files (e.g. blanket fixes to RP/SENU labelling)
   - `module-read-cfg`: see `sen2-blade-csv/module-read-cfg` for applying bespoke SEN2 module parsing, otherwise defaults
   - `:checks`: see `plans/checks` for applying bespoke SEN2 issue checks, otherwise defaults"
  (let [sen2-ds-map (pre/pre-submission-workbook-filepath->ds-map data-path
                                                                  {:module-sheet-names sheet-names
                                                                   :module-read-cfg    module-read-cfg})
        census-dates-ds (sen2/census-years->census-dates-ds census-years)
        sen2-census-raw (if manually-updated-sen2
                          manually-updated-sen2
                          (plans/plans-placements-on-census-dates sen2-ds-map
                                                                  census-dates-ds))]
    (as-> {:blade-csv-ds-map sen2-ds-map
           :census-raw (if updates-file
                         (->> updates-file
                              plans/updates-csv-file->ds
                              plans/update-plans-placements-on-census-dates sen2-census-raw)
                         sen2-census-raw)
           :checks checks
           :census-dates census-dates-ds} $
      (assoc $ :issues (plans/issues->ds (:census-raw $) checks))
      (assoc $ :issues-summary (-> (:issues $)
                                   (plans/issues->ds checks)
                                   (plans/summarise-issues checks))))))

(defn write-out-issues [issues issues-csv]
  (tc/write! issues issues-csv))

(defn generate-records-with-missing-data [{:keys [issues-ds issue-key id-key]
                                           :or {id-key :upn}}]
  ;; issue keys can include, but not limited to `:issue-no-placement-detail`,
  ;; `:issue-missing-sen-type`
  (-> issues-ds
      (tc/select-rows #(and ((complement nil?) (id-key %))
                            (true? (issue-key %))))
      (tc/unique-by [id-key :census-year])
      (tc/select-columns [id-key :census-year])
      (tc/add-column :update nil)
      (tc/order-by [:census-year id-key])))

(defn write-out-records-with-missing-data [map-of-missing-data filepath]
  ;; `map-of-missing-data should be a map with key corresponding to the
  ;; issue (e.g. \"Missing placements\") and value being
  (as-> map-of-missing-data $
    (mapv (fn [[k v]] (assoc {}
                             ::large/sheet-name k
                             ::large/data v)) $)
    (large/create-workbook $)
    (large/save-workbook! $ filepath)))
