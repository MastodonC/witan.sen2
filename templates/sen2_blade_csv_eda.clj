(ns sen2-blade-csv-eda
  "EDA of SEN2 Blade read from COLLECT Blade CSV export."
  #:nextjournal.clerk{:toc                  true
                      :visibility           {:code   :hide
                                             :result :hide}
                      :page-size            nil
                      :auto-expand-results? true
                      :budget               nil}
  (:require [clojure.string :as str]
            [clojure.java.io :as io]
            [nextjournal.clerk :as clerk]
            [sen2-blade-csv :as sen2-blade] ; <- replace with workpackage specific version
            [witan.sen2.return.person-level.blade.eda :as sen2-blade-eda]
            [witan.send.adroddiad.clerk.html :as chtml]))

(def client-name      "Mastodon C")
(def workpackage-name "witan.sen2")
(def out-dir "Output directory" "./tmp/")

(defn doc-var [v] (format "%s:  \n`%s`." (-> v meta :doc) (var-get v)))

{::clerk/visibility {:result :show}}
^#::clerk{:viewer clerk/md, :no-cache true} ; Notebook header
(str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
     (format "# %s SEND %s  \n" client-name workpackage-name)
     (format "`%s`\n\n" *ns*)
     (format "%s\n\n" ((comp :doc meta) *ns*))
     (format "Produced: `%s`\n\n"  (.format (java.time.LocalDateTime/now)
                                            (java.time.format.DateTimeFormatter/ofPattern "dd-MMM-uuuu HH:mm:ss"
                                                                                          (java.util.Locale. "en_GB")))))
;;; # SEN2 Blade EDA
;;; ## Parameters
;;; ### Output directory
^#::clerk{:viewer clerk/md}
(doc-var #'out-dir)


;;; ### SEN2 Blade
^#::clerk{:viewer clerk/md}
(format "Read from: `%s`:" sen2-blade/data-dir)
^#::clerk{:viewer (partial clerk/table {::clerk/width :prose})}
(into [["Module Key" "File Name" "Exists?"]]
      (map (fn [[k v]]
             [k v (if (.exists (io/file (str sen2-blade/data-dir v))) "✅" "❌")]))
      sen2-blade/file-names)

;; NOTE: The `person` module should be de-identified as follows:
;; - [x] Contents of the `surname` field deleted.
;; - [x] Contents of the `forename` field deleted.
;; - [x] Contents of `personbirthdate`
;;       (which were of the form "YYYY-Mmm-DD 00:00:00")
;;       edited to first day of the month
;;       (so of the form "YYYY-Mmm-01 00:00:00").
;; - [x] Contents of the `postcode` field deleted.



;;; ## Dataset structure & categorical values
(sen2-blade-eda/report-all-module-info
 @sen2-blade/ds-map
 {:module-titles                       sen2-blade/module-titles
  :module-col-name->label              sen2-blade/module-col-name->label
  :module-src-col-name->col-name       sen2-blade/module-src-col-name->col-name})



;;; ## Database structure
(sen2-blade-eda/report-expected-schema)



;;; ## COLLECT `:*-table-id`s
(sen2-blade-eda/report-collect-keys)


;;; ### Table relationships by COLLECT `:*-table-id`
;; The hierarchy is proper (due to COLLECT):
;; - primary keys are unique
;; - all foreign keys in child are contained in parent
;; - not all parent records have children
;; - some parents have multiple children
(sen2-blade-eda/report-collect-key-relationships @sen2-blade/ds-map)


;;; ### `table-id-ds`
(sen2-blade-eda/report-table-id-ds @sen2-blade/table-id-ds)



;;; ## `:person-table-id` & `:requests-table-id`
;; For person, named-plan, placement-detail & sen-need modules with ancestor `:*-table-id`s.


;;; ### Table relationships by `:person-table-id` & `:requests-table-id`
(sen2-blade-eda/report-key-relationships @sen2-blade/ds-map)


;;; ### Unique keys
;; Note: Except for `person`, OK if not a unique key without `requests-table-id`,
(sen2-blade-eda/report-unique-keys @sen2-blade/ds-map)
(sen2-blade-eda/report-table-keys)



;;; ## Output
^#::clerk{:viewer clerk/md}
(str "This notebook (as HTML)"
     ":  \n`" out-dir (clojure.string/replace (str *ns*) #"^.*\." "") ".html`")

^#::clerk{:visibility {:result :hide}}
(comment ;; clerk build to a standalone html file
  (when (chtml/build-ns! *ns* {:project-path "./templates"
                               :out-dir      "./tmp"})
    (clerk/show! (chtml/ns->filepath *ns* "./templates")))

  )
