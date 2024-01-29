(ns sen2-blade-eda
  "EDA of SEN2 Blade."
  {:nextjournal.clerk/toc                  true
   :nextjournal.clerk/visibility           {:code   :hide
                                            :result :show}
   :nextjournal.clerk/page-size            nil
   :nextjournal.clerk/auto-expand-results? true
   :nextjournal.clerk/budget               nil}
  (:require [clojure.string :as string]
            [clojure.java.io :as io]
            [nextjournal.clerk :as clerk]
            [sen2-blade :as sen2-blade] ; <- replace with workpackage specific version
            [witan.sen2.return.person-level.blade.eda :as sen2-blade-eda]))

^{;; Notebook header
  ::clerk/no-cache true}
(clerk/md (str "![Mastodon C](https://www.mastodonc.com/wp-content/themes/MastodonC-2018/dist/images/logo_mastodonc.png)  \n"
               "# witan.sen2"
               (format "  \n`%s`  \n" *ns*)
               ((comp :doc meta) *ns*)
               "  \nTimeStamp: " (.format (java.time.LocalDateTime/now)
                                          (java.time.format.DateTimeFormatter/ISO_LOCAL_DATE_TIME))))



;;; # SEN2 Blade EDA
;;; ## Parameters
;;; ### Output directory
^{::clerk/visibility {:result :hide}}
(def out-dir
  "Output directory"
  "./tmp/")

^{::clerk/viewer clerk/md}
(format "%s: `%s`." ((comp :doc meta) #'out-dir) out-dir)


;;; ### SEN2 Blade
;; Read from:
^{::clerk/viewer (partial clerk/table {::clerk/width :prose})}
(into [["Module Key" "File Path" "Exists?"]]
      (map (fn [[k v]]
             [k v (if (.exists (io/file v)) "✅" "❌")]))
      sen2-blade/file-paths)

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
(sen2-blade-eda/report-table-keys)


;;; ### `*-table-id` Key relationships
;; The hierarchy is proper (due to COLLECT):
;; - primary keys are unique
;; - all foreign keys in child are contained in parent
;; - not all parent records have children
;; - some parents have multiple children
(sen2-blade-eda/report-key-relationships @sen2-blade/ds-map)


;;; ### `table-id-ds`
(sen2-blade-eda/report-table-id-ds @sen2-blade/table-id-ds)



;;; ## Composite keys
;; Note: OK if not a unique key without `requests-table-id`,
(sen2-blade-eda/report-composite-keys @sen2-blade/ds-map)




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
