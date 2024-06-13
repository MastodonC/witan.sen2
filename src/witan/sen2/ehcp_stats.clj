(ns witan.sen2.ehcp-stats
  "GOV.UK EHCP stats from https://explore-education-statistics.service.gov.uk/find-statistics/education-health-and-care-plans"
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [tablecloth.api :as tc]))

(def caseload (delay (-> "sen2_estab_caseload.csv"
                         io/resource
                         io/file
                         (tc/dataset {:key-fn    (comp keyword #(string/replace % #"_" "-"))
                                      :parser-fn {:num-caseload [:int32 #(if (#{"z" "x"} %)
                                                                           :tech.v3.dataset/missing
                                                                           (parse-long %))]}})
                         (tc/select-rows #(and (= (:establishment-group %) "Total")
                                               (= (:ehcp-or-statement   %) "Total")))
                         (tc/select-columns [:new-la-code
                                             :old-la-code
                                             :la-name
                                             :time-period
                                             :num-caseload])))) ;; this doesn't work with districts

(defn generate-current-pop [s la-or-gss]
  "Returns total EHCP caseload for LA `s` specified by name (when `la-or-gss` is `:la`) or GSS code (when `la-or-gss` is `:gss`)"
  (let [pred (cond
               (= la-or-gss :gss) (fn [x] (= (:new-la-code x) s))
               (= la-or-gss :la ) (fn [x] (= (:la-name     x) s)))]
    (tc/select-rows @caseload #(pred %))))
