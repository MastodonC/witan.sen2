(ns witan.sen2.ehcp-stats
  "GOV.UK EHCP stats from https://explore-education-statistics.service.gov.uk/find-statistics/education-health-and-care-plans"
  (:require [clojure.java.io :as io]
            [tablecloth.api :as tc]))

(def caseload (delay (-> "sen2_estab_caseload.csv"
                         io/resource
                         io/file
                         (tc/dataset {:key-fn keyword})
                         (tc/select-rows #(and (= (:establishment_group %) "Total")
                                               (= (:ehcp_or_statement %) "Total")))
                         (tc/select-columns [:num_caseload :time_period
                                             :new_la_code :la_name])))) ;; this doesn't work with districts

(defn generate-current-pop [s la-or-gss]
  "Returns total EHCP caseload for LA `s` specified by name (when `la-or-gss` is `:la`) or GSS code (when `la-or-gss` is `:gss`)"
  (let [pred (cond
               (= la-or-gss :gss) (fn [x] (= (:new_la_code x) s))
               (= la-or-gss :la ) (fn [x] (= (:la_name     x) s)))]
    (tc/select-rows @caseload #(pred %))))
