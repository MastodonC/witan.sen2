(ns witan.sen2.sen2
  (:require [clojure.java.io :as io]
            [tablecloth.api :as tc]))

"A script which pulls data for a single gss-code from the SEN2 caseload, and generates total SEN2 counts per LAn"

(def caseload-2010-2022 (delay (-> "sen2_estab_caseload.csv"
                                   io/resource
                                   io/file
                                   tc/dataset))) ;; this doesn't work with districts

(defn process-sen2-caseload [year caseload-data]
  (-> caseload-data
      (tc/select-rows #(and (= (get % "time_period") year)
                            (= (get % "establishment_type") "Total")
                            (= (get % "establishment_group") "Total")))
      (tc/select-columns ["Total_all" "time_period" "new_la_code" "la_name"])))


(defn generate-current-pop [gss]
  (apply tc/concat
         (map (fn [year] (process-sen2-caseload gss year @caseload-2010-2022))
              (range 2010 2023))))
