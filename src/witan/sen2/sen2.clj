(ns witan.sen2.sen2
  (:require [clojure.java.io :as io]
            [tablecloth.api :as tc]))

"A script which pulls data for a single gss-code from the SEN2 caseload, and generates total SEN2 counts per LAn"

(def caseload-2010-2022 (delay (-> "sen2_estab_caseload.csv"
                                   io/resource
                                   io/file
                                   (tc/dataset {:key-fn keyword})
                                   (tc/select-rows #(and (= (:establishment_type %) "Total")
                                                         (= (:establishment_group %) "Total")))
                                   (tc/select-columns [:Total_all :time_period
                                                       :new_la_code :la_name])))) ;; this doesn't work with districts

(defn generate-current-pop [s la-or-gss]
  (let [pred (cond
               (= la-or-gss :gss)
               (fn [x] (= (:new_la_code x) s))

               (= la-or-gss :la)
               (fn [x] (= (:la_name x) s)))]
    (tc/select-rows @caseload-2010-2022 #(pred %))))
