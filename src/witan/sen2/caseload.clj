(ns witan.sen2.caseload
  "Published SEN2 EHCP caseload stats from https://explore-education-statistics.service.gov.uk/find-statistics/education-health-and-care-plans"
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as str]
            [tech.v3.dataset :as ds]
            [tablecloth.api :as tc]))



;;; # Data files
(def default-resource-file-name
  "Name of default resource CSV file containing caseload data."
  "2025-06-26-caseload.csv")



;;; # Raw data
;;; ## Utility functions
(defn time-period->census-year
  "Parse scholastic year time-period string (e.g. \"202425\") to SEN2 census-year (e.g. 2025)."
  [time-period]
  (-> time-period
      (str/replace #"^(\d{2})\d{2}(\d{2})" "$1$2")
      parse-long))

(defn parse-long-stat
  "Parse integer statistic string `s`, treating 'x' as missing."
  [s]
  (if (#{"x"} s)
    :tech.v3.dataset/missing
    (parse-long s)))

(defn parse-double-stat
  "Parse floating point statistic string `s`, treating 'x' as missing."
  [s]
  (if (#{"x"} s)
    :tech.v3.dataset/missing
    (parse-double s)))


;;; ## Metadata
(def column-spec-ds
  "Dataset of CSV data file column specifications.
     Labels from `data-guidance.txt`."
  (->
   [["time_period"                                  "Time period"]
    ["time_identifier"                              "Time identifier"]
    ["geographic_level"                             "Geographic level"]
    ["country_code"                                 "Country code"]
    ["country_name"                                 "Country name"]
    ["region_code"                                  "Region code"]
    ["region_name"                                  "Region name"]
    ["new_la_code"                                  "New LA code"]
    ["old_la_code"                                  "Old LA code"]
    ["la_name"                                      "LA name"]
    ["breakdown_topic"                              "Breakdown topic"]
    ["breakdown"                                    "Characteristics of child or young person with EHC plan"]
    ["ehcplans"                                     "Total number of EHC plans"]
    ["mainstream_la_maintained"                     "Mainstream LA maintained schools"]
    ["mainstream_la_maintained_resourced_provision" "Mainstream LA maintained schools - resourced provision"]
    ["mainstream_la_maintained_senunit"             "Mainstream LA maintained schools - SEN units"]
    ["mainstream_academy"                           "Mainstream academies"]
    ["mainstream_academy_resourced_provision"       "Mainstream academies - resourced provision"]
    ["mainstream_academy_senunit"                   "Mainstream academies - SEN units"]
    ["mainstream_free_school"                       "Mainstream free schools"]
    ["mainstream_free_school_resourced_provision"   "Mainstream free schools - resourced provision"]
    ["mainstream_free_school_senunit"               "Mainstream free schools - SEN units"]
    ["mainstream_independent"                       "Mainstream independent schools"]
    ["mainstream_total"                             "Total mainstream schools"]
    ["mainstream_total_pc"                          "Total percentage mainstream schools"]
    ["special_la_maintained"                        "LA maintained special schools"]
    ["special_academy_free"                         "Special academies and free schools"]
    ["special_independent"                          "Independent special schools"]
    ["special_non_maintained"                       "Non maintained special schools"]
    ["special_total"                                "Total special schools"]
    ["special_total_pc"                             "Total percentage special schools"]
    ["ap_pru_academy"                               "Alternative provision academies"]
    ["ap_pru_free_school"                           "Alternative provision free schools"]
    ["ap_pru_la_maintained"                         "Pupil referral units"]
    ["ap_pru_total"                                 "Total alternative provision"]
    ["AP_PRU_total_pc"                              "Total percentage alternative provision"]
    ["general_fe_tertiary_colleges"                 "FE colleges and sixth forms"]
    ["specialist_post_16_institutions"              "Specialist post 16 establishments"]
    ["ukrlp_provider"                               "UKRLP providers"]
    ["fe_total"                                     "Total further education and post 16"]
    ["fe_total_pc"                                  "Total percentage further education and post 16"]
    ["elective_home_education"                      "Elective home education"]
    ["other_arrangements_la"                        "Other arrangements made by LA"]
    ["other_arrangements_parents"                   "Other arrangements made by parents"]
    ["online_provider"                              "Online providers"]
    ["w_settings"                                   "Welsh schools and establishments"]
    ["other_schools"                                "Other school types"]
    ["other_placement_settings"                     "Other types of placement"]
    ["neet"                                         "Not in employment, education or training"]
    ["neet_ntci"                                    "Not in education or training - notice to cease issued"]
    ["neet_other"                                   "Not in education or training - other age groups"]
    ["neet_other_csa"                               "Not in education or training - compulsory school age"]
    ["ed_elsewhere"                                 "Total educated elsewhere"]
    ["ed_elsewhere_pc"                              "Total percentage educated elsewhere"]
    ["nm_early_years"                               "Non maintained early years"]
    ["nm_early_years_pc"                            "Non maintained early years percentage"]
    ["placement_unknown"                            "Placement not recorded"]
    ["placement_unknown_pc"                         "Placement not recorded percentage"]
    ["await_prov_2022"                              "Awaiting provision (pre 2023 only)"]
    ["perm_ex_2022"                                 "Permanently excluded (pre 2023 only)"]]
   (tc/dataset {:column-names [:csv-col-name :col-label]})
   (tc/map-columns :col-name [:csv-col-name] (comp keyword #(str/replace % #"_" "-")))
   (tc/add-column :parser-fn (fn [ds]
                               (let [string-csv-col-names  (into #{} (take-while
                                                                      (complement #{"ehcplans"})
                                                                      (:csv-col-name ds)))]
                                 (map #(cond
                                         (string-csv-col-names %) :string
                                         (re-find #"_pc$" %)      [:float parse-double-stat]
                                         :else                    [:int32 parse-long-stat])
                                      (:csv-col-name ds)))))
   (tc/reorder-columns [:col-name])))

(def src-col-name->col-name
  "Map source data file column name to dataset column name."
  (apply zipmap ((juxt :csv-col-name :col-name) column-spec-ds)))

(def parser-fn
  "Parser function for reading data file."
  (apply zipmap ((juxt :col-name :parser-fn) column-spec-ds)))


;;; ## Read raw data file
(defn ->ds
  "Read caseload CSV file into dataset, adding `:census-year` column."
  [& {::keys [resource-file-name file-path key-fn parser-fn dataset-name]
      :or    {resource-file-name default-resource-file-name
              key-fn             src-col-name->col-name
              parser-fn          parser-fn}
      :as    options}]
  (with-open [in (-> (or file-path (io/resource resource-file-name))
                     io/file
                     io/input-stream)]
    (-> (ds/->dataset in (merge {:file-type    :csv
                                 :separator    ","
                                 :dataset-name (or dataset-name file-path resource-file-name)
                                 :header-row?  true
                                 :key-fn       key-fn
                                 :parser-fn    parser-fn}
                                options))
        (tc/map-columns :census-year :int16 [:time-period] time-period->census-year)
        (tc/reorder-columns [:census-year]))))

(def col-name->label
  "Map dataset column names to display labels."
  (merge {:census-year "SEN2 census year (derived from \"time period\")"}
         (apply zipmap ((juxt :col-name :col-label) column-spec-ds))))

(comment ;; EDA: Structure of raw caseload dataset
  (defn- csv-ds-column-info
    "Column info for a dataset `ds` read from CSV file."
    [ds col-name->src-col-name col-name->label]
    (-> ds
        (tc/info)
        (tc/select-columns [:col-name :datatype :n-valid :n-missing :min :max])
        (tc/map-columns :csv-col-name [:col-name] col-name->src-col-name)
        (tc/map-columns :col-label    [:col-name] col-name->label)
        (tc/reorder-columns [:col-name :csv-col-name :col-label])))

  (-> (->ds
       #_{::resource-file-name default-resource-file-name})
      (csv-ds-column-info (set/map-invert src-col-name->col-name)
                          col-name->label)
      (vary-meta assoc :print-index-range 1000))
  ;;=> 2025-06-26-caseload.csv: descriptive-stats [61 8]:
  ;;   
  ;;   |                                     :col-name |                                :csv-col-name |                                             :col-label | :datatype | :n-valid | :n-missing |   :min |     :max |
  ;;   |-----------------------------------------------|----------------------------------------------|--------------------------------------------------------|-----------|---------:|-----------:|-------:|---------:|
  ;;   |                                  :census-year |                                              |          SEN2 census year (derived from "time period") |    :int16 |    25339 |          0 | 2019.0 |   2025.0 |
  ;;   |                                  :time-period |                                  time_period |                                            Time period |   :string |    25339 |          0 |        |          |
  ;;   |                              :time-identifier |                              time_identifier |                                        Time identifier |   :string |    25339 |          0 |        |          |
  ;;   |                             :geographic-level |                             geographic_level |                                       Geographic level |   :string |    25339 |          0 |        |          |
  ;;   |                                 :country-code |                                 country_code |                                           Country code |   :string |    25339 |          0 |        |          |
  ;;   |                                 :country-name |                                 country_name |                                           Country name |   :string |    25339 |          0 |        |          |
  ;;   |                                  :region-code |                                  region_code |                                            Region code |   :string |    25174 |        165 |        |          |
  ;;   |                                  :region-name |                                  region_name |                                            Region name |   :string |    25174 |        165 |        |          |
  ;;   |                                  :new-la-code |                                  new_la_code |                                            New LA code |   :string |    23708 |       1631 |        |          |
  ;;   |                                  :old-la-code |                                  old_la_code |                                            Old LA code |   :string |    23708 |       1631 |        |          |
  ;;   |                                      :la-name |                                      la_name |                                                LA name |   :string |    23708 |       1631 |        |          |
  ;;   |                              :breakdown-topic |                              breakdown_topic |                                        Breakdown topic |   :string |    25339 |          0 |        |          |
  ;;   |                                    :breakdown |                                    breakdown | Characteristics of child or young person with EHC plan |   :string |    25339 |          0 |        |          |
  ;;   |                                     :ehcplans |                                     ehcplans |                              Total number of EHC plans |    :int32 |    25245 |         94 |    1.0 | 638745.0 |
  ;;   |                     :mainstream-la-maintained |                     mainstream_la_maintained |                       Mainstream LA maintained schools |    :int32 |    25092 |        247 |    0.0 |  95542.0 |
  ;;   | :mainstream-la-maintained-resourced-provision | mainstream_la_maintained_resourced_provision | Mainstream LA maintained schools - resourced provision |    :int32 |    25092 |        247 |    0.0 |   6730.0 |
  ;;   |             :mainstream-la-maintained-senunit |             mainstream_la_maintained_senunit |           Mainstream LA maintained schools - SEN units |    :int32 |    25092 |        247 |    0.0 |   3674.0 |
  ;;   |                           :mainstream-academy |                           mainstream_academy |                                   Mainstream academies |    :int32 |    25092 |        247 |    0.0 | 141630.0 |
  ;;   |       :mainstream-academy-resourced-provision |       mainstream_academy_resourced_provision |             Mainstream academies - resourced provision |    :int32 |    25092 |        247 |    0.0 |   8578.0 |
  ;;   |                   :mainstream-academy-senunit |                   mainstream_academy_senunit |                       Mainstream academies - SEN units |    :int32 |    25092 |        247 |    0.0 |   5773.0 |
  ;;   |                       :mainstream-free-school |                       mainstream_free_school |                                Mainstream free schools |    :int32 |    25092 |        247 |    0.0 |   9366.0 |
  ;;   |   :mainstream-free-school-resourced-provision |   mainstream_free_school_resourced_provision |          Mainstream free schools - resourced provision |    :int32 |    25091 |        248 |    0.0 |    221.0 |
  ;;   |               :mainstream-free-school-senunit |               mainstream_free_school_senunit |                    Mainstream free schools - SEN units |    :int32 |    25091 |        248 |    0.0 |    104.0 |
  ;;   |                       :mainstream-independent |                       mainstream_independent |                         Mainstream independent schools |    :int32 |    25092 |        247 |    0.0 |   7174.0 |
  ;;   |                             :mainstream-total |                             mainstream_total |                               Total mainstream schools |    :int32 |    25092 |        247 |    0.0 | 278236.0 |
  ;;   |                          :mainstream-total-pc |                          mainstream_total_pc |                    Total percentage mainstream schools |  :float32 |    25092 |        247 |    0.0 |    100.0 |
  ;;   |                        :special-la-maintained |                        special_la_maintained |                          LA maintained special schools |    :int32 |    25092 |        247 |    0.0 |  87350.0 |
  ;;   |                         :special-academy-free |                         special_academy_free |                     Special academies and free schools |    :int32 |    25092 |        247 |    0.0 |  73932.0 |
  ;;   |                          :special-independent |                          special_independent |                            Independent special schools |    :int32 |    25092 |        247 |    0.0 |  29647.0 |
  ;;   |                       :special-non-maintained |                       special_non_maintained |                         Non maintained special schools |    :int32 |    25092 |        247 |    0.0 |   4381.0 |
  ;;   |                                :special-total |                                special_total |                                  Total special schools |    :int32 |    25092 |        247 |    0.0 | 193880.0 |
  ;;   |                             :special-total-pc |                             special_total_pc |                       Total percentage special schools |  :float32 |    25092 |        247 |    0.0 |    100.0 |
  ;;   |                               :ap-pru-academy |                               ap_pru_academy |                        Alternative provision academies |    :int32 |    25092 |        247 |    0.0 |   1967.0 |
  ;;   |                           :ap-pru-free-school |                           ap_pru_free_school |                     Alternative provision free schools |    :int32 |    25092 |        247 |    0.0 |    596.0 |
  ;;   |                         :ap-pru-la-maintained |                         ap_pru_la_maintained |                                   Pupil referral units |    :int32 |    25092 |        247 |    0.0 |   2298.0 |
  ;;   |                                 :ap-pru-total |                                 ap_pru_total |                            Total alternative provision |    :int32 |    25092 |        247 |    0.0 |   4858.0 |
  ;;   |                              :AP-PRU-total-pc |                              AP_PRU_total_pc |                 Total percentage alternative provision |  :float32 |    25092 |        247 |    0.0 |    100.0 |
  ;;   |                 :general-fe-tertiary-colleges |                 general_fe_tertiary_colleges |                            FE colleges and sixth forms |    :int32 |    25092 |        247 |    0.0 |  70998.0 |
  ;;   |              :specialist-post-16-institutions |              specialist_post_16_institutions |                      Specialist post 16 establishments |    :int32 |    25092 |        247 |    0.0 |   9675.0 |
  ;;   |                               :ukrlp-provider |                               ukrlp_provider |                                        UKRLP providers |    :int32 |    24446 |        893 |    0.0 |   8188.0 |
  ;;   |                                     :fe-total |                                     fe_total |                    Total further education and post 16 |    :int32 |    25092 |        247 |    0.0 |  88158.0 |
  ;;   |                                  :fe-total-pc |                                  fe_total_pc |         Total percentage further education and post 16 |  :float32 |    25092 |        247 |    0.0 |    100.0 |
  ;;   |                      :elective-home-education |                      elective_home_education |                                Elective home education |    :int32 |    24930 |        409 |    0.0 |   7155.0 |
  ;;   |                        :other-arrangements-la |                        other_arrangements_la |                          Other arrangements made by LA |    :int32 |    25092 |        247 |    0.0 |  11524.0 |
  ;;   |                   :other-arrangements-parents |                   other_arrangements_parents |                     Other arrangements made by parents |    :int32 |    25092 |        247 |    0.0 |   2809.0 |
  ;;   |                              :online-provider |                              online_provider |                                       Online providers |    :int32 |    12182 |      13157 |    0.0 |    123.0 |
  ;;   |                                   :w-settings |                                   w_settings |                       Welsh schools and establishments |    :int32 |    12182 |      13157 |    0.0 |    402.0 |
  ;;   |                                :other-schools |                                other_schools |                                     Other school types |    :int32 |    12182 |      13157 |    0.0 |    175.0 |
  ;;   |                     :other-placement-settings |                     other_placement_settings |                               Other types of placement |    :int32 |    25188 |        151 |    0.0 |   4498.0 |
  ;;   |                                         :neet |                                         neet |               Not in employment, education or training |    :int32 |    25092 |        247 |    0.0 |  18056.0 |
  ;;   |                                    :neet-ntci |                                    neet_ntci |  Not in education or training - notice to cease issued |    :int32 |    24446 |        893 |    0.0 |   2268.0 |
  ;;   |                                   :neet-other |                                   neet_other |        Not in education or training - other age groups |    :int32 |    24446 |        893 |    0.0 |   2992.0 |
  ;;   |                               :neet-other-csa |                               neet_other_csa |   Not in education or training - compulsory school age |    :int32 |    24446 |        893 |    0.0 |   2460.0 |
  ;;   |                                 :ed-elsewhere |                                 ed_elsewhere |                               Total educated elsewhere |    :int32 |    25092 |        247 |    0.0 |  49750.0 |
  ;;   |                              :ed-elsewhere-pc |                              ed_elsewhere_pc |                    Total percentage educated elsewhere |  :float32 |    25092 |        247 |    0.0 |    100.0 |
  ;;   |                               :nm-early-years |                               nm_early_years |                             Non maintained early years |    :int32 |    25092 |        247 |    0.0 |   4524.0 |
  ;;   |                            :nm-early-years-pc |                            nm_early_years_pc |                  Non maintained early years percentage |  :float32 |    25092 |        247 |    0.0 |    100.0 |
  ;;   |                            :placement-unknown |                            placement_unknown |                                 Placement not recorded |    :int32 |    24446 |        893 |    0.0 |  19339.0 |
  ;;   |                         :placement-unknown-pc |                         placement_unknown_pc |                      Placement not recorded percentage |  :float32 |    24446 |        893 |    0.0 |    100.0 |
  ;;   |                              :await-prov-2022 |                              await_prov_2022 |                     Awaiting provision (pre 2023 only) |    :int32 |      646 |      24693 |    0.0 |   6342.0 |
  ;;   |                                 :perm-ex-2022 |                                 perm_ex_2022 |                   Permanently excluded (pre 2023 only) |    :int32 |      646 |      24693 |    0.0 |    121.0 |
  ;;   
  
  :rcf)
