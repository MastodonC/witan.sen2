(ns witan.sen2
  "Special educational needs survey (SEN2) information
   from [gov.uk](https://www.gov.uk/guidance/special-educational-needs-survey)
   unless stated otherwise."
  (:require [tablecloth.api :as tc]))

;;; # SEN2 census dates
(def census-year->date-string
  "Map SEN2 census year to census date (as ISO8601 date strings)"
  (sorted-map
   2015 "2015-01-15" ; from [SEN2 2015 Guide v1.3](https://dera.ioe.ac.uk/21852/1/SEN2_2015_Guide_Version_1.3.pdf) (retrieved 2023-04-12)
   2016 "2016-01-21" ; from [SEN2 2016 Guide v1.3](https://dera.ioe.ac.uk/24874/1/SEN2_2016_Guide_Version_1.3.pdf) (retrieved 2023-04-12)
   2017 "2017-01-19" ; from [SEN2 2017 Guide v1.2](https://dera.ioe.ac.uk/27646/1/SEN2_2017_Guide_Version_1.2.pdf) (retrieved 2023-04-12)
   2018 "2018-01-18" ; from [SEN2 2018 Guide v1.2](https://dera.ioe.ac.uk/30003/1/SEN2_2018_Guide_Version_1.2.pdf) (retrieved 2023-04-12)
   2019 "2019-01-17" ; from [SEN2 2019 Guide v1.2](https://dera.ioe.ac.uk/32271/1/SEN2_2019_Guide_Version_1.2.pdf) (retrieved 2023-04-12)
   2020 "2020-01-16" ; from [SEN2 2020 Guide v1.1](https://dera.ioe.ac.uk/34219/1/SEN2_2020_Guide_Version_1.1.pdf) (retrieved 2023-04-12)
   2021 "2021-01-14" ; from [SEN2 2021 Guide v1.1](https://dera.ioe.ac.uk/36468/1/SEN2_2021_Guide_Version_1.1.pdf) (retrieved 2023-04-12)
   2022 "2022-01-20" ; from [SEN2 2022 Guide v1.1](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1013751/SEN2_2022_Guide.pdf) (retrieved 2023-04-12)
   2023 "2023-01-19" ; from [SEN2 2023 Guide v1.0](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1099346/2023_SEN2_Person_level_-_Guide_Version_1.0.pdf) (retrieved 2023-04-12)
   2024 "2024-01-18" ; from [SEN2 2024 Guide v1.0](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1174521/2024_SEN2_Person_level_-_Guide_Version_1.0.pdf) (retrieved 2023-07-28)
   2025 "2025-01-16" ; from [SEN2 2025 Guide v1.0](https://assets.publishing.service.gov.uk/media/656892015936bb000d3167c0/2025_SEN2_person_level_guide_v1.pdf) (retrieved 2023-12-01)
   2026 "2026-01-15" ; from [gov.uk SENB2 page](https://www.gov.uk/guidance/special-educational-needs-survey) (retrieved 2025-01-28)
   2027 "2027-01-21" ; from [gov.uk SENB2 page](https://www.gov.uk/guidance/special-educational-needs-survey) (retrieved 2025-09-26)
   ))

(def census-year->date
  "Map SEN2 census year to census date"
  (update-vals census-year->date-string
               #(java.time.LocalDate/parse %
                                           (java.time.format.DateTimeFormatter/ofPattern "uuuu-MM-dd"
                                                                                         (java.util.Locale. "en_GB")))))

(defn date->census-date
  "Given a date `d` and vector of `census-dates`, returns the (first) SEN2 census-date on or after that date.
   Returns `nil` for dates `d` that are before (or on) the first defined census-date
   or after the last defined census-date."
  ([d] (date->census-date d (vals census-year->date)))
  ([d census-dates]
   (let [sorted-census-dates (sort census-dates)]
     (when (.isBefore (first sorted-census-dates) d)
       (some #(if (.isBefore % d) nil %)
             sorted-census-dates)))))

(defn census-years->census-dates-ds
  "Return dataset of census-dates given vector of `census-years`."
  [census-years]
  (-> (tc/dataset {:census-year census-years})
      (tc/convert-types {:census-year :int16})
      (tc/map-columns :census-date [:census-year] census-year->date)
      (tc/set-dataset-name "census-dates")))

(def census-dates-col-name->label
  "Column labels for display."
  {:census-year "SEN2 census year"
   :census-date "SEN2 census date"})

