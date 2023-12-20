(ns witan.sen2-test
  (:require [clojure.test :refer [deftest testing is]]
            [witan.sen2 :as sen2]))

(def census-dates
  "SEN2 census dates for 2021-2024 for testing."
  (-> sen2/census-year->date
      (select-keys [2021 2022 2023 2024])
      vals))

(comment
  census-dates
  ;; => (#object[java.time.LocalDate 0x5f880321 "2021-01-14"]
  ;;     #object[java.time.LocalDate 0x2e23b3c8 "2022-01-20"]
  ;;     #object[java.time.LocalDate 0x21e38366 "2023-01-19"]
  ;;     #object[java.time.LocalDate 0x37ac52c "2024-01-18"])
  )

(deftest date->census-date
  (testing "A date on or before first census-date should return nil."
    (is (nil? (sen2/date->census-date (java.time.LocalDate/parse "2021-01-13")
                                      census-dates))
        "Date before first census-date did not return nil.")
    (is (nil? (sen2/date->census-date (java.time.LocalDate/parse "2021-01-14")
                                      census-dates))
        "Date on first census-date did not return nil."))

  (testing "A date tat is a census date after the first should return the same."
    (is (= (sen2/date->census-date (java.time.LocalDate/parse "2022-01-20")
                                   census-dates)
           (java.time.LocalDate/parse "2022-01-20"))
        "2022 census date did not return same.")
    (is (= (sen2/date->census-date (java.time.LocalDate/parse "2023-01-19")
                                   census-dates)
           (java.time.LocalDate/parse "2023-01-19"))
        "2023 census date did not return same.")
    (is (= (sen2/date->census-date (java.time.LocalDate/parse "2024-01-18")
                                   census-dates)
           (java.time.LocalDate/parse "2024-01-18"))
        "2024 census date did not return same."))

  (testing "A date that is between census dates should the following census date."
    (is (= (sen2/date->census-date (java.time.LocalDate/parse "2022-01-21")
                                   census-dates)
           (java.time.LocalDate/parse "2023-01-19"))
        "Date day after 2022 census date did not return 2023 census date.")
    (is (= (sen2/date->census-date (java.time.LocalDate/parse "2023-01-18")
                                   census-dates)
           (java.time.LocalDate/parse "2023-01-19"))
        "Date day before 2023 census date did not return 2023 census date."))

  (testing "A date after the last census date should return nil."
    (is (nil? (sen2/date->census-date (java.time.LocalDate/parse "2024-01-19")
                                      census-dates))
        "Date after last census-date did not return nil.")))

(comment
  (clojure.test/run-tests)
  )
