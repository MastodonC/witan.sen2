(ns witan.sen2.return.person-level.blade-export.csv-census
  "Extract raw census of plans & placements open on census dates from SEN2 person level return COLLECT Blade Export CSV files"
  (:require [tablecloth.api :as tc]
            [witan.sen2 :as sen2]
            [witan.sen2.return.person-level.blade-export.csv :as sen2-blade-csv]
            ))



;;; # Utility functions
(defn age-at-start-of-school-year
  "Age on 31st August prior to `date` for child with date of birth `dob`.

  `dob` & `date` should be java.time.LocalDate objects

  Age at the start of the school/academic year is the age on 31st August
  prior to the school/academic year, per:

  - The [gov.uk website](https://www.gov.uk/schools-admissions/school-starting-age),
  which (as of 23-NOV-2022) states:
  \"Most children start school full-time in the September after their
  fourth birthday. This means they’ll turn 5 during their first school
  year. For example, if your child’s fourth birthday is between 1
  September 2021 and 31 August 2022 they will usually start school in
  September 2022.\".

  - The [2022 SEN2 guide](https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/1013751/SEN2_2022_Guide.pdf),
  which states in section 2, part 1, item 1.1 that: \"age breakdown
  refers to age as at 31 August 2021\", implying (since this is for
  the January 2022 SEN2 return) that age for SEN2 breakdowns is as of
  31-AUG prior to starting the school year.
  "
  [date-of-birth date]
  (- (- (.getYear date)          (if (< (.getMonthValue date)          9) 1 0))
     (- (.getYear date-of-birth) (if (< (.getMonthValue date-of-birth) 9) 1 0))
     1))

(defn- inclusive-range
  "Returns a lazy seq of nums from `start` (inclusive) to `end` (inclusive), by step 1"
  [start end]
  (range start (inc end)))

(def ncys
  "Set of national curriculum years (NCY), coded numerically, with
  reception as NCY 0 and earlier NCYs as negative integers."
  (into (sorted-set) (inclusive-range -4 20)))

(def age-at-start-of-school-year->ncy
  "Maps age in whole number of years on 31st August prior to starting
  the school/academic year to the corresponding national curriculum year (NCY).

  Early years NCYs are coded numerically, with reception as year 0 and
  earlier NCYs as negative integers.

  Ages and NCYs for children aged 4 to 15 at the start of school/academic
  year (reception to NCY 11) are per https://www.gov.uk/national-curriculum.
  Extension to ages 0-3 (NCYs -4 to -1) and ages 16-24 (NCYs 12 to 20),
  is by linear extrapolation, such that the offset between NCY and age
  at the beginning of the school/academic year is -4.

  The maximum age for SEND is 25, but per the relevant legislation
  \"a local authority may continue to maintain an EHC plan for a young
  person until the end of the academic year during which the young
  person attains the age of 25\", such that they will have been 24 at
  the start of that school/academic year, hence the maximum age in
  this map is 24 (NCY 20).
  "
  (apply sorted-map (interleave (map #(+ % 4) ncys) ncys)))




;;; # Census dates
(defn census-years->census-dates-ds
  "Return dataset of census-dates given vector of `census-years`."
  [census-years]
  (-> (tc/dataset [[:census-year census-years]])
      #_(tc/convert-types {:census-year :int16})
      (tc/map-columns :census-date [:census-year] sen2/census-year->date)
      (tc/set-dataset-name "census-dates")))

(def census-dates-col-name->label
  "Column labels for `census-dates` columns `:census-year` and `:census-date`."
  {:census-year "SEN2 census year"
   :census-date "SEN2 census date"})



;;; # Person information
(defn person-on-census-dates
  "Selected columns from `person` table with record for each of `census-dates-ds`,
   with age at the start of school year containing the `:census-date` and corresponding nominal NCY."
  [sen2-blade-csv-ds-map {:keys [census-years
                                 census-dates-ds]}]
  (let [census-dates-ds (or census-dates-ds (census-years->census-dates-ds census-years))]
    (-> (:person sen2-blade-csv-ds-map)
        (tc/select-columns [:sen2-table-id :person-table-id :person-order-seq-column :upn :unique-learner-number :person-birth-date])
        (tc/cross-join census-dates-ds)
        (tc/map-columns :age-at-start-of-school-year [:person-birth-date :census-date] age-at-start-of-school-year)
        (tc/map-columns :nominal-ncy [:age-at-start-of-school-year] age-at-start-of-school-year->ncy)
        (tc/set-dataset-name "Person ages and nominal NCYs on census dates"))))

(defn person-on-census-dates-col-name->label
  "Column labels for display."
  [person-on-census-dates]
  (-> (merge sen2-blade-csv/person-col-name->label
             census-dates-col-name->label
             {:age-at-start-of-school-year "Age at start of school year"
              :nominal-ncy                 "Nominal NCY for age"})
      (select-keys (tc/column-names person-on-census-dates))))

