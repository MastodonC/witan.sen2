^{:nextjournal.clerk/toc true}
(ns client-validation-deck
  {:nextjournal.clerk/visibility {:code   :hide
                                  :result :hide}}
  (:require [nextjournal.clerk :as clerk]
            [nextjournal.clerk.viewer :as v]
            [nextjournal.clerk-slideshow :as slideshow]
            [tablecloth.api :as tc]
            [witan.send.adroddiad.clerk.html :as chtml]
            [witan.send.adroddiad.slides :as sl]
            [witan.send.hobbiton.wp-x-y.process-202n-blade :as blade]
            ;; replace with namespace containing output of `witan.sen2.template/process-raw-sen2`
            [witan.send.hobbiton.wp-x-y.process-202n-1-blade :as old-blade]
            ;; replace with namespace containing output of `witan.sen2.template/process-raw-sen2`
            ))

(
;;; Template input section
 )

(def out-dir "")
(def sen2-year "")
(def sen2-year-1 (- sen2-year 1))
(def presentation-date "")
(def client-name "")
(def work-package "")
(def sen2-data "output of `witan.sen2.template/process-raw-sen2`")
(def old-sen2-data "output of `witan.sen2.template/process-raw-sen2`")


(
;;; # Clerk notebook helpers

 )

(defn transform-child-viewers [viewer & update-args]
  (update viewer :transform-fn (partial comp #(apply update % :nextjournal/viewers v/update-viewers update-args))))

(def table-viewer-no-pagination
  (transform-child-viewers v/table-viewer {:page-size #(dissoc % :page-size)}))

(defn clerk-table-no-pagination
  ([xs] (clerk-table-no-pagination {} xs))
  ([viewer-opts xs] (clerk/with-viewer table-viewer-no-pagination viewer-opts xs)))

(clerk/add-viewers! [slideshow/viewer])

(def presentation-title (str sen2-year "SEN2 Data Validation"))

(defn mc-logo []
  (clerk/html
   {::clerk/width :full}
   [:div.h-full.max-h-full.bottom-0.-right-12.absolute [:img {:src sl/mc-logo}]]))

(
;;; Notebook
 )

{:nextjournal.clerk/visibility {:result :show}}

(clerk/html
 ;;; Title Page
 {::clerk/width :full}
 [:div.max-w-screen-2xl.font-sans
  [:p.text-6xl.font-extrabold presentation-title]
  [:p.text-3xl.font-bold work-package]
  [:p.text-3xl.italic presentation-date]
  [:p.text-5xl.font-bold.-mb-8.-mt-2 (format "For %s" client-name)]
  [:p.text-4xl.font-bold.italic "Presented by Mastodon C"]
  [:p.text-1xl "Use arrow keys to navigate and ESC to see an overview."]])

(mc-logo)

;; ---
;; # EHCP count per calendar year

(let [count-sen2-year-1 (-> sen2-data
                            :census-raw
                            (tc/select-rows #(= sen2-year-1 (:census-year %)))
                            (tc/row-count))
      count-sen2-year (-> sen2-data
                          :census-raw
                          (tc/select-rows #(= sen2-year (:census-year %)))
                          (tc/row-count))]
  (clerk/html
   [:div.max-w-screen-2xl.font-sans
    [:ul.list-disc
     [:li.text-3xl.mb-4.mt-4 (str "There were "
                                  count-sen2-year-1
                                  " EHCPs reported for calendar year "
                                  sen2-year-1
                                  " in the " sen2-year " return.")]
     [:li.text-3xl.mb-4.mt-4 (str "There were "
                                  count-sen2-year
                                  " EHCPs reported for calendar year " sen2-year
                                  " in the " sen2-year " return.")]
     [:li.text-3xl.mb-4.mt-4 (str "This is a difference of "
                                  (abs (- count-sen2-year count-sen2-year-1))
                                  " or "
                                  (int (* (/ (- count-sen2-year count-sen2-year-1)
                                             count-sen2-year-1)
                                          100))
                                  "%.")]]]))

(mc-logo)

;; ---
;; Compare Totals with DfE Caseload

(clerk/table
 {::clerk/width :full}
 (-> sen2-data
     :census-raw
     (sen2-blade-plans-placements/plan-placement-module-summary-with-caseload client-name)
     (tc/update-columns :census-date (partial map #(.format % (java.time.format.DateTimeFormatter/ofPattern
                                                               "dd-MMM-uuuu"
                                                               (java.util.Locale. "en_GB")))))
     (tc/rename-columns sen2-blade-plans-placements/plan-placement-stats-col-name->label)))

(mc-logo)

;; ---
;; # Compare Totals with Previous SEN2 Data

(clerk/html
 [:div.max-w-screen-2xl.font-sans
  [:ul.list-disc
   [:li.text-3xl.mb-4.mt-4 (str "There is a count difference of "
                                (- (-> sen2-data
                                       :census-raw
                                       (tc/select-rows #(= sen2-year-1 (:census-year %)))
                                       (tc/row-count))
                                   (-> old-sen2-data
                                       :census-raw
                                       (tc/select-rows #(= sen2-year-1 (:census-year %)))
                                       (tc/row-count)))
                                " between SEN2 returns")]]])

(mc-logo)

;; ---
;; # Issues

(clerk/table
 (let [header-date-1 (-> sen2-data
                         :census-dates
                         (tc/select-rows 0)
                         :census-date
                         first
                         str)
       header-date-2 (-> sen2-data
                         :census-dates
                         (tc/select-rows 1)
                         :census-date
                         first
                         str)]
   (-> sen2-data
       :issues-summary
       (tc/select-rows #(or (< 0 (get % header-date-1))
                            (< 0 (get % header-date-2)))))))

(mc-logo)

{:nextjournal.clerk/visibility {:result :hide}}

(comment

  ;; Output

  (chtml/ns->html out-dir *ns*)

  )
