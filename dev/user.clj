(ns user
  (:require [nextjournal.clerk :as clerk]))

(clerk/serve! {:browse? true :port 1631})
