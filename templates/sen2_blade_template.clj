(ns sen2-blade-template
  "SEN2 Blade read from Excel submission template."
  (:require [witan.sen2.return.person-level.blade.template :as sen2-blade-template]))



;;; # Parameters
;;; ## SEN2 Excel submission template file
(def template-filepath
  "SEN2 Person Level Census Template XLSM file containing SEN2 submission datasets."
  "./data/example-sen2-submission-template-2024-v1-2.xlsm")



;;; # Read SEN2 Blade from Excel submission template file
(def ds-map
  "Map of SEN2 Blade datasets."
  (delay (sen2-blade-template/template-file->ds-map template-filepath)))


;;; ## Bring in defs required for EDA/documentation
;; Not required if not doing a sen2-blade-eda.
(def module-titles
  sen2-blade-template/module-titles)

(def module-col-name->label
  sen2-blade-template/module-col-name->label)

(def module-src-col-name->col-name
  sen2-blade-template/module-src-col-name->col-name)

