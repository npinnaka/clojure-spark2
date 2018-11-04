(ns clojure-spark2.core
  (:require [flambo.session :as session]
            [flambo.api :as api]
            [flambo.sql :as sql]
            [flambo.sql-functions :as func]
            [flambo.conf :as conf])

  (:import [org.apache.spark.sql Column]
           [org.apache.spark.sql functions]))

(defn build-spark-session[app-name]
  (->
   (session/session-builder)
   (session/master "local")
   (session/get-or-create)))

(def spark (build-spark-session "app"))

(defn create-map
  "create a map using zipmap"
  [columns values]
  (zipmap (map keyword columns) values))

(defn data-frame->map
  "convert data frame to a clojure map"
  [data-frame]
  (let [columns (sql/columns data-frame)
        data
        (->
         data-frame
         (.toJavaRDD)
         (api/map sql/row->vec)
         api/collect)]
    (map #(create-map columns %) data)))

(defn build-columns
  "prepare a column array"
  [& mycols]
  (into-array Column (map func/col mycols)))

;;cars_df.select($"id", explode($"cars")).select($"id", $"col.department", $"col.emp_count", $"col.expenses")

(def input-ds (sql/json-file spark "resources/cartypes.json"))

;(->>
; (build-columns "id" "cars")
; .select input-ds)
;(.show)

(def final-map
  (->
   input-ds
   (.select (build-columns "id" (functions/explode (functions/col "cars"))))
   (.select (build-columns "id" "col.department" "col.emp_count" "col.expenses"))
   (data-frame->map)))

(def exclude-fileds #{"id"})

(defn x[map-entry key-prefix]
  (if (not (contains? exclude-fileds ((comp name first) map-entry)))
    {(str key-prefix "_" ((comp name first) map-entry)) (second map-entry)}
    {((comp name first) map-entry) (second map-entry)}))

(defn transform-map[in-map]
  (if (contains? in-map :department)
    (into {} (keep #(x % (:department in-map)) in-map))))

(map transform-map final-map)


;
;(defn read-json[sparkSession file-name]
;  (println (type sparkSession))
;  (sql/json-file sparkSession file-name))
;
;(.show (read-json((build-spark-session "abc"), "resources/cartypes.json")))