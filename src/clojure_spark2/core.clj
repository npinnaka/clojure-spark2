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

(def departments ["IT" "FINANCE" "SALES" "MARKETING" "OPERATIONS" "HR"])

(defn prepare-coverage-fields[key-prefix v]
  (merge {} {(str key-prefix "_department") v}
         {(str key-prefix "_emp_count") v}
         {(str key-prefix "_expenses") v}))


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

(def out-map (map transform-map final-map))

(defn abc[item]
  (into (sorted-map) (filter #(= item (get % "id")) out-map)))

(defn xys[unique-keys out-map]
  (map abc unique-keys))

(defn xc [in-map]
  (merge (into {} (map #(prepare-coverage-fields % nil) departments)) in-map)
  )

(defn final-output-vector[in-vec-map]
  (map xc in-vec-map)
  )

(->
 (distinct (map #(get % "id") out-map))
 (xys out-map)
 (final-output-vector))

;(defn read-json[sparkSession file-name]
;  (println (type sparkSession))
;  (sql/json-file sparkSession file-name))
;
;(.show (read-json((build-spark-session "abc"), "resources/cartypes.json")))

;({"FINANCE_department"    nil,
;  "SALES_emp_count"       20,
;  "MARKETING_expenses"    33000,
;  "HR_emp_count"          nil,
;  "MARKETING_emp_count"   35,
;  "SALES_department"      "SALES",
;  "FINANCE_expenses"      nil,
;  "IT_expenses"           45000,
;  "OPERATIONS_expenses"   nil,
;  "id"                    1,
;  "OPERATIONS_department" nil,
;  "FINANCE_emp_count"     nil,
;  "OPERATIONS_emp_count"  nil,
;  "MARKETING_department"  "MARKETING",
;  "HR_department"         nil,
;  "IT_emp_count"          250,
;  "SALES_expenses"        32000,
;  "HR_expenses"           nil,
;  "IT_department"         "IT"}
;  {"FINANCE_department"    "FINANCE",
;   "SALES_emp_count"       nil,
;   "MARKETING_expenses"    nil,
;   "HR_emp_count"          32,
;   "MARKETING_emp_count"   nil,
;   "SALES_department"      nil,
;   "FINANCE_expenses"      19000,
;   "IT_expenses"           nil,
;   "OPERATIONS_expenses"   24000,
;   "id"                    2,
;   "OPERATIONS_department" "OPERATIONS",
;   "FINANCE_emp_count"     5,
;   "OPERATIONS_emp_count"  300,
;   "MARKETING_department"  nil,
;   "HR_department"         "HR",
;   "IT_emp_count"          nil,
;   "SALES_expenses"        nil,
;   "HR_expenses"           17000,
;   "IT_department"         nil})