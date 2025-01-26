(ns coherence.projection
  (:require [clojure.spec.alpha :as s]
            [coherence.specs]))

;;; patching

(defn ->path
  [x]
  (cond-> x
    (keyword? x) vector))

(defn- assoc-many
  [m kvs]
  (reduce-kv
   (fn [result k v]
     (assoc-in result (->path k) v))
   m
   kvs))

(defn- dissoc-many
  [m ks]
  (reduce
   (fn [result ks']
     (let [path (->path ks')]
       (cond
         (= 1 (count path)) (apply dissoc result path)
         (identical? ::not-found (get-in result path ::not-found)) result
         :else (update-in result (pop path) dissoc (peek path)))))
   m
   ks))

(defn apply-patch
  "Applies `patch` to `m`."
  [m patch]
  {:pre [(s/valid? :coherence.specs/patch patch)]}
  (-> m
      (assoc-many (:assoc patch))
      (dissoc-many (:dissoc patch))))