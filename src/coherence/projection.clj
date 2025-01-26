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

;;; projection

(defn- aggr-map->vec
  [m]
  (into []
        (mapcat (fn [[kind children]]
                  (map (fn [[id aggr]]
                         {:id [kind id]
                          :aggregate aggr})
                       children)))
        m))

(defn project
  "Applies patches from an event stream to affected aggregates from `start`
   onwards. The initial state of an aggregate is either created with `loadf` or
   taken from the very first replayed event. Returns a transducer if `coll` is
   not provided."
  ([start loadf]
   (fn [rf]
     (let [m (volatile! {})
           replayed (volatile! false)]
       (fn
         ([] (rf))
         ([result]
          (rf result))
         ([result {seq-no :seq-no {[kind id] :aggregate patch :patch} :action}]
          (if (< seq-no start)
            (do
              (vswap! m update-in [kind id] apply-patch patch)
              result)
            (let [patched (-> (or (get-in @m [kind id])
                                  (loadf kind id))
                              (apply-patch patch))]
              (vswap! m assoc-in [kind id] patched)
              (let [value (if @replayed
                            {:seq-no seq-no
                             :aggregates [{:id [kind id]
                                           :aggregate patched}]}
                            {:seq-no seq-no
                             :aggregates (aggr-map->vec @m)})]
                (vreset! replayed true)
                (rf result value)))))))))
  ([start loadf coll]
   (sequence (project start loadf) coll)))