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
                          :state aggr})
                       children)))
        m))

(defn project
  "Applies patches from an event stream to all aggregates from `start` onwards.
   Each returned item provides the state of all affected aggregates at a point
   in time. The initial state is either created with `loadf` or taken from the
   very first replayed event. Returns a transducer if `coll` is not provided.
   
   **Example**
   ```clojure
   (let [loadf (constantly {:a 1}) ; initial state of aggregate [:thing 1]
         event-stream  [{:seq-no 3
                         :source ::test
                         :timestamp (java.time.Instant/now)
                         :action {:reason ::assoc-b
                                  :actor [:test 1]
                                  :aggregate [:thing 1]
                                  :patch {:assoc {:b 2}}}}
                        {:seq-no 4
                         :source ::test
                         :timestamp (java.time.Instant/now)
                         :action {:reason ::assoc-c
                                  :actor [:test 1]
                                  :aggregate [:thing 1]
                                  :patch {:assoc {:c 3}}}}]]
     (project 3 loadf event-stream))
     ; => ({:seq-no 3
     ;      :aggregates [{:id [:thing 1] :state {:a 1 :b 2}}]}
     ;     {:seq-no 4
     ;      :aggregates [{:id [:thing 1] :state {:a 1 :b 2 :c 3}}]})
   ```"
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
                                           :state patched}]}
                            {:seq-no seq-no
                             :aggregates (aggr-map->vec @m)})]
                (vreset! replayed true)
                (rf result value)))))))))
  ([start loadf coll]
   (sequence (project start loadf) coll)))

;;; merging

(defn- merge-aggregates
  [src dst]
  (let [pred (complement (set (map :id dst)))]
    (into dst (filter #(pred (:id %)) src))))

(defn merge-projections
  "Combines `projections` into a single projection and merges all aggregates
   contained within.
   
   **Example**
   ```clojure
   (let [projections [{:seq-no 1
                       :aggregates [{:id [:thing 1] :state {:a 1}}
                                    {:id [:thing 2] :state {:a 1}}]}
                      {:seq-no 2
                       :aggregates [{:id [:thing 2] :state {:a 2}}
                                    {:id [:thing 3] :state {:a 3}}]}]]
     (merge-projections projections))
   ; => {:seq-no 2
   ;     :aggregates [{:id [:thing 2] :state {:a 2}}
   ;                  {:id [:thing 3] :state {:a 3}}
   ;                  {:id [:thing 1], :state {:a 1}}]}
   ```"
  [projections]
  (reduce
   (fn [result {:keys [seq-no aggregates]}]
     (-> result
         (assoc :seq-no seq-no)
         (update-in [:aggregates] merge-aggregates aggregates)))
   {}
   projections))