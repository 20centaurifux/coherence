(ns coherence.core
  (:refer-clojure :exclude [transduce])
  (:require
   [clojure.spec.alpha :as s]
   [coherence.specs]
   [com.rpl.defexception :refer [defexception]]
   [failjure.core :as f]))

(defn action?
  "Tests if `x` is an action."
  [x]
  (s/valid? :coherence.specs.event/action x))

(defn effect?
  "Tests if `x` is an effect."
  [x]
  (s/valid? :coherence.specs.event/effect x))

(defprotocol Store
  (open-write [store])
  (open-read [store]))

(defn store?
  "Tests if `x` implements `Store` protocol."
  [x]
  (satisfies? Store x))

(defprotocol Closed
  (closed? [x]))

;;; write events

(defexception WriteConflictException)

(defprotocol Writer
  (commit! [writer])
  (rollback! [writer])
  (next-seq-no [writer])
  (append! [writer ev])
  (next-conflict [writer offset [aggregate-kind aggregate-id] resolved]))

(defrecord ^:private Failure [message result]
  f/HasFailed
  (failed? [_] true)
  (message [self] (:message self)))

(defn- start-seq-no
  [w base]
  (let [start (next-seq-no w)]
    (if (> base start)
      (->Failure "Invalid sequence number."
                 {:result :invalid-seq-no
                  :next-seq-no start})
      start)))

(defn- history->events
  [history start]
  (vec (map-indexed (fn [idx ev]
                      (assoc ev :seq-no (+ start idx)))
                    history)))

(defn- check-conflicts
  [w base resolved events]
  (reduce (fn [result {{aggregate :aggregate} :action :as ev}]
            (if-let [conflict (and aggregate
                                   (next-conflict w base aggregate resolved))]
              (reduced (->Failure "Conflict found."
                                  {:result :conflict
                                   :events result
                                   :conflict {:ours ev
                                              :theirs conflict}}))
              (conj result ev)))
          []
          events))

(defn- write-events!
  [w events]
  (try
    (doseq [ev events] (append! w ev))
    {:result :ok
     :events events}
    (catch WriteConflictException e
      (->Failure "Write conflict."
                 {:result :write-conflict
                  :exception e}))))

(s/def ::new-action (s/keys :req-un [:coherence.specs/timestamp
                                     :coherence.specs/source
                                     :coherence.specs/action]
                            :opt-un [:coherence.specs/triggers]))

(s/def ::new-effect (s/keys :req-un [:coherence.specs/timestamp
                                     :coherence.specs/source
                                     :coherence.specs/effect]))

(s/def ::new-event (s/or :action ::new-action
                         :effect ::new-effect))

(defn rebase!
  "Appends `history` to `store` on top of `base` inside a transaction. Sequence
   numbers in `resolved` are treated as resolved conflicts. If `dry` is true
   perform a trial run with no changes made. Returns a map containing a result
   code and details depending on context.

   - success
       - `:result`: `:ok`
       - `:events`: vector of written events
   - invalid base
       - `:result`: `:invalid-seq-no`
       - `:next-seq-no`: next available sequence number
   - conflict
       - `:result`: `:conflict`
       - `:events`: vector of accepted events
       - `:conflict`: map containing affected events from history (`:ours`)
         and store (`:theirs`)
   - write conflict in underlying store implementation
      - `:result`: `:write-conflict`
      - `:exception`: catched exception"
  [store base history & {:keys [resolved dry] :or {resolved [] dry false}}]
  {:pre [(store? store)
         (s/valid? :coherence.specs/seq-no base)
         (s/valid? (s/coll-of ::new-event :min-count 1) history)
         (s/valid? (s/coll-of :coherence.specs/seq-no :distinct true) resolved)]}
  (with-open [w (open-write store)]
    (let [result (f/ok->> (start-seq-no w base)
                   (history->events history)
                   (check-conflicts w base resolved)
                   (write-events! w))]
      (if (and (f/ok? result) (not dry))
        (commit! w)
        (rollback! w))
      (cond-> result
        (f/failed? result) :result))))

;;; read events

(defprotocol Reader
  (stream-events [reader xform f init offset])
  (max-seq-no [reader]))

(defn transduce
  "Reduces an event stream with a transformation (`xform` `f`). Skips `offset`
   events."
  [xform f init store & {:keys [offset] :or {offset 0}}]
  {:pre [(store? store)
         (or (zero? offset)
             (s/valid? :coherence.specs/seq-no offset))]}
  (when (>= offset 0)
    (with-open [r (open-read store)]
      (stream-events r xform f init offset))))

(defn current-seq-no
  "Returns latest sequence number."
  [store]
  {:pre [(store? store)]}
  (with-open [r (open-read store)]
    (max-seq-no r)))