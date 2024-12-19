(ns coherence.core
  (:refer-clojure :exclude [transduce])
  (:require [clojure.spec.alpha :as s]
            [coherence.specs]
            [com.rpl.defexception :refer [defexception]]))

(defexception WriteConflictException)

(defn action?
  "Tests if x is an action."
  [x]
  (s/valid? :coherence.specs.event/action x))

(defn effect?
  "Tests if x is an effect."
  [x]
  (s/valid? :coherence.specs.event/effect x))

(defprotocol Store
  (open-write [store])
  (open-read [store]))

(defprotocol Closed
  (closed? [x]))

;;; write events

(defprotocol Writer
  (commit! [writer])
  (rollback! [writer])
  (next-seq-no [writer])
  (append! [writer ev])
  (next-conflict [writer offset [aggregate-kind aggregate-id] resolved]))

(defn- write-event!
  [w {seq-no :seq-no {aggregate :aggregate} :action :as ev} resolved]
  (let [seq-no' (next-seq-no w)]
    (if (> seq-no seq-no')
      {:result :invalid-seq-no
       :event ev
       :next-seq-no seq-no'}
      (if-let [conflict (and aggregate
                             (next-conflict w seq-no aggregate resolved))]
        {:result :conflict
         :ours ev
         :theirs conflict}
        (let [ev' (assoc ev :seq-no seq-no')]
          (try
            (append! w ev')
            {:result :ok
             :event ev'}
            (catch WriteConflictException _
              {:result :write-conflict
               :event ev'})))))))

(defn- write-events!
  [w [ev & more] resolved]
  (when ev
    (let [{result-code :result :as result} (write-event! w ev resolved)]
      (if-not (identical? :ok result-code)
        [result]
        (cons result (write-events! w more resolved))))))

(defn append-events!
  "Appends events to store inside a transaction. Stops at the first error.
   Returns the result of each insert operation as result.

   Sequence numbers in resolved are treated as resolved conflicts.

   Success
    {:result :ok
     :event appended event}
   
   Invalid sequence number:
    {:result :invalid-seq-no
     :event affected event
     :next-seq-no next available sequence number}
   
   Conflict:
    {:result :conflict
     :ours affected event
     :theirs found conflict}
   
   Write conflict of underlying store implementation:
    {:result :write-conflict
     :event affected event}"
  [store events & {:keys [resolved] :or {resolved []}}]
  (with-open [w (open-write store)]
    (let [results (write-events! w events resolved)]
      (if (-> results last :result #{:ok})
        (commit! w)
        (rollback! w))
      results)))

;;; read events

(defprotocol Reader
  (stream-events [reader xform f init offset]))

(defn transduce
  "Reduces an event stream with a transformation (xform f)."
  [xform f init store & {:keys [offset] :or {offset 0}}]
  (when (>= offset 0)
    (with-open [r (open-read store)]
      (stream-events r xform f init offset))))