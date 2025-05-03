(ns coherence.core-tests
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest testing is]]
            [clojure.test.check.generators :as gen]
            [coherence.core :refer :all]
            [spy.assert :as assert]
            [spy.protocol :as p])
  (:import coherence.core.WriteConflictException))

;;; predicates

(deftest test-action?
  (testing "action is action"
    (is (action? (gen/generate (s/gen :coherence.specs.event/action)))))
  (testing "effect is no action"
    (is (not (action? (gen/generate (s/gen :coherence.specs.event/effect))))))
  (testing "arbitary data is no action"
    (is (not (action? (gen/generate gen/any))))))

(deftest test-effect?
  (testing "effect is effect"
    (is (effect? (gen/generate (s/gen :coherence.specs.event/effect)))))
  (testing "action is no effect"
    (is (not (effect? (gen/generate (s/gen :coherence.specs.event/action))))))
  (testing "arbitary data is no effect"
    (is (not (effect? (gen/generate gen/any))))))

(deftest test-store?
  (testing "Store is Store"
    (is (store? (reify Store))))
  (testing "arbitary data is no Store"
    (is (not (store? (gen/generate gen/any))))))

(defprotocol ^:private Closable
  :extend-via-metadata true
  (close [this]))

;;; write events

(defmacro ^:private writer
  [& body]
  `(p/mock
    Writer
    ~@body
    Closable
    (close [_])))

(deftype WriterStore [w]
  Store
  (open-write [_] w))

(defn- action-gen
  []
  (gen/fmap #(dissoc % :seq-no)
            (s/gen :coherence.specs.event/action)))

(deftest test-rebase!-one
  (testing "seq-no greater than next available seq-no"
    (let [action (gen/generate (action-gen))
          writer (writer
                  (next-seq-no [_] 1)
                  (rollback! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [{result :result
             next-seq-no' :next-seq-no} (rebase! store 2 [action])]
        (assert/called-once? (:next-seq-no spy))
        (assert/called-once? (:rollback! spy))
        (assert/called-once? (:close spy))
        (is (#{:invalid-seq-no} result))
        (is (= 1 next-seq-no')))))

  (testing "seq-no equals next available seq-no"
    (let [action (gen/generate (action-gen))
          writer (writer
                  (next-seq-no [_] 1)
                  (next-conflict [_ _ [_ _] _])
                  (append! [_ _])
                  (commit! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [{result :result
             [ev] :events} (rebase! store 1 [action])]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-once-with? (:next-conflict spy) writer 1 (get-in action [:action :aggregate]) [])
        (assert/called-once-with? (:append! spy) writer (assoc action :seq-no 1))
        (assert/called-once? (:commit! spy))
        (assert/called-once? (:close spy))
        (is (#{:ok} result))
        (is (= (assoc action :seq-no 1) ev)))))

  (testing "seq-no equals next available seq-no, write conflict"
    (let [action (gen/generate (action-gen))
          writer (writer
                  (next-seq-no [_] 1)
                  (next-conflict [_ _ [_ _] _])
                  (append! [_ _] (throw (->WriteConflictException)))
                  (rollback! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let  [{result :result
              ex :exception} (rebase! store 1 [action])]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-once-with? (:next-conflict spy) writer 1 (get-in action [:action :aggregate]) [])
        (assert/called-once-with? (:append! spy) writer (assoc action :seq-no 1))
        (assert/called-once? (:rollback! spy))
        (assert/called-once? (:close spy))
        (is (#{:write-conflict} result))
        (is (instance? WriteConflictException ex)))))

  (testing "seq-no less than next available seq-no, no conflict found"
    (let [action (gen/generate (action-gen))
          writer (writer
                  (next-seq-no [_] 2)
                  (next-conflict [_ _ [_ _] _])
                  (append! [_ _])
                  (commit! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [{result :result
             [ev] :events} (rebase! store 1 [action])]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-once-with? (:next-conflict spy) writer 1 (get-in ev [:action :aggregate]) [])
        (assert/called-once-with? (:append! spy) writer (assoc action :seq-no 2))
        (assert/called-once? (:commit! spy))
        (assert/called-once? (:close spy))
        (is (#{:ok} result))
        (is (= (assoc action :seq-no 2) ev)))))

  (testing "seq-no less than next available seq-no, no conflict found, write conflict"
    (let [action (gen/generate (action-gen))
          writer (writer
                  (next-seq-no [_] 2)
                  (next-conflict [_ _ [_ _] _])
                  (append! [_ _] (throw (->WriteConflictException)))
                  (rollback! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [{result :result
             ex :exception} (rebase! store 1 [action])]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-once-with? (:next-conflict spy) writer 1 (get-in action [:action :aggregate]) [])
        (assert/called-once-with? (:append! spy) writer (assoc action :seq-no 2))
        (assert/called-once? (:rollback! spy))
        (assert/called-once? (:close spy))
        (is (#{:write-conflict} result))
        (is (instance? WriteConflictException ex)))))

  (testing "seq-no less than next available seq-no, conflict found"
    (let [action (gen/generate (action-gen))
          conflict (-> (gen/generate (action-gen))
                       (assoc :seq-no 2)
                       (assoc-in [:action :aggregate] (get-in action [:action :aggregate])))
          writer (writer
                  (next-seq-no [_] 2)
                  (next-conflict [_ _ [_ _] _] conflict)
                  (rollback! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [{result :result
             events :events
             {:keys [:ours :theirs]} :conflict} (rebase! store 1 [action])]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-once-with? (:next-conflict spy) writer 1 (get-in action [:action :aggregate]) [])
        (assert/called-once? (:rollback! spy))
        (assert/called-once? (:close spy))
        (is (#{:conflict} result))
        (is (empty? events))
        (is (= (assoc action :seq-no 2) ours))
        (is (= theirs conflict)))))

  (testing "seq-no less than next available seq-no, resolve conflict"
    (let [action (gen/generate (action-gen))
          conflict (-> (gen/generate (action-gen))
                       (assoc :seq-no 2)
                       (assoc-in
                        [:action :aggregate]
                        (get-in action [:action :aggregate])))
          writer (writer
                  (next-seq-no [_] 2)
                  (next-conflict [_ _ [_ _] resolved]
                                 (when (empty? resolved)
                                   conflict))
                  (append! [_ _])
                  (rollback! [_])
                  (commit! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      ;; fail to append due to conflict
      (let [{result :result
             events :events
             {:keys [:ours :theirs]} :conflict}  (rebase! store 1 [action])]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-once-with? (:next-conflict spy) writer 1 (get-in action [:action :aggregate]) [])
        (assert/called-once? (:rollback! spy))
        (assert/called-once? (:close spy))
        (is (#{:conflict} result))
        (is (empty? events))
        (is (= (assoc action :seq-no 2) ours))
        (is (= theirs conflict)))
      ;; mark conflict as resolved & append
      (let [{:keys [:result :events]} (rebase! store 2 [action] :resolved [1])]
        (assert/called-n-times? (:next-seq-no (p/spies writer)) 2)
        (assert/called-n-times? (:next-conflict spy) 2)
        (assert/called-with? (:next-conflict spy) writer 2 (get-in action [:action :aggregate]) [1])
        (assert/called-once-with? (:append! spy) writer (assoc action :seq-no 2))
        (assert/called-once? (:commit! spy))
        (assert/called-n-times? (:close spy) 2)
        (is (#{:ok} result))
        (is (= [(assoc action :seq-no 2)] events))))))

(deftest test-rebase!-many
  (testing "append two events"
    (let [a (gen/generate (action-gen))
          b (gen/generate (action-gen))
          seq-no (volatile! 0)
          writer (writer
                  (next-seq-no [_] (vswap! seq-no inc))
                  (next-conflict [_ _ [_ _] _])
                  (append! [_ _])
                  (commit! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [{result :result
             [a' b'] :events} (rebase! store 1 [a b])]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-n-times? (:next-conflict spy) 2)
        (assert/called-with? (:next-conflict spy) writer 1 (get-in a [:action :aggregate]) [])
        (assert/called-with? (:next-conflict spy) writer 1 (get-in b [:action :aggregate]) [])
        (assert/called-n-times? (:append! spy) 2)
        (assert/called-with? (:append! spy) writer (assoc a :seq-no 1))
        (assert/called-with? (:append! spy) writer (assoc b :seq-no 2))
        (assert/called-once? (:commit! (p/spies writer)))
        (assert/called-once? (:close (p/spies writer)))
        (is (#{:ok} result))
        (is (= (assoc a :seq-no 1) a'))
        (is (= (assoc b :seq-no 2) b')))))

  (testing "conflict terminates operation"
    (let [a (gen/generate (action-gen))
          b (gen/generate (action-gen))
          c (gen/generate (action-gen))
          conflict (-> (gen/generate (action-gen))
                       (assoc :seq-no 3)
                       (assoc-in
                        [:action :aggregate]
                        (get-in c [:action :aggregate])))
          seq-no (volatile! 0)
          writer (writer
                  (next-seq-no [_] (vswap! seq-no inc))
                  (next-conflict [_ _ aggregate _]
                                 (when (= (get-in c [:action :aggregate]) aggregate)
                                   conflict))
                  (append! [_ _])
                  (rollback! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [{result :result
             [a' b'] :events
             {:keys [:ours :theirs]} :conflict} (rebase! store 1 [a b c])]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-n-times? (:next-conflict spy) 3)
        (assert/called-with? (:next-conflict spy) writer 1 (get-in a [:action :aggregate]) [])
        (assert/called-with? (:next-conflict spy) writer 1 (get-in b [:action :aggregate]) [])
        (assert/called-with? (:next-conflict spy) writer 1 (get-in c [:action :aggregate]) [])
        (assert/not-called? (:append! spy))
        (assert/called-once? (:rollback! (p/spies writer)))
        (assert/called-once? (:close (p/spies writer)))
        (is (#{:conflict} result))
        (is (= (assoc a :seq-no 1) a'))
        (is (= (assoc b :seq-no 2) b'))
        (is (= (assoc c :seq-no 3) ours))
        (is (= theirs conflict)))))

  (testing "write conflict terminates operation"
    (let [a (gen/generate (action-gen))
          b (gen/generate (action-gen))
          c (gen/generate (action-gen))
          seq-no (volatile! 0)
          writer (writer
                  (next-seq-no [_] (vswap! seq-no inc))
                  (next-conflict [_ _ [_ _] _])
                  (append! [_ ev]
                           (when (= 2 (:seq-no ev))
                             (throw (->WriteConflictException))))
                  (rollback! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [{result :result
             ex :exception} (rebase! store 1 [a b c])]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-n-times? (:next-conflict spy) 3)
        (assert/called-with? (:next-conflict spy) writer 1 (get-in a [:action :aggregate]) [])
        (assert/called-with? (:next-conflict spy) writer 1 (get-in b [:action :aggregate]) [])
        (assert/called-with? (:next-conflict spy) writer 1 (get-in c [:action :aggregate]) [])
        (assert/called-n-times? (:append! spy) 2)
        (assert/called-with? (:append! spy) writer (assoc a :seq-no 1))
        (assert/called-with? (:append! spy) writer (assoc b :seq-no 2))
        (assert/called-once? (:rollback! (p/spies writer)))
        (assert/called-once? (:close (p/spies writer)))
        (is (#{:write-conflict} result))
        (is (instance? WriteConflictException ex))))))

(deftest test-rebase!-dry
  (testing "rollback instead of commit"
    (let [action (gen/generate (action-gen))
          writer (writer
                  (next-seq-no [_] 1)
                  (next-conflict [_ _ [_ _] _])
                  (append! [_ _])
                  (rollback! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [{result :result
             [ev] :events} (rebase! store 1 [action] :dry true)]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-once-with? (:next-conflict spy) writer 1 (get-in action [:action :aggregate]) [])
        (assert/called-once-with? (:append! spy) writer (assoc action :seq-no 1))
        (assert/called-once? (:rollback! spy))
        (assert/called-once? (:close spy))
        (is (#{:ok} result))
        (is (= (assoc action :seq-no 1) ev))))))

;;; read events

(defn- reader
  [events]
  (p/mock
   Reader
   (read-events
    [_ xform f init offset]
    (let [xf (comp (filter (fn [{:keys [seq-no]}]
                             (>= seq-no offset)))
                   xform)]
      (transduce xf f init events)))

   (filter-aggregates
    [_ xform f init offset identities]
    (let [xf (comp (filter (fn [{seq-no :seq-no {[kind id] :aggregate} :action}]
                             (and (>= seq-no offset)
                                  (some (fn [[kind' id']]
                                          (and (= kind kind')
                                               (= id id')))
                                        identities))))
                   xform)]
      (transduce xf f init events)))

   (filter-aggregate-kinds
    [_ xform f init offset kinds]
    (let [xf (comp (filter (fn [{seq-no :seq-no {[kind _] :aggregate} :action}]
                             (and (>= seq-no offset)
                                  (kind (set kinds)))))
                   xform)]
      (transduce xf f init events)))

   (max-seq-no
    [_]
    (-> events last :seq-no))

   (query-conflicts
    [_ offset aggregate]
    (filter (fn [{seq-no :seq-no :as ev}]
              (and (>= seq-no offset)
                   (= (-> ev :action :aggregate) aggregate)))
            events))

   Closable
   (close [_])))

(deftype ReaderStore [r]
  Store
  (open-read [_] r))

(defn- ->action
  ([seq-no aggregate]
   (-> (gen/generate (action-gen))
       (assoc :seq-no seq-no)
       (assoc-in [:action :aggregate] aggregate)))
  ([seq-no]
   (->action seq-no (gen/generate (s/gen :coherence.specs/identity)))))

(deftest test-seq-no
  (testing "seq-no"
    (let [events (for [idx (range 1 5)] (->action idx))
          reader (reader events)
          store (->ReaderStore reader)
          spy (p/spies reader)
          seq-no (seq-no store)]
      (assert/called-once? (:max-seq-no spy))
      (is (= 4 seq-no)))))

(deftest test-conflicts
  (let [[agg1 agg2 agg3] (gen/sample (s/gen :coherence.specs/identity) 3)
        events [(->action 1 agg1)
                (->action 2 agg2)
                (->action 3 agg1)
                (->action 4 agg3)
                (->action 5 agg1)
                (->action 6 agg2)]]
    (testing "find conflicts"
      (let [reader (reader events)
            store (->ReaderStore reader)
            spy (p/spies reader)]
        (let [result (conflicts store 2 agg1)]
          (assert/called-once-with? (:query-conflicts spy) reader 2 agg1)
          (is (= (mapv events [2 4]) result)))))

    (testing "no conflicts"
      (let [reader (reader events)
            store (->ReaderStore reader)
            spy (p/spies reader)]
        (let [result (conflicts store 5 agg3)]
          (assert/called-once-with? (:query-conflicts spy) reader 5 agg3)
          (is (empty? result)))))))

(deftest test-stream-and-filter-events
  (let [[agg1 agg2 agg3] (gen/sample (s/gen :coherence.specs/identity) 3)
        events [(->action 1 agg1)
                (->action 2 agg2)
                (->action 3 agg2)
                (->action 4 agg1)
                (->action 5 agg3)
                (->action 6 agg1)]]
    (testing "stream-events without offset"
      (let [reader (reader events)
            store (->ReaderStore reader)
            spy (p/spies reader)]
        (let [result (stream-events (map identity)
                                    conj
                                    []
                                    store)]
          (assert/called-once? (:read-events spy))
          (is (= events result)))))

    (testing "stream-events with offset"
      (let [reader (reader events)
            store (->ReaderStore reader)
            spy (p/spies reader)]
        (let [result (stream-events (map identity)
                                    conj
                                    []
                                    store
                                    :offset 2)]
          (assert/called-once? (:read-events spy))
          (is (= (drop 1 events) result)))))

    (testing "filter-events-by-aggregates"
      (let [reader (reader events)
            store (->ReaderStore reader)
            spy (p/spies reader)]
        (let [result (filter-events-by-aggregates (map identity)
                                                  conj
                                                  []
                                                  store
                                                  3
                                                  [agg2 agg3])]
          (assert/called-once? (:filter-aggregates spy))
          (is (= (mapv events [2 4]) result)))))

    (testing "filter-events-by-aggregate-kinds"
      (let [reader (reader events)
            store (->ReaderStore reader)
            spy (p/spies reader)]
        (let [result (filter-events-by-aggregate-kinds (map identity)
                                                       conj
                                                       []
                                                       store
                                                       2
                                                       [(first agg2)])]
          (assert/called-once? (:filter-aggregate-kinds spy))
          (is (= (mapv events [1 2]) result)))))))