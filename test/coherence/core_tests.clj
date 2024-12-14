(ns coherence.core-tests
  (:require [clojure.spec.alpha :as s]
            [clojure.test :refer [deftest testing is]]
            [clojure.test.check.generators :as gen]
            [coherence.core :refer :all]
            [spy.assert :as assert]
            [spy.core :as spy]
            [spy.protocol :as p]))

(defn- action-gen
  ([seq-no]
   (gen/fmap #(assoc % :seq-no seq-no)
             (s/gen :coherence.specs.event/action)))
  ([]
   (s/gen :coherence.specs.event/action)))

(def ^:private effect-gen
  (s/gen :coherence.specs.event/effect))

;;; event predicates

(deftest test-action?
  (testing "action is action"
    (is (action? (gen/generate (action-gen)))))
  (testing "effect is no action"
    (is (not (action? (gen/generate effect-gen)))))
  (testing "arbitary data is no action"
    (is (not (action? (gen/generate gen/any))))))

(deftest test-effect?
  (testing "effect is effect"
    (is (effect? (gen/generate effect-gen))))
  (testing "action is no effect"
    (is (not (effect? (gen/generate (action-gen))))))
  (testing "arbitary data is no effect"
    (is (not (effect? (gen/generate gen/any))))))

;;; append events

(defprotocol ^:private Closable
  :extend-via-metadata true
  (close [this]))

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

(deftest test-append!-one
  (testing "seq-no greater than next available seq-no"
    (let [ev (gen/generate (action-gen 2))
          writer (writer
                  (next-seq-no [_] 1)
                  (rollback! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [[{result :result
              ev' :event}] (append-events! store [ev])]
        (assert/called-once? (:next-seq-no spy))
        (assert/called-once? (:rollback! spy))
        (assert/called-once? (:close spy))
        (is (#{:invalid-seq-no} result))
        (is (= ev ev')))))

  (testing "seq-no equals next available seq-no"
    (let [ev (gen/generate (action-gen 1))
          writer (writer
                  (next-seq-no [_] 1)
                  (next-conflict [_ _ [_ _] _])
                  (append! [_ _])
                  (commit! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [[{result :result
              ev' :event}] (append-events! store [ev])]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-once-with? (:next-conflict spy) writer 1 (get-in ev [:action :aggregate]) [])
        (assert/called-once-with? (:append! spy) writer ev)
        (assert/called-once? (:commit! spy))
        (assert/called-once? (:close spy))
        (is (#{:ok} result))
        (is (= ev ev')))))

  (testing "seq-no equals next available seq-no, write conflict"
    (let [ev (gen/generate (action-gen 1))
          writer (writer
                  (next-seq-no [_] 1)
                  (next-conflict [_ _ [_ _] _])
                  (append! [_ _] (throw (->WriteConflictException)))
                  (rollback! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let  [[{result :result
               ev' :event}] (append-events! store [ev])]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-once-with? (:next-conflict spy) writer 1 (get-in ev [:action :aggregate]) [])
        (assert/called-once-with? (:append! spy) writer ev)
        (is (= 'coherence.core.WriteConflictException
               (-> (spy :append!) spy/first-response :thrown :via first :type)))
        (assert/called-once? (:rollback! spy))
        (assert/called-once? (:close spy))
        (is (#{:write-conflict} result))
        (is (= ev ev')))))

  (testing "seq-no less than next available seq-no, no conflict found"
    (let [ev (gen/generate (action-gen 1))
          writer (writer
                  (next-seq-no [_] 2)
                  (next-conflict [_ _ [_ _] _])
                  (append! [_ _])
                  (commit! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [[{result :result
              ev' :event}] (append-events! store [ev])]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-once-with? (:next-conflict spy) writer 1 (get-in ev [:action :aggregate]) [])
        (assert/called-once-with? (:append! spy) writer (assoc ev :seq-no 2))
        (assert/called-once? (:commit! spy))
        (assert/called-once? (:close spy))
        (is (#{:ok} result))
        (is (= (assoc ev :seq-no 2) ev')))))

  (testing "seq-no less than next available seq-no, no conflict found, write conflict"
    (let [ev (gen/generate (action-gen 1))
          writer (writer
                  (next-seq-no [_] 2)
                  (next-conflict [_ _ [_ _] _])
                  (append! [_ _] (throw (->WriteConflictException)))
                  (rollback! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [[{result :result
              ev' :event}] (append-events! store [ev])]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-once-with? (:next-conflict spy) writer 1 (get-in ev [:action :aggregate]) [])
        (assert/called-once-with? (:append! spy) writer (assoc ev :seq-no 2))
        (is (= 'coherence.core.WriteConflictException
               (-> (spy :append!) spy/first-response :thrown :via first :type)))
        (assert/called-once? (:rollback! spy))
        (assert/called-once? (:close spy))
        (is (#{:write-conflict} result))
        (is (= (assoc ev :seq-no 2) ev')))))

  (testing "seq-no less than next available seq-no, conflict found"
    (let [ev (gen/generate (action-gen 1))
          conflict (assoc-in (gen/generate (action-gen 2))
                             [:action :aggregate]
                             (get-in ev [:action :aggregate]))
          writer (writer
                  (next-seq-no [_] 2)
                  (next-conflict [_ _ [_ _] _] conflict)
                  (rollback! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [[{result :result
              ours :ours
              theirs :theirs}] (append-events! store [ev])]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-once-with? (:next-conflict spy) writer 1 (get-in ev [:action :aggregate]) [])
        (assert/called-once? (:rollback! spy))
        (assert/called-once? (:close spy))
        (is (#{:conflict} result))
        (is (= ours ev))
        (is (= theirs conflict)))))

  (testing "seq-no less than next available seq-no, resolve conflict"
    (let [ev (gen/generate (action-gen 1))
          conflict (assoc-in (gen/generate (action-gen 2))
                             [:action :aggregate]
                             (get-in ev [:action :aggregate]))
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
      (let [[{result :result
              ours :ours
              theirs :theirs}] (append-events! store [ev])]
        (assert/called-once? (:next-seq-no (p/spies writer)))
        (assert/called-once-with? (:next-conflict spy) writer 1 (get-in ev [:action :aggregate]) [])
        (assert/called-once? (:rollback! spy))
        (assert/called-once? (:close spy))
        (is (#{:conflict} result))
        (is (= ours ev))
        (is (= theirs conflict)))

      ;; mark conflict as resolved & append
      (let [[{result :result
              ev' :event}] (append-events! store [ev] :resolved [1])]
        (assert/called-n-times? (:next-seq-no (p/spies writer)) 2)
        (assert/called-n-times? (:next-conflict spy) 2)
        (assert/called-with? (:next-conflict spy) writer 1 (get-in ev [:action :aggregate]) [1])
        (assert/called-once-with? (:append! spy) writer (assoc ev :seq-no 2))
        (assert/called-once? (:commit! spy))
        (assert/called-n-times? (:close spy) 2)
        (is (#{:ok} result))
        (is (= (assoc ev :seq-no 2) ev'))))))

(deftest test-append!-many
  (testing "append two events"
    (let [a (gen/generate (action-gen 1))
          b (gen/generate (action-gen 2))
          seq-no (volatile! 0)
          writer (writer
                  (next-seq-no [_] (vswap! seq-no inc))
                  (next-conflict [_ _ [_ _] _])
                  (append! [_ _])
                  (commit! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [[{result :result a' :event}
             {result' :result b' :event}] (append-events! store [a b])]
        (assert/called-n-times? (:next-seq-no (p/spies writer)) 2)
        (assert/called-n-times? (:next-conflict spy) 2)
        (assert/called-with? (:next-conflict spy) writer 1 (get-in a [:action :aggregate]) [])
        (assert/called-with? (:next-conflict spy) writer 2 (get-in b [:action :aggregate]) [])
        (assert/called-n-times? (:append! spy) 2)
        (assert/called-with? (:append! spy) writer a)
        (assert/called-with? (:append! spy) writer b)
        (assert/called-once? (:commit! (p/spies writer)))
        (assert/called-once? (:close (p/spies writer)))
        (is (#{:ok} result))
        (is (= a a'))
        (is (#{:ok} result'))
        (is (= b b')))))

  (testing "invalid seq-no terminates operation"
    (let [a (gen/generate (action-gen 1))
          b (gen/generate (action-gen 3))
          c (gen/generate (action-gen 4))
          seq-no (volatile! 0)
          writer (writer
                  (next-seq-no [_] (vswap! seq-no inc))
                  (next-conflict [_ offset [_ _] _]
                                 (when (= 2 offset)
                                   a))
                  (append! [_ _])
                  (rollback! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [[{result :result a' :event}
             {result' :result b' :event}] (append-events! store [a b c])]
        (assert/called-at-least-n-times? (:next-seq-no (p/spies writer)) 2)
        (assert/called-once? (:next-conflict spy))
        (assert/called-with? (:next-conflict spy) writer 1 (get-in a [:action :aggregate]) [])
        (assert/called-once-with? (:append! spy) writer a)
        (assert/called-once? (:rollback! (p/spies writer)))
        (assert/called-once? (:close (p/spies writer)))
        (is (#{:ok} result))
        (is (= a a'))
        (is (#{:invalid-seq-no} result'))
        (is (= b b')))))

  (testing "conflict terminates operation"
    (let [a (gen/generate (action-gen 1))
          b (gen/generate (action-gen 2))
          c (gen/generate (action-gen 3))
          seq-no (volatile! 0)
          writer (writer
                  (next-seq-no [_] (vswap! seq-no inc))
                  (next-conflict [_ offset [_ _] _]
                                 (when (= 2 offset)
                                   a))
                  (append! [_ _])
                  (rollback! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [[{result :result a' :event}
             {result' :result ours :ours theirs :theirs}] (append-events! store [a b c])]
        (assert/called-n-times? (:next-seq-no (p/spies writer)) 2)
        (assert/called-n-times? (:next-conflict spy) 2)
        (assert/called-with? (:next-conflict spy) writer 1 (get-in a [:action :aggregate]) [])
        (assert/called-with? (:next-conflict spy) writer 2 (get-in b [:action :aggregate]) [])
        (assert/called-once-with? (:append! spy) writer a)
        (assert/called-once? (:rollback! (p/spies writer)))
        (assert/called-once? (:close (p/spies writer)))
        (is (#{:ok} result))
        (is (= a a'))
        (is (#{:conflict} result'))
        (is (= ours b))
        (is (= theirs a)))))

  (testing "write conflict terminates operation"
    (let [a (gen/generate (action-gen 1))
          b (gen/generate (action-gen 2))
          c (gen/generate (action-gen 3))
          seq-no (volatile! 0)
          writer (writer
                  (next-seq-no [_] (vswap! seq-no inc))
                  (next-conflict [_ _ [_ _] _])
                  (append! [_ ev]
                           (when (= b ev)
                             (throw (->WriteConflictException))))
                  (rollback! [_]))
          spy (p/spies writer)
          store (->WriterStore writer)]
      (let [[{result :result a' :event}
             {result' :result b' :event}] (append-events! store [a b c])]
        (assert/called-n-times? (:next-seq-no (p/spies writer)) 2)
        (assert/called-n-times? (:next-conflict spy) 2)
        (assert/called-with? (:next-conflict spy) writer 1 (get-in a [:action :aggregate]) [])
        (assert/called-with? (:next-conflict spy) writer 2 (get-in b [:action :aggregate]) [])
        (assert/called-n-times? (:append! spy) 2)
        (assert/called-with? (:append! spy) writer a)
        (assert/called-with? (:append! spy) writer b)
        (is (= 'coherence.core.WriteConflictException
               (-> (spy :append!) spy/responses second :thrown :via first :type)))
        (assert/called-once? (:rollback! (p/spies writer)))
        (assert/called-once? (:close (p/spies writer)))
        (is (#{:ok} result))
        (is (= a a'))
        (is (#{:write-conflict} result'))
        (is (= b b'))))))