(ns coherence.writer-tests
  (:require [clojure.test :refer [testing is]]
            [clojure.test.check.generators :as gen]
            [coherence.core :refer :all]
            [coherence.test-utils :as utils])
  (:import (coherence.core WriteConflictException)))

(defn test-open-write
  [store]
  (testing "returns Writer"
    (with-open [writer (open-write store)]
      (is (satisfies? Writer writer))))

  (testing "Writer is not closed"
    (with-open [writer (open-write store)]
      (is (not (closed? writer))))))

(defn test-close
  [store]
  (testing "returns nil"
    (let [writer (open-write store)]
      (is (nil? (.close writer)))))

  (testing "Writer is closed"
    (let [writer (open-write store)]
      (.close writer)
      (is (closed? writer)))))

(defn test-commit
  [store]
  (testing "returns nil"
    (with-open [writer (open-write store)]
      (is (nil? (commit! writer)))))

  (testing "Writer is not closed"
    (with-open [writer (open-write store)]
      (commit! writer)
      (is (not (closed? writer)))))

  (testing "closed Writer throws Exception"
    (let [writer (open-write store)]
      (.close writer)
      (is (thrown? Exception (commit! writer))))))

(defn test-rollback
  [store]
  (testing "returns nil"
    (with-open [writer (open-write store)]
      (is (nil? (rollback! writer)))))

  (testing "Writer is not closed"
    (with-open [writer (open-write store)]
      (rollback! writer)
      (is (not (closed? writer)))))

  (testing "closed Writer throws Exception"
    (let [writer (open-write store)]
      (.close writer)
      (is (thrown? Exception (rollback! writer))))))

(defn test-append
  [store]
  (testing "action returns nil"
    (let [ev (gen/generate utils/action-gen)]
      (with-open [writer (open-write store)]
        (is nil? (append! writer ev)))))

  (testing "effect returns nil"
    (let [ev (gen/generate utils/effect-gen)]
      (with-open [writer (open-write store)]
        (is nil? (append! writer ev)))))

  (testing "arbitary data throws exception"
    (let [x (gen/generate (gen/map gen/keyword gen/any))]
      (with-open [writer (open-write store)]
        (is (thrown? Exception (append! writer x))))))

  (testing "closed Writer throws exception"
    (let [ev (gen/generate utils/event-gen)]
      (with-open [writer (open-write store)]
        (.close writer)
        (is (thrown? Exception (append! writer ev)))))))

(defn test-next-seq-no_same-transaction
  [store]
  (testing "empty event store returns 1"
    (with-open [writer (open-write store)]
      (let [seq-no (next-seq-no writer)]
        (is (= 1 seq-no)))))

  (testing "event increments next available seq-no"
    (let [ev (gen/generate utils/event-gen)]
      (with-open [writer (open-write store)]
        (append! writer ev)
        (let [seq-no (next-seq-no writer)]
          (is (= (inc (:seq-no ev)) seq-no))))))

  (testing "closed Writer throws Exception"
    (let [writer (open-write store)]
      (.close writer)
      (is (thrown? Exception (next-seq-no writer))))))

(defn test-next-seq-no_after_commit
  [store]
  (testing "committed event increments next available seq-no"
    (let [ev (gen/generate utils/event-gen)]
      ;; Append action and commit.
      (with-open [writer (open-write store)]
        (append! writer ev)
        (commit! writer))
      ;; New session should receive incremented seq-no.
      (with-open [writer (open-write store)]
        (let [seq-no (next-seq-no writer)]
          (is (= (inc (:seq-no ev)) seq-no)))))))

(defn test-next-seq-no_after_rollback
  [store]
  (testing "rolled back event doesn't change next available seq-no"
    (let [ev (gen/generate utils/event-gen)]
      ;; Append action and roll back.
      (with-open [writer (open-write store)]
        (append! writer ev)
        (rollback! writer)))
    ;; New session should receive default seq-no.
    (with-open [writer (open-write store)]
      (let [seq-no (next-seq-no writer)]
        (is (= 1 seq-no))))))

(defn test-next-conflict_same-transaction
  [store]
  (testing "empty event store has no conflicts"
    (with-open [writer (open-write store)]
      (let [conflict (next-conflict writer 1 [:thing 1] #{})]
        (is (nil? conflict)))))

  (testing "find conflicting action"
    (let [events [{:seq-no 1
                   :source ::test
                   :timestamp (utils/now)
                   :action {:reason ::create
                            :actor [:test 1]
                            :aggregate [:thing 1]
                            :patch {:a 1}}}
                  {:seq-no 2
                   :source ::test
                   :timestamp (utils/now)
                   :action {:reason ::update
                            :actor [:test 1]
                            :aggregate [:thing 1]
                            :patch {:a 2}}}]]
      (with-open [writer (open-write store)]
        (run! (partial append! writer) events)
        (testing "find conflict"
          (let [conflict (next-conflict writer 1 [:thing 1] #{})]
            (is (= (events 0) conflict))))

        (testing "skip resolved conflict"
          (let [conflict (next-conflict writer 1 [:thing 1] #{1})]
            (is (= (events 1) conflict))))

        (testing "respect offset"
          (let [conflict (next-conflict writer 2 [:thing 1] #{})]
            (is (= (events 1) conflict)))))))

  (testing "find conflicting effect"
    (let [events [{:seq-no 1
                   :source ::test
                   :timestamp (utils/now)
                   :action {:reason ::create
                            :actor [:test 1]
                            :aggregate [:thing 1]
                            :patch {:a 1}}
                   :triggers #{[:test 1]}}
                  {:seq-no 2
                   :source ::test
                   :timestamp (utils/now)
                   :effect {:reason ::whatever
                            :trigger [:test 1]}}
                  {:seq-no 3
                   :source ::test
                   :timestamp (utils/now)
                   :action {:reason ::update
                            :actor [:test 1]
                            :aggregate [:thing 1]
                            :patch {:a 2}}}]]
      (with-open [writer (open-write store)]
        (run! (partial append! writer) events)
        (testing "find conflict"
          (let [conflict (next-conflict writer 2 [:thing 1] #{})]
            (is (= (events 1) conflict))))

        (testing "skip resolved conflict"
          (let [conflict (next-conflict writer 2 [:thing 1] #{2})]
            (is (= (events 2) conflict))))

        (testing "respect offset"
          (let [conflict (next-conflict writer 3 [:thing 1] #{})]
            (is (= (events 2) conflict)))))))

  (testing "skip other aggregates"
    (let [events [{:seq-no 1
                   :source ::test
                   :timestamp (utils/now)
                   :action {:reason ::create
                            :actor [:test 1]
                            :aggregate [:thing 1]
                            :patch {:a 1}}
                   :triggers #{[:test 1]}}
                  {:seq-no 2
                   :source ::test
                   :timestamp (utils/now)
                   :action {:reason ::create
                            :actor [:test 1]
                            :aggregate [:thing 2]
                            :patch {:a 2}}
                   :triggers #{[:test 2]}}
                  {:seq-no 3
                   :source ::test
                   :timestamp (utils/now)
                   :effect {:reason ::whatever
                            :trigger [:test 1]}}
                  {:seq-no 4
                   :source ::test
                   :timestamp (utils/now)
                   :action {:reason ::update
                            :actor [:test 1]
                            :aggregate [:thing 2]
                            :patch {:a 3}}}]]
      (with-open [writer (open-write store)]
        (run! (partial append! writer) events)
        (testing "skip action"
          (let [conflict (next-conflict writer 1 [:thing 2] #{})]
            (is (= (events 1) conflict))))

        (testing "skip effect"
          (let [conflict (next-conflict writer 3 [:thing 2] #{})]
            (is (= (events 3) conflict)))))))

  (testing "closed Writer throws Exception"
    (let [writer (open-write store)]
      (.close writer)
      (is (thrown? Exception (next-conflict writer 1 [:thing 1] #{}))))))

(defn test-next-conflict_after_commit
  [store]
  (testing "find next conflict"
    (let [ev {:seq-no 1
              :source ::test
              :timestamp (utils/now)
              :action {:reason ::create
                       :actor [:test 1]
                       :aggregate [:thing 1]
                       :patch {:a 1}}}]
      ;; Append action and commit.
      (with-open [writer (open-write store)]
        (append! writer ev)
        (commit! writer))
      ;; Conflict should be found.
      (with-open [writer (open-write store)]
        (let [conflict (next-conflict writer 1 [:thing 1] #{})]
          (is (= ev conflict)))))))

(defn test-next-conflict_after_rollback
  [store]
  (testing "find next conflict"
    (let [ev {:seq-no 1
              :source ::test
              :timestamp (utils/now)
              :action {:reason ::create
                       :actor [:test 1]
                       :aggregate [:thing 1]
                       :patch {:a 1}}}]
      ;; Append action and commit.
      (with-open [writer (open-write store)]
        (append! writer ev)
        (rollback! writer))
      ;; Conflict should not be found.
      (with-open [writer (open-write store)]
        (let [conflict (next-conflict writer 1 [:thing 1] #{})]
          (is (nil? conflict)))))))

(defn test-write_conflict
  [store]
  (testing "concurrent events (single writer)"
    (let [ev (assoc (gen/generate utils/event-gen) :seq-no 1)]
      (with-open [writer (open-write store)]
        (append! writer ev)
        (is (thrown? WriteConflictException (append! writer ev))))))

  (testing "concurrent events (two writers)"
    (let [ev (assoc (gen/generate utils/event-gen) :seq-no 1)
          ev' (assoc (gen/generate utils/event-gen) :seq-no 1)]
      (with-open [writer (open-write store)
                  writer' (open-write store)]
        (append! writer ev)
        (commit! writer)
        (is (thrown? Exception (append! writer' ev')))))))