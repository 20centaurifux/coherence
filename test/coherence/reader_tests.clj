(ns coherence.reader-tests
  (:require [clojure.test :refer [testing is]]
            [coherence.core :refer :all :exclude [transduce]]
            [coherence.specs]
            [coherence.test-utils :as utils]))

(defn test-open-read
  [store]
  (testing "returns Reader"
    (with-open [reader (open-read store)]
      (is (satisfies? Reader reader))))

  (testing "Reader is not closed"
    (with-open [reader (open-read store)]
      (is (not (closed? reader))))))

(defn test-close
  [store]
  (testing "returns nil"
    (let [reader (open-read store)]
      (is (nil? (.close reader)))))

  (testing "Reader is closed"
    (let [reader (open-read store)]
      (.close reader)
      (is (closed? reader)))))

(defn- write-events
  [store events]
  (with-open [writer (open-write store)]
    (run! (partial append! writer) events)
    (commit! writer)))

(defn test-max-seq-no
  [store]
  (testing "empty store"
    (with-open [reader (open-read store)]
      (is (= 0 (max-seq-no reader)))))

  (testing "filled store"
    (let [actions [{:seq-no 100
                    :source ::test
                    :timestamp (utils/now)
                    :action {:reason ::create
                             :actor [:test 1]
                             :aggregate [:thing 1]
                             :patch {:assoc {:a  1}}}}]]
      (write-events store actions)
      (with-open [reader (open-read store)]
        (is (= 100 (max-seq-no reader)))))))

(defn test-query-conflicts
  [store]
  (let [actions [{:seq-no 1
                  :source ::test
                  :timestamp (utils/now)
                  :action {:reason ::create
                           :actor [:test 1]
                           :aggregate [:thing 1]
                           :patch {:assoc {:a 1}}}}
                 {:seq-no 2
                  :source ::test
                  :timestamp (utils/now)
                  :action {:reason ::create
                           :actor [:test 1]
                           :aggregate [:thing 2]
                           :patch {:assoc {:a 1}}}}
                 {:seq-no 3
                  :source ::test
                  :timestamp (utils/now)
                  :action {:reason ::update
                           :actor [:test 1]
                           :aggregate [:thing 1]
                           :patch {:assoc {:a 2}}}}]]
    (write-events store actions)
    (testing "no conflicts"
      (with-open [reader (open-read store)]
        (let [result (query-conflicts reader 1 [:thing 3])]
          (is (empty? result)))))

    (testing "find single conflict"
      (with-open [reader (open-read store)]
        (let [result (query-conflicts reader 2 [:thing 1])]
          (is (= [(actions 2)] result)))))

    (testing "find many conflicts"
      (with-open [reader (open-read store)]
        (let [result (query-conflicts reader 1 [:thing 1])]
          (is (= [(actions 0) (actions 2)] result)))))))

(defn test-stream-events_no-replay
  [store]
  (let [actions [{:seq-no 1
                  :source ::test
                  :timestamp (utils/now)
                  :action {:reason ::create
                           :actor [:test 1]
                           :aggregate [:thing 1]
                           :patch {:assoc {:a 1}}}
                  :triggers #{[:trg 1] [:trg 2]}}
                 {:seq-no 2
                  :source ::test
                  :timestamp (utils/now)
                  :action {:reason ::update
                           :actor [:test 1]
                           :aggregate [:thing 1]
                           :patch {:assoc {:a 2}}}}]]
    (write-events store actions)
    (testing "all events"
      (doseq [offset [0 1]]
        (with-open [reader (open-read store)]
          (let [result (stream-events reader (map identity) conj [] offset)]
            (is (= actions result))))))

    (testing "skip one event"
      (with-open [reader (open-read store)]
        (let [result (stream-events reader (map identity) conj [] 2)]
          (is (= (drop 1 actions) result)))))))

(defn test-stream-events_replay
  [store]
  (let [events [{:seq-no 1
                 :source ::test
                 :timestamp (utils/now)
                 :action {:reason ::create
                          :actor [:test 1]
                          :aggregate [:thing 1]
                          :patch {:assoc {:a 1}}}
                 :triggers #{[:trg 1]}}
                {:seq-no 2
                 :source ::test
                 :timestamp (utils/now)
                 :action {:reason ::create
                          :actor [:test 1]
                          :aggregate [:thing 2]
                          :patch {:assoc {:a 1}}}
                 :triggers #{[:trg 2]}}
                {:seq-no 3
                 :source ::test
                 :timestamp (utils/now)
                 :action {:reason ::update
                          :actor [:test 1]
                          :aggregate [:thing 1]
                          :patch {:assoc {:a 2}}}
                 :triggers #{[:trg 1]}}
                {:seq-no 4
                 :source ::test
                 :timestamp (utils/now)
                 :effect {:reason ::whatever
                          :trigger [:trg 2]}}
                {:seq-no 5
                 :source ::test
                 :timestamp (utils/now)
                 :action {:reason ::update
                          :actor [:test 1]
                          :aggregate [:thing 2]
                          :patch {:assoc {:a 2}}}
                 :triggers #{[:trg 2]}}
                {:seq-no 6
                 :source ::test
                 :timestamp (utils/now)
                 :action {:reason ::update
                          :actor [:test 1]
                          :aggregate [:thing 1]
                          :patch {:assoc {:a 3}}}
                 :triggers #{[:trg 1]}}]]
    (write-events store events)
    (testing "all events"
      (doseq [offset [0 1]]
        (with-open [reader (open-read store)]
          (let [result (stream-events reader (map identity) conj [] offset)
                expected (filter (fn [{action :action}]
                                   (some? action))
                                 events)]
            (is (= expected result))))))

    (testing "one event replayed, one not"
      (with-open [reader (open-read store)]
        (let [result (stream-events reader (map identity) conj [] 3)
              expected (filter (fn [{seq-no :seq-no {aggregate :aggregate} :action}]
                                 (and (some? aggregate)
                                      (or (>= seq-no 3)
                                          (= aggregate [:thing 2]))))
                               events)]
          (is (= expected result)))))))