(ns coherence.reader-tests
  (:require [clojure.test :refer [testing is]]
            [coherence.core :refer :all]
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
          (is (= (mapv actions [0 2]) result)))))))

(defn test-read-events_no-replay
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
          (let [result (read-events reader (map identity) conj [] offset)]
            (is (= actions result))))))

    (testing "skip one event"
      (with-open [reader (open-read store)]
        (let [result (read-events reader (map identity) conj [] 2)]
          (is (= (drop 1 actions) result)))))))

(defn test-read-events_replay
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
          (let [result (read-events reader (map identity) conj [] offset)
                expected (filter (fn [{action :action}]
                                   (some? action))
                                 events)]
            (is (= expected result))))))

    (testing "one event replayed, one not"
      (with-open [reader (open-read store)]
        (let [result (read-events reader (map identity) conj [] 3)
              expected (filter (fn [{seq-no :seq-no {aggregate :aggregate} :action}]
                                 (and (some? aggregate)
                                      (or (>= seq-no 3)
                                          (= aggregate [:thing 2]))))
                               events)]
          (is (= expected result)))))))

(defn test-filter-aggregates_no-replay
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
                           :patch {:assoc {:a 1 :b 2}}}
                  :triggers #{[:trg 2]}}]]
    (write-events store actions)
    (testing "filter aggregate, one event"
      (with-open [reader (open-read store)]
        (let [result (filter-aggregates reader (map identity) conj [] 0 [[:thing 2]])]
          (is (= [(actions 1)] result)))))

    (testing "filter aggregate, many events"
      (with-open [reader (open-read store)]
        (let [result (filter-aggregates reader (map identity) conj [] 0 [[:thing 1]])]
          (is (= (mapv actions [0 2]) result)))))

    (testing "filter aggregates"
      (with-open [reader (open-read store)]
        (let [result (filter-aggregates reader (map identity) conj [] 0 [[:thing 1] [:thing 2]])]
          (is (= actions result)))))

    (testing "filter aggregates, skip first"
      (with-open [reader (open-read store)]
        (let [result (filter-aggregates reader (map identity) conj [] 2 [[:thing 1] [:thing 2]])]
          (is (= (rest actions) result)))))))

(defn test-filter-aggregates_replay
  [store]
  (let [actions [{:seq-no 1
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
                           :aggregate [:thing 1]
                           :patch {:assoc {:a 2}}}
                  :triggers #{[:trg 1]}}
                 {:seq-no 3
                  :source ::test
                  :timestamp (utils/now)
                  :effect {:reason ::whatever
                           :trigger [:trg 1]}}
                 {:seq-no 4
                  :source ::test
                  :timestamp (utils/now)
                  :action {:reason ::update
                           :actor [:test 1]
                           :aggregate [:thing 2]
                           :patch {:assoc {:a 1}}}
                  :triggers #{[:trg 2]}}]]
    (write-events store actions)
    (testing "filter aggregates, one event replayed"
      (with-open [reader (open-read store)]
        (let [result (filter-aggregates reader (map identity) conj [] 3 [[:thing 1]])]
          (is (= (mapv actions [0 1]) result)))))))

(defn test-filter-aggregate-kinds_no-replay
  [store]
  (let [actions [{:seq-no 1
                  :source ::test
                  :timestamp (utils/now)
                  :action {:reason ::create
                           :actor [:test 1]
                           :aggregate [:foo 1]
                           :patch {:assoc {:a 1}}}
                  :triggers #{[:trg 1]}}
                 {:seq-no 2
                  :source ::test
                  :timestamp (utils/now)
                  :action {:reason ::create
                           :actor [:test 1]
                           :aggregate [:bar 1]
                           :patch {:assoc {:a 1}}}
                  :triggers #{[:trg 2]}}
                 {:seq-no 3
                  :source ::test
                  :timestamp (utils/now)
                  :action {:reason ::create
                           :actor [:test 1]
                           :aggregate [:baz 1]
                           :patch {:assoc {:a 1}}}
                  :triggers #{[:trg 3]}}
                 {:seq-no 4
                  :source ::test
                  :timestamp (utils/now)
                  :action {:reason ::create
                           :actor [:test 1]
                           :aggregate [:bar 2]
                           :patch {:assoc {:a 1}}}
                  :triggers #{[:trg 4]}}]]
    (write-events store actions)
    (testing "filter aggregate kind, one event"
      (with-open [reader (open-read store)]
        (let [result (filter-aggregate-kinds reader (map identity) conj [] 0 [:foo])]
          (is (= [(actions 0)] result)))))

    (testing "filter aggregate kind, many events"
      (with-open [reader (open-read store)]
        (let [result (filter-aggregate-kinds reader (map identity) conj [] 0 [:bar])]
          (is (= (mapv actions [1 3]) result)))))

    (testing "filter aggregate kinds"
      (with-open [reader (open-read store)]
        (let [result (filter-aggregate-kinds reader (map identity) conj [] 0 [:foo :bar])]
          (is (= (mapv actions [0 1 3]) result)))))

    (testing "filter aggregates kinds, skip first"
      (with-open [reader (open-read store)]
        (let [result (filter-aggregate-kinds reader (map identity) conj [] 2 [:foo :bar])]
          (is (= (mapv actions [1 3]) result)))))))

(defn test-filter-aggregate-kinds_replay
  [store]
  (let [actions [{:seq-no 1
                  :source ::test
                  :timestamp (utils/now)
                  :action {:reason ::create
                           :actor [:test 1]
                           :aggregate [:foo 1]
                           :patch {:assoc {:a 1}}}
                  :triggers #{[:trg 1]}}
                 {:seq-no 2
                  :source ::test
                  :timestamp (utils/now)
                  :action {:reason ::create
                           :actor [:test 1]
                           :aggregate [:bar 1]
                           :patch {:assoc {:a 1}}}
                  :triggers #{[:trg 2]}}
                 {:seq-no 3
                  :source ::test
                  :timestamp (utils/now)
                  :action {:reason ::create
                           :actor [:test 1]
                           :aggregate [:baz 1]
                           :patch {:assoc {:a 1}}}
                  :triggers #{[:trg 3]}}
                 {:seq-no 4
                  :source ::test
                  :timestamp (utils/now)
                  :effect {:reason ::whatever
                           :trigger [:trg 1]}}
                 {:seq-no 5
                  :source ::test
                  :timestamp (utils/now)
                  :action {:reason ::create
                           :actor [:test 1]
                           :aggregate [:bar 2]
                           :patch {:assoc {:a 1}}}
                  :triggers #{[:trg 4]}}]]
    (write-events store actions)
    (testing "filter aggregate kinds, one event replayed"
      (with-open [reader (open-read store)]
        (let [result (filter-aggregate-kinds reader (map identity) conj [] 3 [:foo :bar])]
          (is (= (mapv actions [0 4]) result)))))))