(ns coherence.projection-tests
  (:require [clojure.test :refer [deftest testing is]]
            [coherence.projection :refer [apply-patch
                                          merge-projections
                                          project]]
            [coherence.test-utils :as utils]
            [spy.assert :as assert]
            [spy.core :as spy]))

(deftest test-apply-patch
  (testing "assoc key"
    (let [patch {:assoc {:a 1}}]
      (testing "empty aggregate"
        (let [actual (apply-patch {} patch)]
          (is (= {:a 1} actual))))

      (testing "disjunction"
        (let [actual (apply-patch {:b 2} patch)]
          (is (= {:a 1 :b 2} actual))))

      (testing "disjunction, associate value from patch"
        (let [actual (apply-patch {:a 0 :b 2} patch)]
          (is (= {:a 1 :b 2} actual))))))

  (testing "assoc nested key"
    (let [patch {:assoc {[:a :b] 1}}]
      (testing "empty aggregate"
        (let [actual (apply-patch {} patch)]
          (is (= {:a {:b 1}} actual))))

      (testing "disjunction"
        (let [actual (apply-patch {:a {:c 2}} patch)]
          (is (= {:a {:b 1 :c 2}} actual))))

      (testing "disjunction, associate value from patch"
        (let [actual (apply-patch {:a {:b 2 :c 3}} patch)]
          (is (= {:a {:b 1 :c 3}} actual))))))

  (testing "dissoc key"
    (let [patch {:dissoc [:a]}]
      (testing "empty aggregate"
        (let [actual (apply-patch {} patch)]
          (is (= {} actual))))

      (testing "complement"
        (let [actual (apply-patch {:a 1 :b 2} patch)]
          (is (= {:b 2} actual))))))

  (testing "dissoc nested key"
    (let [patch {:dissoc [[:a :b]]}]
      (testing "empty aggregate"
        (let [actual (apply-patch {} patch)]
          (is (= {} actual))))

      (testing "complement"
        (let [actual (apply-patch {:a {:b 1 :c 2} :d 3} patch)]
          (is (= {:a {:c 2} :d 3} actual))))))

  (testing "assoc & dissoc"
    (let [patch {:assoc {[:a :b] 1}
                 :dissoc [:c]}
          result (apply-patch  {:a {:d 2} :c 3 :d 4} patch)]
      (is (= {:a {:b 1 :d 2} :d 4} result)))))

(deftest test-project
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
    (testing "no replay"
      (letfn [(loadf [_ id]
                (case id
                  1 {:a 2}
                  2 {:a 1}))]
        (testing "transducer"
          (let [loadf' (spy/spy loadf)
                result (into []
                             (comp (filter #(>= (:seq-no %) 5))
                                   (project 5 loadf'))
                             events)]
            (is (= 2 (count result)))
            (is (= {:seq-no 5
                    :aggregates [{:id [:thing 2], :aggregate {:a 2}}]}
                   (result 0))
                (is (= {:seq-no 6
                        :aggregates [{:id [:thing 1], :aggregate {:a 3}}]}
                       (result 1))))
            (assert/called-n-times? loadf' 2)
            (assert/called-with? loadf' :thing 1)
            (assert/called-with? loadf' :thing 2)))

        (testing "sequence"
          (let [loadf' (spy/spy loadf)
                result (vec (project 5
                                     loadf'
                                     (filter #(>= (:seq-no %) 5) events)))]
            (is (= 2 (count result)))
            (is (= {:seq-no 5
                    :aggregates [{:id [:thing 2], :aggregate {:a 2}}]}
                   (result 0))
                (is (= {:seq-no 6
                        :aggregates [{:id [:thing 1], :aggregate {:a 3}}]}
                       (result 1))))
            (assert/called-n-times? loadf' 2)
            (assert/called-with? loadf' :thing 1)
            (assert/called-with? loadf' :thing 2)))))

    (testing "replay"
      (testing "transducer"
        (let [loadf (spy/spy (constantly {}))
              result (into [] (project 5 loadf) events)]
          (is (= 2 (count result)))
          (is (= {:seq-no 5
                  :aggregates [{:id [:thing 1], :aggregate {:a 2}}
                               {:id [:thing 2], :aggregate {:a 2}}]}
                 (result 0))
              (is (= {:seq-no 6
                      :aggregates [{:id [:thing 1], :aggregate {:a 3}}]}
                     (result 1))))
          (assert/not-called? loadf)))

      (testing "sequence"
        (let [loadf (spy/spy (constantly {}))
              result (vec (project 5 loadf events))]
          (is (= 2 (count result)))
          (is (= {:seq-no 5
                  :aggregates [{:id [:thing 1], :aggregate {:a 2}}
                               {:id [:thing 2], :aggregate {:a 2}}]}
                 (result 0))
              (is (= {:seq-no 6
                      :aggregates [{:id [:thing 1], :aggregate {:a 3}}]}
                     (result 1))))
          (assert/not-called? loadf))))))

(deftest test-merge-projections
  (testing "one projection"
    (let [projections [{:seq-no 1
                        :aggregates [{:id [:thing 1] :aggregate {:a 1}}
                                     {:id [:thing 2] :aggregate {:a 2}}]}]
          {:keys [seq-no aggregates]} (merge-projections projections)]
      (is (= 1 seq-no))
      (is (= [{:id [:thing 1] :aggregate {:a 1}}
              {:id [:thing 2] :aggregate {:a 2}}]
             (sort-by :id aggregates)))))

  (testing "multiple projections"
    (let [projections [{:seq-no 1
                        :aggregates [{:id [:thing 1] :aggregate {:a 1}}
                                     {:id [:thing 2] :aggregate {:a 1}}]}
                       {:seq-no 2
                        :aggregates [{:id [:thing 2] :aggregate {:a 2}}
                                     {:id [:thing 3] :aggregate {:a 3}}]}]
          {:keys [seq-no aggregates]} (merge-projections projections)]
      (is (= 2 seq-no))
      (is (= [{:id [:thing 1] :aggregate {:a 1}}
              {:id [:thing 2] :aggregate {:a 2}}
              {:id [:thing 3] :aggregate {:a 3}}]
             (sort-by :id aggregates))))))