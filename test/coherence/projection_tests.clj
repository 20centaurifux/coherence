(ns coherence.projection-tests
  (:require [clojure.test :refer [deftest testing is]]
            [coherence.projection :refer [apply-patch]]))

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