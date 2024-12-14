(ns coherence.jdbc.core-tests
  (:require [clojure.test :refer [use-fixtures deftest]]
            [coherence.core :refer [Store ->WriteConflictException]]
            [coherence.jdbc.core :refer [->Store except]]
            [coherence.jdbc.ddl :refer [create-tables]]
            [coherence.writer-tests :as t]
            [next.jdbc :as jdbc]))

(def ^:private ^:dynamic store (reify
                                 Store
                                 (open-write [_] (throw (UnsupportedOperationException.)))))

(def ^:private opts {:sql {:dialect :ansi
                           :quoted-snake true}
                     :tables {:event :event
                              :action :action
                              :effect :effect
                              :trigger :trigger}})

(defn- data-source-fixture
  [f]
  (let [ds {:dbtype "sqlite"
            :dbname "file::memory:?cache=shared"}]
    (with-open [conn (jdbc/get-connection ds {:auto-commit true})]
      (run! #(jdbc/execute-one! conn %) (create-tables opts))
      (binding [store (->Store ds opts)]
        (f)))))

(use-fixtures :each data-source-fixture)

(defmethod except org.sqlite.SQLiteException
  [e]
  (if (= (.getErrorCode e) org.sqlite.core.Codes/SQLITE_CONSTRAINT)
    (throw (->WriteConflictException "SQL constraint violation occured." {} e))
    (throw e)))

(deftest test-open-write
  (t/test-open-write store))

(deftest test-close
  (t/test-close store))

(deftest test-append
  (t/test-append store))

(deftest test-commit
  (t/test-commit store))

(deftest test-rollback
  (t/test-rollback store))

(deftest test-next-seq-no_same-transaction
  (t/test-next-seq-no_same-transaction store))

(deftest test-next-seq-no_after_commit
  (t/test-next-seq-no_after_commit store))

(deftest test-next-seq-no_after_rollback
  (t/test-next-seq-no_after_rollback store))

(deftest writer_next-conflict_same-transaction
  (t/test-next-conflict_same-transaction store))

(deftest test_next-conflict_after_commit
  (t/test-next-conflict_after_commit store))

(deftest test-next-conflict_after_rollback
  (t/test-next-conflict_after_rollback store))

(deftest test-concurrent-writers
  (t/test-write_conflict store))