(ns coherence.jdbc.store-tests
  (:require [clojure.test :refer [use-fixtures deftest]]
            [coherence.core :refer [Store ->WriteConflictException]]
            [coherence.jdbc.core :refer [init-schema ->Store except]]
            [coherence.reader-tests :as rt]
            [coherence.writer-tests :as wt]
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

(defn- sqlite-fixture
  [f]
  (let [ds {:dbtype "sqlite"
            :dbname "file::memory:?cache=shared"}]
    (with-open [_ (jdbc/get-connection ds)]
      (binding [store (->Store ds opts)]
        (init-schema store)
        (f)))))

(use-fixtures :each sqlite-fixture)

(defmethod except org.sqlite.SQLiteException
  [e]
  (if (= (.getErrorCode e) org.sqlite.core.Codes/SQLITE_CONSTRAINT)
    (throw (->WriteConflictException "SQL constraint violation occured." {} e))
    (throw e)))

;;; Writer

(deftest test-writer_open-write
  (wt/test-open-write store))

(deftest test-writer_close
  (wt/test-close store))

(deftest test-writer_append
  (wt/test-append store))

(deftest test-writer_commit
  (wt/test-commit store))

(deftest test-writer_rollback
  (wt/test-rollback store))

(deftest test-writer_next-seq-no_same-transaction
  (wt/test-next-seq-no_same-transaction store))

(deftest test-writer_next-seq-no_after_commit
  (wt/test-next-seq-no_after_commit store))

(deftest test-writer_next-seq-no_after_rollback
  (wt/test-next-seq-no_after_rollback store))

(deftest test-writer_next-conflict_same-transaction
  (wt/test-next-conflict_same-transaction store))

(deftest test-writer_next-conflict_after_commit
  (wt/test-next-conflict_after_commit store))

(deftest test-writer_next-conflict_after_rollback
  (wt/test-next-conflict_after_rollback store))

(deftest test-writer_concurrent-writers
  (wt/test-write_conflict store))

;;; Reader

(deftest test-reader_open-read
  (rt/test-open-read store))

(deftest test-reader_close
  (rt/test-close store))

(deftest test-reader_max-seq-no
  (rt/test-max-seq-no store))

(deftest test-reader_query-conflicts
  (rt/test-query-conflicts store))

(deftest test-reader_read-events_no-replay
  (rt/test-read-events_no-replay store))

(deftest test-reader_read-events_replay
  (rt/test-read-events_replay store))

(deftest test-filter-aggregates_no-replay
  (rt/test-filter-aggregates_no-replay store))

(deftest test-filter-aggregates_replay
  (rt/test-filter-aggregates_replay store))

(deftest test-filter-aggregate-kinds_no-replay
  (rt/test-filter-aggregate-kinds_no-replay store))

(deftest test-filter-aggregate-kinds_replay
  (rt/test-filter-aggregate-kinds_replay store))