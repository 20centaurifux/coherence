(ns coherence.jdbc.core
  (:require [clojure.edn :as edn]
            [coherence.core :as c]
            [coherence.jdbc.ddl :as ddl]
            [honey.sql :as sql]
            [meander.epsilon :as m]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [pold.core :as p]))

(defprotocol Connection
  (closed? [conn])
  (commit! [conn])
  (rollback! [conn])
  (execute-one! [conn stmt])
  (plan! [conn stmt]))

;;; transform events to HoneySQL

(defn insert-event
  [tables ev]
  (m/search ev
    ;; event table 
    {:seq-no ?seq-no
     :timestamp ?timestamp
     :source ?source}
    {:insert-into (:event tables)
     :values [{:seq-no ?seq-no
               :timestamp (.getEpochSecond ?timestamp)
               :source (pr-str ?source)}]}
    ;; action table
    {:seq-no ?seq-no
     :action {:reason ?reason
              :actor [?actor-kind ?actor-id]
              :aggregate [?aggregate-kind ?aggregate-id]
              :patch ?patch}}
    {:insert-into (:action tables)
     :values [{:seq-no ?seq-no
               :reason (pr-str ?reason)
               :actor-kind (pr-str ?actor-kind)
               :actor-id (pr-str ?actor-id)
               :aggregate-kind (pr-str ?aggregate-kind)
               :aggregate-id (pr-str ?aggregate-id)
               :patch (pr-str ?patch)}]}
    ;; trigger table
    {:seq-no ?seq-no
     :triggers (m/scan [?kind ?id])}

    {:insert-into (:trigger tables)
     :values [{:seq-no ?seq-no
               :trigger-kind (pr-str ?kind)
               :trigger-id (pr-str ?id)}]}
    ;; effect table
    {:seq-no ?seq-no
     :effect {:reason ?reason
              :trigger [?kind ?id]}}
    {:insert-into (:effect tables)
     :values [{:seq-no ?seq-no
               :reason (pr-str ?reason)
               :trigger-kind (pr-str ?kind)
               :trigger-id (pr-str ?id)}]}))

;;; transform rows to events

(defn- row->action
  [ev]
  (m/match ev
    {:seq-no ?seq-no
     :timestamp ?timestamp
     :source ?source
     :reason ?reason
     :actor-kind ?actor-kind
     :actor-id ?actor-id
     :aggregate-kind ?aggregate-kind
     :aggregate-id ?aggregate-id
     :patch ?patch}
    {:seq-no ?seq-no
     :timestamp (java.time.Instant/ofEpochSecond ?timestamp)
     :source (edn/read-string ?source)
     :action {:reason (edn/read-string ?reason)
              :actor [(edn/read-string ?actor-kind)
                      (edn/read-string ?actor-id)]
              :aggregate [(edn/read-string ?aggregate-kind)
                          (edn/read-string ?aggregate-id)]
              :patch (edn/read-string ?patch)}}))

(defn- merge-action-rows
  []
  (letfn [(deserialize [& more]
            (mapv edn/read-string more))]
    (p/pold
     (p/partitioner
      (p/part
       :seq-no
       (fn [{:keys [:trigger-kind :trigger-id] :as row}]
         (cond-> (row->action row)
           (and trigger-kind trigger-id) (assoc :triggers
                                                #{(deserialize trigger-kind trigger-id)})))
       (fn [result {:keys [:trigger-kind :trigger-id]}]
         (update result :triggers conj (deserialize trigger-kind trigger-id))))))))

(defn- row->effect
  [ev]
  (m/match ev
    {:seq-no ?seq-no
     :timestamp ?timestamp
     :source ?source
     :reason ?reason
     :trigger-kind ?trigger-kind
     :trigger-id ?trigger-id}
    {:seq-no ?seq-no
     :timestamp (java.time.Instant/ofEpochSecond ?timestamp)
     :source (edn/read-string ?source)
     :effect {:reason (edn/read-string ?reason)
              :trigger [(edn/read-string ?trigger-kind)
                        (edn/read-string ?trigger-id)]}}))

;;; query conflicts

(defn- query-next-conflicting-action
  [conn {:keys [action event trigger]} offset [aggregate-kind aggregate-id] resolved]
  (let [q {:select [:ev/timestamp
                    :ev/source
                    [:a/seq-no :seq-no]
                    :a/reason
                    :a/actor-kind
                    :a/actor-id
                    :a/aggregate-kind
                    :a/aggregate-id
                    :a/patch
                    :t/trigger-kind
                    :t/trigger-id]
           :from [[action :a]]
           :join [[event :ev] [:= :a/seq-no :ev/seq-no]]
           :left-join [[trigger :t] [:= :a/seq-no :t/seq-no]]
           :where (cond-> [:and
                           [:>= :a/seq-no offset]
                           [:= :a/aggregate-kind (pr-str aggregate-kind)]
                           [:= :a/aggregate-id (pr-str aggregate-id)]]
                    (seq resolved) (conj [[:not [:in :a/seq-no resolved]]]))
           :order-by [:a/seq-no]}]
    (->> (plan! conn q)
         (into [] (comp (merge-action-rows) (take 1)))
         first)))

(defn- query-next-conflicting-effect
  [conn {:keys [effect event action trigger]} offset [aggregate-kind aggregate-id] resolved]
  (let [q {:select [:ev/timestamp
                    :ev/source
                    [:eff/seq-no :seq-no]
                    :eff/reason
                    :eff/trigger-kind
                    :eff/trigger-id]
           :from [[effect :eff]]
           :join [[event :ev] [:= :eff/seq-no :ev/seq-no]]
           :where (cond-> [:and
                           [:>= :eff/seq-no offset]
                           [:exists {:select [:a/seq-no]
                                     :from [[action :a]]
                                     :join [[trigger :t] [:= :t/seq-no :a/seq-no]]
                                     :where [:and
                                             [:= :t/trigger-kind :eff/trigger-kind]
                                             [:= :t/trigger-id :eff/trigger-id]
                                             [:= :a/aggregate-kind (pr-str aggregate-kind)]
                                             [:= :a/aggregate-id (pr-str aggregate-id)]]}]]
                    (seq resolved) (conj [[:not [:in :eff/seq-no resolved]]]))
           :order-by [:eff/seq-no]
           :limit 1}]
    (some-> (execute-one! conn q)
            row->effect)))

(def ^:private query-conflicts (juxt query-next-conflicting-action
                                     query-next-conflicting-effect))

;;; Writer

(defmulti except class)

(defmethod except java.lang.Exception
  [e]
  (throw e))

(defn- query-max-seq-no
  [conn {:keys [event]}]
  (let [q {:select [[[:coalesce [:max :seq_no] [:inline 0]] :seq_no]]
           :from event}]
    (-> (execute-one! conn q)
        :seq-no)))

(deftype Writer [conn tables]
  c/Closed
  (closed? [_]
    (closed? conn))

  java.io.Closeable
  (close [_]
    (.close conn))

  c/Writer
  (commit! [_]
    (try
      (commit! conn)
      (catch Exception e (except e))))

  (rollback! [_]
    (try
      (rollback! conn)
      (catch Exception e (except e))))

  (next-seq-no [_]
    (try
      (-> (query-max-seq-no conn tables)
          inc)
      (catch Exception e (except e))))

  (append! [_ ev]
    (try
      (run! #(execute-one! conn %)
            (insert-event tables ev))
      (catch Exception e (except e))))

  (next-conflict [_ offset aggregate resolved]
    (try
      (->> (query-conflicts conn tables offset aggregate resolved)
           (sort-by #(get % :seq-no Long/MAX_VALUE))
           first)
      (catch Exception e (except e)))))

;;; Reader

(defn- select-actions-lt-offset-query
  [{:keys [effect event action trigger]} offset]
  {:with [[:aff {:select [:a/aggregate-kind :a/aggregate-id]
                 :from [[effect :eff]]
                 :join [[trigger :t] [:and
                                      [:= :t/trigger-kind :eff/trigger-kind]
                                      [:= :t/trigger-id :eff/trigger-id]]
                        [action :a] [:= :t/seq-no :a/seq-no]]
                 :where [:>= :eff/seq-no offset]
                 :group-by [:a/aggregate-kind :a/aggregate-id]}]]
   :select [:ev/timestamp
            :ev/source
            [:a/seq-no :seq-no]
            :a/reason
            :a/actor-kind
            :a/actor-id
            :a/aggregate-kind
            :a/aggregate-id
            :a/patch
            :t/trigger-kind
            :t/trigger-id]
   :from :aff
   :join [[action :a] [:and
                       [:= :a/aggregate-kind :aff/aggregate-kind]
                       [:= :a/aggregate-id :aff/aggregate-id]]
          [event :ev] [:= :ev/seq-no :a/seq-no]]
   :left-join [[trigger :t] [:= :t/seq-no :a/seq-no]]
   :where [[:< :a/seq-no offset]]})

(defn- select-actions-gte-offset-query
  [{:keys [action event trigger]} offset]
  {:select [:ev/timestamp
            :ev/source
            [:a/seq-no :seq-no]
            :a/reason
            :a/actor-kind
            :a/actor-id
            :a/aggregate-kind
            :a/aggregate-id
            :a/patch
            :t/trigger-kind
            :t/trigger-id]
   :from [[action :a]]
   :join [[event :ev] [:= :ev/seq-no :a/seq-no]]
   :left-join [[trigger :t] [:= :t/seq-no :a/seq-no]]
   :where [:>= :a/seq-no offset]})

(defn- select-actions-query
  [tables offset]
  {:union [(select-actions-lt-offset-query tables offset)
           (select-actions-gte-offset-query tables offset)]
   :order-by [:seq-no]})

(deftype Reader [conn tables]
  c/Closed
  (closed? [_]
    (closed? conn))

  java.io.Closeable
  (close [_]
    (.close conn))

  c/Reader
  (stream-events [_ xform f init offset]
    (let [q (select-actions-query tables offset)]
      (transduce (comp (merge-action-rows) xform)
                 f
                 init
                 (plan! conn q)))))

;;; Store

(deftype WrappedConnection [conn sql-opts]
  Connection
  (closed? [_]
    (.isClosed conn))

  (commit! [_]
    (.commit conn))

  (rollback! [_]
    (.rollback conn))

  (execute-one! [_ stmt]
    (jdbc/execute-one! conn
                       (sql/format stmt sql-opts)
                       {:builder-fn rs/as-unqualified-kebab-maps}))

  (plan! [_ stmt]
    (jdbc/plan conn
               (sql/format stmt sql-opts)
               jdbc/unqualified-snake-kebab-opts))

  java.io.Closeable
  (close [_]
    (.close conn)))

(defn- wrap-connection
  [ds sql-opts]
  (->WrappedConnection (jdbc/get-connection ds {:auto-commit false}) sql-opts))

(defprotocol Schema
  (init-schema [_]))

(deftype Store [ds opts]
  Schema
  (init-schema [_]
    (with-open [conn (jdbc/get-connection ds {:auto-commit false})]
      (run! #(jdbc/execute-one! conn %) (ddl/create-tables opts))
      (.commit conn)))

  c/Store
  (open-write [_]
    (->Writer (wrap-connection ds (:sql opts))
              (:tables opts)))

  (open-read [_]
    (->Reader (wrap-connection ds (:sql opts))
              (:tables opts))))