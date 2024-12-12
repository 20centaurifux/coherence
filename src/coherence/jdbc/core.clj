(ns coherence.jdbc.core
  (:require [clojure.edn :as edn]
            [coherence.core :as c]
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
       (fn [{:keys [:trigger-kind :trigger-id] :as input}]
         (cond-> (row->action input)
           (and trigger-kind trigger-id) (assoc :triggers
                                                [(deserialize trigger-kind trigger-id)])))
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
  [conn tables offset [aggregate-kind aggregate-id] resolved]
  (let [q {:select [:a/* :ev/* :t/*]
           :from [[(:action tables) :a]]
           :join [[(:event tables) :ev] [:= :a/seq-no :ev/seq-no]]
           :left-join [[(:trigger tables) :t] [:= :a/seq-no :t/seq-no]]
           :where (cond-> [:and
                           [:>= :a/seq-no offset]
                           [:= :a/aggregate-kind (pr-str aggregate-kind)]
                           [:= :a/aggregate-id (pr-str aggregate-id)]]
                    (seq resolved) (conj [[:not [:in :a/seq-no resolved]]]))
           :order-by [:a/seq-no]}
        xf (comp (merge-action-rows) (take 1))]
    (first (into [] xf (plan! conn q)))))

(defn- query-next-conflicting-effect
  [conn tables offset [aggregate-kind aggregate-id] resolved]
  (let [q {:select [:eff/* :ev/*]
           :from [[(:effect tables) :eff]]
           :join [[(:event tables) :ev] [:= :eff/seq-no :ev/seq-no]]
           :where (cond-> [:and
                           [:>= :eff/seq-no offset]
                           [:exists {:select [:a/seq-no]
                                     :from [[(:action tables) :a]]
                                     :join [[(:trigger tables) :t] [:= :t/seq-no :a/seq-no]]
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

;;; Writer implementation

(defmulti except class)

(defmethod except java.lang.Exception
  [e]
  (throw e))

(deftype Writer [conn tables]
  c/Writer
  (closed? [_]
    (closed? conn))

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
      (let [q {:select [[[:coalesce [:max :seq_no] [:inline 0]] :seq_no]]
               :from (:event tables)}]
        (-> (execute-one! conn q)
            :seq-no
            inc))
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
      (catch Exception e (except e))))

  java.io.Closeable
  (close [_]
    (.close conn)))

;;; Store implementation

(deftype WrappedConnection [conn opts]
  Connection
  (closed? [_]
    (.isClosed conn))

  (commit! [_]
    (.commit conn))

  (rollback! [_]
    (.rollback conn))

  (execute-one! [_ stmt]
    ; TODO: (println "execute-one!" (sql/format stmt opts))
    (jdbc/execute-one! conn
                       (sql/format stmt opts)
                       {:builder-fn rs/as-unqualified-kebab-maps}))

  (plan! [_ stmt]
    ; TODO: (println "-plan!" (sql/format stmt opts))
    (jdbc/plan conn
               (sql/format stmt opts)
               jdbc/unqualified-snake-kebab-opts))

  java.io.Closeable
  (close [_]
    (.close conn)))

(deftype Store [ds opts]
  c/Store
  (open-write [_]
    (let [conn (jdbc/get-connection ds {:auto-commit false})]
      (->Writer (->WrappedConnection conn (:sql opts))
                (:tables opts)))))