(ns coherence.jdbc.ddl
  (:require [honey.sql :as sql]))

(defn- create-event-table
  [relation]
  {:create-table [relation :if-not-exists]
   :with-columns
   [[:seq-no :int :primary-key :not-null]
    [:timestamp :int :not-null]
    [:source [:varchar 50] :not-null]]})

(defn- create-action-table
  [relation event-table]
  {:create-table [relation :if-not-exists]
   :with-columns
   [[:seq-no :int :primary-key :not-null]
    [:reason [:varchar 50] :not-null]
    [:actor-kind [:varchar 50] :not-null]
    [:actor-id [:varchar 50] :not-null]
    [:aggregate-kind [:varchar 50] :not-null]
    [:aggregate-id [:varchar 50] :not-null]
    [:patch [:varchar 10000] :not-null]
    [[:foreign-key :seq-no] [:references event-table :seq-no]]]})

(defn- create-action-table-aggregate-index
  [relation]
  {:create-index [(keyword (str (name relation) "-aggregate-idx"))
                  [relation :aggregate-kind :aggregate-id]]})

(defn- create-effect-table
  [relation event-table]
  {:create-table [relation :if-not-exists]
   :with-columns
   [[:seq-no :int :primary-key :not-null]
    [:reason [:varchar 50] :not-null]
    [:trigger-kind [:varchar 50] :not-null]
    [:trigger-id [:varchar 50] :not-null]
    [[:foreign-key :seq-no] [:references event-table :seq-no]]]})

(defn- create-effect-table-trigger-index
  [relation]
  {:create-index [(keyword (str (name relation) "-trigger-idx"))
                  [relation :trigger-kind :trigger-id]]})

(defn- create-trigger-table
  [trigger-table event-table]
  {:create-table [trigger-table :if-not-exists]
   :with-columns
   [[:trigger-kind [:varchar 50] :not-null]
    [:trigger-id [:varchar 50] :not-null]
    [:seq-no :int :not-null]
    [[:primary-key :trigger-kind :trigger-id :seq-no]]
    [[:foreign-key :seq-no] [:references event-table :seq-no]]]})

(defn create-tables
  [{sql :sql {:keys [event action effect trigger]} :tables}]
  (mapv #(sql/format % sql) [(create-event-table event)
                             (create-action-table action event)
                             (create-action-table-aggregate-index action)
                             (create-effect-table effect event)
                             (create-effect-table-trigger-index effect)
                             (create-trigger-table trigger event)]))