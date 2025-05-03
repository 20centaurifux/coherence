# coherence

## Introduction

It's a common scenario that actors interact with a system simultaneously and separately from each other for arbitrarily long periods of time.

An interaction takes place in two isolated steps:

1. The current state of an entity or aggregate is read.
2. At least one action is performed that changes the state of a previously read entity or aggregate.

If access is synchronized by a locking mechanism, actors hinder each other. Without such a mechanism, operations can lead to conflicts if the same entity or aggregate is changed. This can be the reason for data loss and inconsistency.

**coherence** addresses this problem by storing state transformations as immutable events in an event store. If an event is appended, it can be determined whether it causes a conflict and this can be resolved manually or automatically.

**coherence** follows the command-query separation (CQS) principle. Events are read from the event store and projected into separate read models.

Events have a *sequence number* to preserve order. To trace individual state changes an *event source* and *timestamp* is stored.

## Installation

[![Clojars Project](https://img.shields.io/clojars/v/de.dixieflatline/coherence.svg?include_prereleases)](https://clojars.org/de.dixieflatline/coherence)

## Walkthrough

**coherence** comes with a JDBC implementation for storing and retrieving events. A JDBC driver for a database compatible with [HoneySQL](https://github.com/seancorfield/honeysql) is required.

The example below initializes a SQLite database (which requires the org.xerial/sqlite-jdbc driver).

```
(use 'coherence.core)
(use 'coherence.jdbc.core)

(def honeysql-opts {:dialect :ansi
                    :quoted-snake true})

(def table-names {:event :events
                  :action :actions
                  :effect :effects
                  :trigger :triggers})

(def store-opts {:sql honeysql-opts
                 :tables table-names})

(def ds {:dbtype "sqlite" :dbname "test.db"})

(def store (->Store ds store-opts))

(coherence.jdbc.core/init-schema store)
```

### Actions

An *actor* has a *reason* for applying a *patch* that changes an *aggregate*. Patches may associate values with specified keys and dissociate keys from an aggregate.

```
;; Alice adds two books to the store
(rebase! store
         1
         [{:timestamp (java.time.Instant/now)
           :source :bookstore
           :action {:reason :create
                    :actor [:user "alice"]
                    :aggregate [:book "Neuromancer"]
                    :patch {:assoc {:author "William Gibson"
                                    :genre "SciFi"}}}}
           {:timestamp (java.time.Instant/now)
            :source :bookstore
            :action {:reason :create
                     :actor [:user "alice"]
                     :aggregate [:book "Count Zero"]
                     :patch {:assoc {:author "William Gibson"
                                     :genre "Cyberpunk"}}}}])

; => {:result :ok :events [{:seq-no 1 ...} {:seq-no 2 ...}]}
```

An *action* cannot be appended to the event store if the affected aggregate was changed in the meantime. The conflict is returned and the caller has to resolve it. When appending an *action* to the event store, a list of sequence numbers is passed that should be treated as resolved.

```
;; Bob adds two books to the store
(rebase! store
         1
         [{:timestamp (java.time.Instant/now)
           :source :bookstore
           :action {:reason :create
                    :actor [:user "bob"]
                    :aggregate [:book "Mona Lisa Overdrive"]
                    :patch {:assoc {:author "William Gibson"
                                    :genre "Cyberpunk"}}}}
           {:timestamp (java.time.Instant/now)
            :source :bookstore
            :action {:reason :create
                     :actor [:user "bob"]
                     :aggregate [:book "Neuromancer"]
                     :patch {:assoc {:author "William Gibson"
                                     :genre "Cyberpunk"}}}}])

; => {:result :conflict
;     :events [{:seq-no 3 ...}]
;     :conflict {:ours {:seq-no 4 ...}
;                :theirs {:seq-no 1 ...}}}

;; Bob resolves the conflict
(rebase! store
         1
         [{:timestamp (java.time.Instant/now)
           :source :bookstore
           :action {:reason :create
                    :actor [:user "bob"]
                    :aggregate [:book "Mona Lisa Overdrive"]
                    :patch {:assoc {:author "William Gibson"
                                    :genre "Cyberpunk"}}}}
          {:timestamp (java.time.Instant/now)
           :source :bookstore
           :action {:reason :update
                    :actor [:user "bob"]
                    :aggregate [:book "Neuromancer"]
                    :patch {:assoc {:genre "Cyberpunk"}}}}]
         :resolved [1])

; => {:result :ok :events [{:seq-no 3 ...} {:seq-no 4 ...}]}
```

### Effects & projection

Stored information may depend on external resources. Sometimes data must be made unreadable after a certain point in time due to data protection regulations, for example. In such a case the payload may be stored in encrypted form. The key required for decryption has an expiration date.

In **coherence** *actions* can be linked to any number of *triggers*, which in turn are triggered by an *effect* for a *reason*. If, for instance, a decryption key expires an *effect* event is appended to the event store. If an *effect* is read in during projection, the *actions* of all affected aggregates are replayed.

```
;; Alice stores two receipts with encrypted credit card numbers
(rebase! store
         2
         [{:timestamp (java.time.Instant/now)
           :source :bookstore
           :action {:reason :payment
                    :actor [:user "alice"]
                    :aggregate [:receipt 1]
                    :patch {:assoc {:no "avd2dlg3g93fm1osu"
                                    :total 100}}}
           :triggers [[:encryption-key 1]]}
          {:timestamp (java.time.Instant/now)
           :source :bookstore
           :action {:reason :payment
                    :actor [:user "alice"]
                    :aggregate [:receipt 2]
                    :patch {:assoc {:no "kepf62djsdk8fw21"
                                    :total 200}}}
           :triggers [[:encryption-key 2]]}])

; => {:result :ok :events [{:seq-no 5 ...} {:seq-no 6 ...}]}

;; the encryption key of the first receipt expires
(rebase! store
         6
         [{:timestamp (java.time.Instant/now)
           :source :keystore
           :effect {:reason :key-expired
                    :trigger [:encryption-key 1]}}])

; => {:result :ok :events [{:seq-no 7 ...}]}

;; receipt 1 is replayed
(def xf (map (fn [{seq-no :seq-no {:keys [:aggregate]} :action}]
               {:seq-no seq-no :aggregate aggregate})))

(stream-events xf conj [] store :offset 6)

; => [{:seq-no 5, :aggregate [:receipt 1]}
;     {:seq-no 6, :aggregate [:receipt 2]}]
```