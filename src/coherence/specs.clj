(ns coherence.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.test.check.generators :as gen]))

;;; common

(s/def ::seq-no (s/and integer? pos?))

(s/def ::timestamp
  (s/with-gen #(instance? java.time.Instant %)
    (fn []
      (let [now (.getEpochSecond (java.time.Instant/now))]
        (gen/fmap #(java.time.Instant/ofEpochSecond %)
                  (gen/large-integer* {:min (- now (* 7 24 60 60))
                                       :max now}))))))

(s/def ::kind keyword?)

(s/def ::id (s/or :int integer?
                  :uuid uuid?
                  :string string?))

(s/def ::identity (s/tuple ::kind ::id))

(s/def ::trigger ::identity)

(s/def ::triggers (s/coll-of ::trigger
                             :distinct true
                             :min-count 1))

(s/def ::reason ::kind)

;;; action

(s/def :coherence.specs.patch/key (s/or :keyword keyword?
                                        :keywords (s/coll-of keyword?
                                                             :min-count 1)))

(s/def :coherence.specs.patch/association (s/map-of :coherence.specs.patch/key
                                                    any?
                                                    :min-count 1))

(s/def :coherence.specs.patch/dissociation (s/coll-of :coherence.specs.patch/key
                                                      :distinct true
                                                      :min-count 1))

(s/def ::patch (s/keys :req-un [(or :coherence.specs.patch/association
                                    :coherence.specs.patch/dissociation)]))

(s/def :coherence.specs.action/actor ::identity)

(s/def :coherence.specs.action/aggregate ::identity)

(s/def ::action (s/keys :req-un [::reason
                                 :coherence.specs.action/actor
                                 :coherence.specs.action/aggregate
                                 ::patch]))

;;; effect

(s/def ::effect (s/keys :req-un [::reason ::trigger]))

;;; event

(s/def ::source keyword?)

(s/def :coherence.specs.event/action (s/keys :req-un [::seq-no
                                                      ::timestamp
                                                      ::source
                                                      ::action]
                                             :opt-un [::triggers]))

(s/def :coherence.specs.event/effect (s/keys :req-un [::seq-no
                                                      ::timestamp
                                                      ::source
                                                      ::effect]))

(s/def ::event (s/or :action :coherence.specs.event/action
                     :effect :coherence.specs.event/effect))