(ns coherence.test-utils
  (:require [clojure.spec.alpha :as s]
            [coherence.specs]))

(def event-gen
  (s/gen :coherence.specs/event))

(def action-gen
  (s/gen :coherence.specs.event/action))

(def effect-gen
  (s/gen :coherence.specs.event/effect))

(defn now
  []
  (-> (java.time.Instant/now)
      .getEpochSecond
      java.time.Instant/ofEpochSecond))