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

TODO: Installation

## State transformation

### Actions

An *actor* has a *reason* for applying a *patch* that changes an *aggregate*. Patches may associate values with specified keys and dissociate keys from an aggregate.

TODO: example

An *action* cannot be appended to the event store if the affected aggregate was changed in the meantime. The conflicting event is returned and the caller has to resolve the conflict. When appending an *action* to the event store, a list of sequence numbers is passed that should be treated as resolved.

TODO: example

### Effects

Stored information may depend on external resources. Sometimes data must be made unreadable after a certain point in time due to data protection regulations, for example. In such a case the payload can be stored in encrypted form. The key required for decryption has an expiration date.

In **coherence** *actions* can be linked to any number of *triggers*, which in turn are triggered by an *effect* for a *reason*. If, for instance, a decryption key expires an *effect* event is appended to the event store. If an *effect* is read in during projection, the *actions* of all affected aggregates are replayed.

TODO: example

### Projection

Projectors receive events from an event stream to generate read models. The reading process can start at any offset. If an *effect* detected all *actions* of the affected aggregates up to the current sequence number are replayed.

TODO: example