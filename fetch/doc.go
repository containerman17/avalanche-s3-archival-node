// Package fetch ingests a chain's history over naked p2p: heights resolved to
// container IDs by PullQuery, containers pulled by GetAncestors, delivered to
// the executor strictly ascending out of a bounded RAM queue.
package fetch
