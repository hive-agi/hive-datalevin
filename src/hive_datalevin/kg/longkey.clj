;; Copyright (C) 2026 Pedro Gomes Branquinho (BuddhiLW) <pedrogbranquinho@gmail.com>
;;
;; SPDX-License-Identifier: AGPL-3.0-or-later

(ns hive-datalevin.kg.longkey
  "LMDB key-size audit for a Datalevin store.

   A string value longer than `safe-value-length` is stored and pullable but
   cannot be bound by value in a `:where` clause, and cannot be looked up as a
   `:db.unique/identity`. Datalevin reports no error — the query returns an
   empty result, which is indistinguishable from absence.

   Contract:
     (audit conn)      => {attr {:max-len n :values n :unique u :over-limit? b}}
     (violations conn) => the sub-map whose :over-limit? is true
     (value-safe? s)   => whether one value can be matched by value"
  (:require [datalevin.core :as dl]))

;; =============================================================================
;; Limits
;; =============================================================================

(def ^:const max-key-size
  "LMDB's hard key limit in bytes, as compiled into Datalevin."
  511)

(def ^:const safe-value-length
  "Longest string that is still matchable by value. Measured 2026-08-20 on
   datalevin 0.10.18 by bisection: 497 matches, 498 does not. Independent of
   the attribute's name length — the key carries an integer attribute id."
  497)

(defn value-safe?
  "True when `v` is short enough to be bound by value in a :where clause."
  [v]
  (or (not (string? v))
      (<= (count v) safe-value-length)))

;; =============================================================================
;; Audit
;; =============================================================================

(defn string-attributes
  "Attributes declared :db.type/string, as {attr schema-map}."
  [conn]
  (into (sorted-map)
        (filter (fn [[_ v]] (= :db.type/string (:db/valueType v))))
        (dl/schema conn)))

(defn attribute-stats
  "Longest value and value count for one attribute. Returns nil when the
   attribute has no datoms."
  [db attr]
  (when-let [r (dl/q '[:find (max ?len) (count ?v) .
                       :in $ ?a
                       :where [_ ?a ?v] [(count ?v) ?len]]
                     db attr)]
    (let [[max-len values] r]
      (when max-len
        {:max-len max-len :values values}))))

(defn audit
  "Key-size report for every string attribute in `conn`.

   Each entry carries :max-len, :values, :unique (the :db/unique declaration,
   nil when not unique) and :over-limit? — true when the longest value cannot
   be matched by value."
  [conn]
  (let [db  (dl/db conn)
        sch (string-attributes conn)]
    (into (sorted-map)
          (keep (fn [[attr decl]]
                  (when-let [stats (attribute-stats db attr)]
                    [attr (assoc stats
                                 :unique (:db/unique decl)
                                 :over-limit? (> (:max-len stats) safe-value-length))])))
          sch)))

(defn violations
  "Attributes whose longest value exceeds `safe-value-length`."
  [conn]
  (into (sorted-map) (filter (comp :over-limit? val)) (audit conn)))

(defn queryable-violations
  "Violations that are also REACHED by value: unique-identity attributes, plus
   any attribute the caller names in `value-matched`. These are the ones that
   silently return nothing; a violation on a payload-only attribute is inert."
  [conn value-matched]
  (let [matched (set value-matched)]
    (into (sorted-map)
          (filter (fn [[attr v]] (or (:unique v) (contains? matched attr))))
          (violations conn))))
