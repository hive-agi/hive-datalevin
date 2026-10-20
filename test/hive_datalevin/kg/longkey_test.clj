(ns hive-datalevin.kg.longkey-test
  "Characterises the LMDB key-size cliff and pins the audit that detects it.

   The first deftest is a CHARACTERIZATION test of Datalevin itself: if a
   future version starts matching long values, it fails and tells us the
   guard can be relaxed."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as dl]
            [hive-datalevin.kg.longkey :as lk]))

(def ^:private schema
  {:n/id   {:db/valueType :db.type/string :db/unique :db.unique/identity}
   :n/text {:db/valueType :db.type/string}
   :n/kw   {:db/valueType :db.type/keyword}})

(defn- with-conn [f]
  (let [dir  (str (System/getProperty "java.io.tmpdir") "/dl-longkey-" (System/nanoTime))
        conn (dl/get-conn dir schema)]
    (try (f conn) (finally (dl/close conn)))))

(defn- body [n] (apply str (repeat n "x")))

;; =============================================================================
;; The engine behaviour the guard exists for
;; =============================================================================

;; Datalevin <= 1.1 had an LMDB key cliff: strings past 497 chars were stored
;; and pullable but unmatchable by value and unfindable as a unique identity.
;; Re-measured 2026-10-20 on datalevin 1.2.5: values up to 1,000,000 chars
;; match by value and long unique identities resolve. These characterization
;; tests pin that; if they fail, the engine regressed to a key cliff and the
;; audit below is load-bearing again: re-measure safe-value-length.

(deftest long-values-match-by-value-test
  (with-conn
    (fn [conn]
      (doseq [n [100 lk/safe-value-length (inc lk/safe-value-length) 1000 8000]]
        (dl/transact! conn [{:n/id (str "v" n) :n/text (body n)}]))
      (let [db      (dl/db conn)
            match   (fn [n] (some? (dl/q '[:find ?e . :in $ ?v :where [?e :n/text ?v]] db (body n))))
            pullable (fn [n id] (= n (count (dl/q '[:find ?t . :in $ ?id
                                                    :where [?e :n/id ?id] [?e :n/text ?t]] db id))))]
        (testing "every length round-trips and matches by value"
          (doseq [n [100 lk/safe-value-length (inc lk/safe-value-length) 1000 8000]]
            (is (pullable n (str "v" n)) (str n " chars must round-trip"))
            (is (match n)
                (str n " chars did not match by value; datalevin has a key"
                     " cliff again, re-measure safe-value-length"))))))))

(deftest unique-identity-past-the-old-limit-is-findable-test
  (with-conn
    (fn [conn]
      (let [short-id (body 100)
            long-id  (body 600)]
        (dl/transact! conn [{:n/id short-id :n/kw :a}])
        (dl/transact! conn [{:n/id long-id  :n/kw :b}])
        (let [db (dl/db conn)
              found (fn [id] (some? (dl/q '[:find ?e . :in $ ?id :where [?e :n/id ?id]] db id)))]
          (is (found short-id))
          (is (found long-id)
              "a unique-identity string past the old limit is found by its own identity"))))))

;; =============================================================================
;; The guard
;; =============================================================================

(deftest value-safe?-test
  (is (lk/value-safe? (body lk/safe-value-length)))
  (is (not (lk/value-safe? (body (inc lk/safe-value-length)))))
  (testing "non-strings are never key-limited"
    (is (lk/value-safe? :a-keyword))
    (is (lk/value-safe? 12345))
    (is (lk/value-safe? nil))))

(deftest audit-finds-only-the-over-limit-attribute-test
  (with-conn
    (fn [conn]
      (dl/transact! conn [{:n/id "a" :n/text (body 100)}
                          {:n/id "b" :n/text (body 8000)}])
      (let [report (lk/audit conn)]
        (testing "every string attribute with datoms is reported"
          (is (contains? report :n/text))
          (is (contains? report :n/id)))
        (testing "the long-text attribute is flagged, the short id is not"
          (is (true? (:over-limit? (:n/text report))))
          (is (= 8000 (:max-len (:n/text report))))
          (is (= 2 (:values (:n/text report))))
          (is (false? (:over-limit? (:n/id report)))))
        (testing "the unique declaration is carried through"
          (is (= :db.unique/identity (:unique (:n/id report))))
          (is (nil? (:unique (:n/text report)))))
        (testing "violations is exactly the flagged subset"
          (is (= #{:n/text} (set (keys (lk/violations conn))))))))))

(deftest queryable-violations-separates-inert-from-dangerous-test
  (with-conn
    (fn [conn]
      (dl/transact! conn [{:n/id "a" :n/text (body 8000)}])
      (testing "a long PAYLOAD-only attribute is a violation but inert"
        (is (= #{:n/text} (set (keys (lk/violations conn)))))
        (is (empty? (lk/queryable-violations conn #{}))))
      (testing "naming it as value-matched makes it dangerous"
        (is (= #{:n/text} (set (keys (lk/queryable-violations conn #{:n/text})))))))))

(deftest audit-skips-attributes-with-no-datoms-test
  (with-conn
    (fn [conn]
      (dl/transact! conn [{:n/id "only-id"}])
      (let [report (lk/audit conn)]
        (is (contains? report :n/id))
        (is (not (contains? report :n/text))
            "an attribute with no datoms has no measurable max length")))))
