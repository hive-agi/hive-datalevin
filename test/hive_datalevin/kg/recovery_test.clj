(ns hive-datalevin.kg.recovery-test
  (:require [clojure.test :refer [deftest is testing]]
            [hive-datalevin.kg.recovery :as rec]))
;; SPDX-License-Identifier: MIT

(deftest classify-open-failure-test
  (testing "corruption signatures -> :wal-corrupt"
    (is (= :wal-corrupt (rec/classify-open-failure (Exception. "MDB_CORRUPTED page"))))
    (is (= :wal-corrupt (rec/classify-open-failure (Exception. "Invalid txn-log record magic")))))
  (testing "lock signatures -> :lock-contention"
    (is (= :lock-contention (rec/classify-open-failure (Exception. "Resource temporarily unavailable")))))
  (testing "version signatures -> :version-mismatch"
    (is (= :version-mismatch (rec/classify-open-failure (Exception. "MDB_VERSION_MISMATCH")))))
  (testing "unclassified -> :unknown"
    (is (= :unknown (rec/classify-open-failure (Exception. "totally unrelated failure")))))
  (testing "walks the cause chain"
    (is (= :wal-corrupt
           (rec/classify-open-failure (Exception. "wrapper" (Exception. "MDB_CORRUPTED")))))))

(deftest quarantine-path-test
  (testing "deterministic given a timestamp"
    (is (= "/db/x.corrupt.42" (rec/quarantine-path "/db/x" 42))))
  (testing "live form stamps a numeric suffix"
    (is (re-find #"\.corrupt\.\d+$" (rec/quarantine-path "/db/x")))))

(deftest heal-and-open!-throw-policy-test
  (testing ":throw policy surfaces the open failure after exhausting attempts"
    (let [boom (fn [] (throw (Exception. "MDB_CORRUPTED")))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (rec/heal-and-open! {:policy {:strategy :throw :max-attempts 2}
                                        :db-path "/tmp/none"}
                                       boom)))))
  (testing "open-fn that succeeds returns its value"
    (is (= :conn (rec/heal-and-open! {:db-path "/tmp/none"} (fn [] :conn))))))

(defn- counting-open-fn
  "0-arg thunk that always throws `msg`, plus the atom counting its calls."
  [msg]
  (let [calls (atom 0)]
    [(fn [] (swap! calls inc) (throw (Exception. ^String msg))) calls]))

;; An always-retry strategy function: the injected policy stub that makes the
;; non-retryable rule observable without redefining anything.
(defn- always-retry [_classification _db-path _ex] :retry)

(deftest heal-and-open!-never-retries-unknown-test
  (testing "an :unknown failure opens exactly once even when the policy says :retry"
    (let [[open-fn calls] (counting-open-fn "DBI datalevin/eav is not open")
          ex (try (rec/heal-and-open! {:policy {:strategy always-retry :max-attempts 3}
                                       :db-path "/tmp/none"}
                                      open-fn)
                  nil
                  (catch clojure.lang.ExceptionInfo e e))]
      (is (= 1 @calls))
      (is (= {:classification :unknown :retryable? false :err :storage/open-aborted}
             (select-keys (ex-data ex) [:classification :retryable? :err])))))
  (testing "a classified failure still honours the policy's :retry"
    (let [[open-fn calls] (counting-open-fn "Resource temporarily unavailable")]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"exhausted"
                            (rec/heal-and-open! {:policy {:strategy always-retry :max-attempts 3}
                                                 :db-path "/tmp/none"}
                                                open-fn)))
      (is (= 3 @calls)))))

(deftest retryable-classification?-test
  (is (false? (rec/retryable-classification? :unknown)))
  (is (every? rec/retryable-classification? [:wal-corrupt :lock-contention :version-mismatch])))
