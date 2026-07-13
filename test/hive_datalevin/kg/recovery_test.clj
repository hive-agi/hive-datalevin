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
