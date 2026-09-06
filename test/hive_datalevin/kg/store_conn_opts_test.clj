(ns hive-datalevin.kg.store-conn-opts-test
  "`:conn-opts` reaches datalevin.core/get-conn verbatim, with the host-level
   :cache-limit layered on top. The host uses it to turn off datalevin's
   background statistics sampler on write-heavy slots."
  (:require [clojure.test :refer [deftest is testing]]
            [datalevin.core :as dtlv]
            [hive-spi.kg.protocol :as kg]
            [hive-datalevin.kg.store :as store]))

(defn- tmp-path []
  (str (System/getProperty "java.io.tmpdir") "/hive-dl-conn-opts-" (random-uuid)))

(defn- capture-get-conn-opts
  "Run `f` with dtlv/get-conn stubbed; returns the opts map it was called with."
  [f]
  (let [seen (atom nil)]
    (with-redefs [dtlv/get-conn (fn [_dir _schema opts]
                                  (reset! seen opts)
                                  (atom {}))]
      (f))
    @seen))

(deftest conn-opts-are-forwarded-to-get-conn
  (testing "conn-opts keys pass through and :cache-limit is layered on top"
    (let [s    (store/create-store {:db-path     (tmp-path)
                                    :cache-limit 7
                                    :conn-opts   {:background-sampling? false}})
          opts (capture-get-conn-opts #(kg/ensure-conn! s))]
      (is (= {:background-sampling? false :cache-limit 7} opts))))
  (testing "no conn-opts and no cache-limit => empty opts, as before"
    (let [s    (store/create-store {:db-path (tmp-path)})
          opts (capture-get-conn-opts #(kg/ensure-conn! s))]
      (is (= {} opts)))))
