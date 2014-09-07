(ns kba-index-discovery.core
  (:require [clojure.java.io :as io]
            [clojure.set :as clj-set])
  (:import [java.io PushbackReader])
  (:use [clojure.pprint :only [pprint]]))

(defn load-index-names-file
  [a-file]
  (let [rdr (-> a-file io/reader PushbackReader.)
        records (take-while
                 identity
                 (repeatedly
                  (fn []
                    (try
                      (read rdr)
                      (catch Exception e nil)))))

        res
        (reduce
         (fn [acc {uri        :uri
                  index-name :index-name}]
           (merge-with clj-set/union
                       acc
                       {uri (set index-name)}))
         {}
         records)

        wrtr (-> (str a-file ".records")
                 io/writer)]
    (pprint res wrtr)))

(defn merge-records
  [records]
  (reduce
   (fn [acc r]
     (let [rdr (-> r io/reader PushbackReader.)]
      (merge-with clj-set/union
                  acc
                  (read rdr))))
   {}
   records))

(defn -main
  [& args]
  (load-index-names-file
   (first args)))
