(ns kba-index-discovery.download-landing-pages
  "Download landing pages"
  (:require [clj-http.client :as client]
            [clj-http.cookies :as cookies]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]])
  (:import [java.io PushbackReader]))

(def index-stuff "links-indices.clj")

(def cookie-store (cookies/cookie-store))

(defn get-corpora
  [wrtr]
  (let [rdr (-> index-stuff
                io/reader
                (PushbackReader.))

        index-stuff 
        (filter
         (fn [[uri index-names]]
           (-> index-names
               empty?
               not))
         (read rdr))]
    (doall
     (doseq [[uri index-names] index-stuff]
       (println uri)
       (let [_    (try (client/get uri {:throw-exceptions false
                                        :cookie-store cookie-store
                                        :conn-timeout 5000})
                       (catch Exception e {}))
             resp (try (client/get uri {:throw-exceptions false
                                        :cookie-store cookie-store
                                        :conn-timeout 5000})
                       (catch Exception e {}))]
         (do (println :downloading uri)
             (pprint
              (merge resp
                     {:index-names index-names})
              wrtr)
             (flush)))))))
