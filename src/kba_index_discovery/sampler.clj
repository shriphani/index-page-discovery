(ns kba-index-discovery.sampler
  "Sample a link from w/e we downloaded"
  (:require [clj-http.client :as client]
            [clj-http.cookies :as cookies]
            [clojure.java.io :as io]
            [clojure.string :as string]
            (org.bovinegenius [exploding-fish :as uri]))
  (:use [clojure.pprint :only [pprint]]))

(defn group-uris
  [index-pages-file]
  (group-by
   uri/host
   (string/split-lines
    (slurp index-pages-file))))

(def out-dir "/bos/tmp19/spalakod/index-pages/")

(def cookie-store (cookies/cookie-store))

(defn sample
  [index-pages-file]
  (let [by-host (group-uris index-pages-file)]
    (doall
     (pmap
      (fn [[h links]]
        (let [out-file (str out-dir h ".pages")
              out-handle (io/writer out-file :append true)]
          (doseq [l links]
            (println :downloading l)
            (let [body (:body
                        (client/get l {:throw-exceptions false
                                       :cookie-store cookie-store
                                       :conn-timeout 5000}))]
              (Thread/sleep (+ 1500
                               (rand-int 5000)))
              (pprint {:uri  l
                       :body body}
                      out-handle)))))
      by-host))))
