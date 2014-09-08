(ns kba-index-discovery.process-downloaded-corpus
  "Process the heritrix job in question"
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.string :as string]
            [net.cgrand.enlive-html :as html]
            (org.bovinegenius [exploding-fish :as uri])
            [subotai.warc.warc :as warc])
  (:import [java.io PushbackReader StringReader]))

(defn load-crawl-log
  [heritrix-job]
  (let [crawl-log-file (str heritrix-job "/latest/logs/crawl.log")
        crawl-log-lines (string/split-lines
                         (slurp crawl-log-file))]
    (reduce
     (fn [acc l]
       (let [split-line (string/split l #"\s+")

             timestamp (nth split-line 0)
             code      (nth split-line 1)
             link      (nth split-line 3)
             src-link  (nth split-line 5)]
         (merge acc {link {:code code
                           :src src-link}})))
     {}
     crawl-log-lines)))

(def index-stuff "links-indices.clj")

(defn load-index-names
  []
  (with-open [rdr (-> index-stuff
                      io/reader
                      (PushbackReader.))]
    (read rdr)))

(defn job-warcs
  [job-dir]
  (filter
   (fn [f]
     (and (re-find #".warc.gz" f)
          (not
           (re-find #"/latest/" f))))
   (map
    (fn [f]
      (.getAbsolutePath f))
    (file-seq
     (io/as-file job-dir)))))

(defn load-job-warcs
  [job-dir]
  (let [warc (first (job-warcs job-dir))

        stream (warc/warc-input-stream warc)
        records (warc/stream-html-records-seq stream)]
    (reduce
     (fn [acc record]
       (merge acc {(:warc-target-uri record)
                   (:payload record)}))
     records)))

(defn build-redirect-graph
  "Return a redirect graph"
  [heritrix-job]
  (let [crawl-log-file  (str heritrix-job "/latest/logs/crawl.log")
        crawl-log-lines (string/split-lines
                         (slurp crawl-log-file))]
    (reduce
     (fn [acc l]
       (let [split-line (string/split l #"\s+")
             
             timestamp (nth split-line 0)
             code      (nth split-line 1)
             link      (nth split-line 3)
             redir?    (re-find #"R" (nth split-line 4))
             src-link  (nth split-line 5)]
         (if redir?
           (merge acc {src-link link})
           acc)))
     {}
     crawl-log-lines)))

(defn resolve-redirects
  [a-uri redirect-graph]
  (if (nil? (get redirect-graph a-uri))
    a-uri
    (recur (get redirect-graph a-uri)
           redirect-graph)))

(defn acquire-index-pages
  [job-dir]
  (let [payloads    (load-job-warcs job-dir)
        crawl-log   (load-crawl-log job-dir)
        uri-indices (load-index-names)
        redirect-graph (build-redirect-graph job-dir)]
    (doseq [[url index-names] uri-indices]
      (when url
        (let [resolved-url (resolve-redirects url)
              payload (get payloads resolved-url)
             html-index (try (.indexOf payload "<")
                             (catch Exception e -1))
             html-content (if (neg? html-index)
                            nil
                            (subs payload
                                  html-index))

             links
             (when html-content
               (-> html-content
                   (StringReader.)
                   html/html-resource
                   (html/select [:a])))]
         (doseq [index-page-link
                 (map
                  #(try (->> % :attrs :href (uri/resolve-uri url))
                        (catch Exception e nil))
                  (filter
                   (fn [a-tag]
                     (some #{(html/text a-tag)}
                           index-names))
                   links))]
           (println index-page-link)))))))
