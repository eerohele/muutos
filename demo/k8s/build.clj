(ns build
  (:require [clojure.tools.build.api :as build]))

(def lib 'my/app)
(def version "1.0")
(def class-dir "target/classes")
(def uber-file (format "target/%s-%s-standalone.jar" (name lib) version))

(def basis (delay (build/create-basis {:project "deps.edn"})))

(defn clean [_]
  (build/delete {:path "target"}))

(defn uber [_]
  (clean nil)
  (build/copy-dir {:src-dirs ["src"]
                   :target-dir class-dir})
  (build/compile-clj {:basis @basis
                      :ns-compile '[app.main]
                      :class-dir class-dir})
  (build/uber {:class-dir class-dir
               :uber-file uber-file
               :basis @basis
               :main 'app.main}))

(defn image [_]
  (uber nil)
  (build/process {:command-args ["docker" "build" "--tag" (format "app:%s" version) "."]}))
