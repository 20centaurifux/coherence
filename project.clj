(defproject coherence "0.1.0-SNAPSHOT"
  :description "Event store written in Clojure."
  :url "https://github.com/20centaurifux/coherence"
  :license {:name "AGPLv3"
            :url "https://www.gnu.org/licenses/agpl-3.0"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [com.rpl/defexception "0.2.5"]
                 [de.dixieflatline/pold "0.1.0-SNAPSHOT"]
                 [org.clojure/test.check "1.1.1"]
                 [com.github.seancorfield/next.jdbc "1.3.939"]
                 [com.github.seancorfield/honeysql "2.6.1147"]
                 [meander/epsilon "0.0.650"]]
  :main ^:skip-aot coherence.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :test {:dependencies [[org.xerial/sqlite-jdbc "3.47.1.0"]
                                   [tortue/spy "2.15.0"]]}}
  :plugins [[dev.weavejester/lein-cljfmt "0.12.0"]])