;; Licensed to the Apache Software Foundation (ASF) under one
;; or more contributor license agreements.  See the NOTICE file
;; distributed with this work for additional information
;; regarding copyright ownership.  The ASF licenses this file
;; to you under the Apache License, Version 2.0 (the
;; "License"); you may not use this file except in compliance
;; with the License.  You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.
(ns backtype.storm.config
  (:import [java.io FileReader File])
  (:import [backtype.storm Config ConfigValidation$FieldValidator])
  (:import [backtype.storm.utils Utils LocalState])
  (:import [org.apache.commons.io FileUtils])
  (:require [clojure [string :as str]])
  (:use [backtype.storm util])
  )

(def RESOURCES-SUBDIR "resources")

(defn- clojure-config-name [name]
  (.replace (.toUpperCase name) "_" "-"))

;; define clojure constants for every configuration parameter
(doseq [f (seq (.getFields Config))]
  (let [name (.getName f)
        new-name (clojure-config-name name)]
    (eval
      `(def ~(symbol new-name) (. Config ~(symbol name))))
      ))

(def ALL-CONFIGS
  (dofor [f (seq (.getFields Config))]
         (.get f nil)
         ))

;; 定义多个方法[http://clojuredocs.org/clojure_core/clojure.core/defmulti] (defmulti name dispatch-fn & options)
(defmulti get-FieldValidator class-selector)

;; 类型为nil
(defmethod get-FieldValidator nil [_]
  (throw (IllegalArgumentException. "Cannot validate a nil field.")))

;; 类型直接是FieldValidator, 直接使用
(defmethod get-FieldValidator
  ConfigValidation$FieldValidator [validator] validator)

;; 类型为Object, 比如String等基本类型, klass=java.lang.String
(defmethod get-FieldValidator Object [klass]
  {:pre [(not (nil? klass))]}
  (reify ConfigValidation$FieldValidator
    (validateField [this name v]          ; String name, Object field
      (if (and (not (nil? v))             ; 如果v不为空, 且不是klass类型, 抛出异常
               (not (instance? klass v))) ; 确保FIELD=field, FIELD_SCHEMA=klass这一对配置项满足: (instance? klass value)
        (throw (IllegalArgumentException.
                 (str "field " name " '" v "' must be a '" (.getName klass) "'")))))))

;; Create a mapping of config-string -> validator
;; Config fields must have a _SCHEMA field defined
(def CONFIG-SCHEMA-MAP
  (->> (.getFields Config)            ; 调用Config.class.getFields()获取所有字段  getFields是java.lang.Class类的方法. 返回值为Field[]
          ; 正则表达式 (re-matches #".*_SCHEMA$" "STORM_SCHEMA") 返回STORM_SCHEMA, 加上not进行过滤后
          ; 把字段名字以_SCHEMA结尾的过滤掉(not). 返回值仍然是Field[], 作为后面map表达式的最后一个Item: (map func colls)
          (filter #(not (re-matches #".*_SCHEMA$" (.getName %))))
          ; 现在colls只剩下FIELD的字段了. FIELD_SCHEMA的被过滤掉了
          ; func的返回值是一个[key value]的数组. key为字段值(不含_SCHEMA的字段的值), value为该字段的校验器(通过_SCHEMA字段获取其返回值:class类型)
          (map (fn [f] [(.get f nil) (get-FieldValidator
                                       (-> Config
                                         ; 拼装FIELD_SCHEMA的字段, 主要是为了获得指定的类型
                                         (.getField (str (.getName f) "_SCHEMA")) ; Config.class.getField(fieldName) => Field field
                                         (.get nil)))])           ; field.get(null), 注意Field没有get()方法, 参数必须是Object
          ; 假设SCHEMA字段为Object XXX=String.class, 结果为class java.lang.String
          )  ; (map fun Field[])
          (into {}) ; 上面->> colls (map f)的返回值作为这里的最后一个Item, 即转换为Map类型. 因为->> colls (map f)的返回值是数组类型
  ))

(defn cluster-mode [conf & args]
  (keyword (conf STORM-CLUSTER-MODE)))

(defn local-mode? [conf]
  (let [mode (conf STORM-CLUSTER-MODE)]
    (condp = mode
      "local" true
      "distributed" false
      (throw (IllegalArgumentException.
                (str "Illegal cluster mode in conf: " mode)))
      )))

(defn sampling-rate [conf]
  (->> (conf TOPOLOGY-STATS-SAMPLE-RATE)
       (/ 1)
       int))

(defn mk-stats-sampler [conf]
  (even-sampler (sampling-rate conf)))

; storm.zookeeper.servers:
;     - "server1"
;     - "server2"
;     - "server3"
; nimbus.host: "master"
; 
; ########### These all have default values as shown
; 
; ### storm.* configs are general configurations
; # the local dir is where jars are kept
; storm.local.dir: "/mnt/storm"
; storm.zookeeper.port: 2181
; storm.zookeeper.root: "/storm"

(defn read-default-config []
  (clojurify-structure (Utils/readDefaultConfig)))

(defn validate-configs-with-schemas [conf]
  (doseq [[k v] conf
         :let [schema (CONFIG-SCHEMA-MAP k)]]
    (if (not (nil? schema))
      (.validateField schema k v))))

(defn read-storm-config []
  (let [
        conf (clojurify-structure (Utils/readStormConfig))]
    (validate-configs-with-schemas conf)
    conf))

(defn read-yaml-config [name]
  (let [conf (clojurify-structure (Utils/findAndReadConfigFile name true))]
    (validate-configs-with-schemas conf)
    conf))

;; ----------------------------------------
;; -------------nimbus---------------------
;; ----------------------------------------

;; ${storm.local.dir}/nimbus 其中storm.local.dir配置在storm.yaml中
(defn master-local-dir [conf]
  (let [ret (str (conf STORM-LOCAL-DIR) file-path-separator "nimbus")]
    (FileUtils/forceMkdir (File. ret))
    ret
    ))

(defn master-stormdist-root
  ([conf]
     ; ${storm.local.dir}/nimbus/stormdist
     (str (master-local-dir conf) file-path-separator "stormdist"))
  ([conf storm-id]
     ; 调用上面一个参数. 最终形成${storm.local.dir}/nimbus/stormdist/storm-id
     (str (master-stormdist-root conf) file-path-separator storm-id)))

(defn master-stormjar-path [stormroot]
  (str stormroot file-path-separator "stormjar.jar"))

(defn master-stormcode-path [stormroot]
  (str stormroot file-path-separator "stormcode.ser"))

(defn master-stormconf-path [stormroot]
  (str stormroot file-path-separator "stormconf.ser"))

(defn master-inbox [conf]
  (let [ret (str (master-local-dir conf) file-path-separator "inbox")]
    (FileUtils/forceMkdir (File. ret))
    ret ))

(defn master-inimbus-dir [conf]
  (str (master-local-dir conf) file-path-separator "inimbus"))

;; ----------------------------------------
;; -------------supervisor-----------------
;; ----------------------------------------

;; ${storm.local.dir}/supervisor
(defn supervisor-local-dir [conf]
  (let [ret (str (conf STORM-LOCAL-DIR) file-path-separator "supervisor")]
    (FileUtils/forceMkdir (File. ret))
    ret
    ))

;; ${storm.local.dir}/supervisor/isupervisor
(defn supervisor-isupervisor-dir [conf]
  (str (supervisor-local-dir conf) file-path-separator "isupervisor"))

(defn supervisor-stormdist-root
  ;; ${storm.local.dir}/supervisor/stormdist
  ([conf] (str (supervisor-local-dir conf) file-path-separator "stormdist"))
  ;; ${storm.local.dir}/supervisor/stormdist/storm-id
  ([conf storm-id]
      (str (supervisor-stormdist-root conf) file-path-separator (java.net.URLEncoder/encode storm-id))))

;; ${storm.local.dir}/supervisor/stormdist/storm-id/stormjar.jar
;; stormroot = ${storm.local.dir}/supervisor/stormdist/storm-id
(defn supervisor-stormjar-path [stormroot]
  (str stormroot file-path-separator "stormjar.jar"))

(defn supervisor-stormcode-path [stormroot]
  (str stormroot file-path-separator "stormcode.ser"))

(defn supervisor-stormconf-path [stormroot]
  (str stormroot file-path-separator "stormconf.ser"))

;; ${storm.local.dir}/supervisor/tmp
(defn supervisor-tmp-dir [conf]
  (let [ret (str (supervisor-local-dir conf) file-path-separator "tmp")]
    (FileUtils/forceMkdir (File. ret))
    ret ))

;; ${storm.local.dir}/supervisor/stormdist/storm-id/resources
(defn supervisor-storm-resources-path [stormroot]
  (str stormroot file-path-separator RESOURCES-SUBDIR))

;; ${storm.local.dir}/supervisor/localstate
(defn ^LocalState supervisor-state [conf]
  (LocalState. (str (supervisor-local-dir conf) file-path-separator "localstate")))

(defn read-supervisor-storm-conf [conf storm-id]
  (let [stormroot (supervisor-stormdist-root conf storm-id) ; ${storm.local.dir}/supervisor/stormdist/storm-id
        conf-path (supervisor-stormconf-path stormroot)     ; ${storm.local.dir}/supervisor/stormdist/storm-id/stormconf.ser
        topology-path (supervisor-stormcode-path stormroot)]
    (merge conf (Utils/deserialize (FileUtils/readFileToByteArray (File. conf-path))))
    ))

(defn read-supervisor-topology [conf storm-id]
  (let [stormroot (supervisor-stormdist-root conf storm-id)
        topology-path (supervisor-stormcode-path stormroot)]  ; ${storm.local.dir}/supervisor/stormdist/storm-id/stormcode.ser
    (Utils/deserialize (FileUtils/readFileToByteArray (File. topology-path)))
    ))


;; --------------------------worker------------------------------
(defn worker-root
  ([conf]
     ; ${storm.local.dir}/workers
     (str (conf STORM-LOCAL-DIR) file-path-separator "workers"))
  ([conf id]
     ; ${storm.local.dir}/workers/$worker-id
     (str (worker-root conf) file-path-separator id)))

; ${storm.local.dir}/workers/$worker-id/pids
(defn worker-pids-root
  [conf id]
  (str (worker-root conf id) file-path-separator "pids"))

; ${storm.local.dir}/workers/$worker-id/pids/$pid
(defn worker-pid-path [conf id pid]
  (str (worker-pids-root conf id) file-path-separator pid))

; ${storm.local.dir}/workers/$worker-id/heartbeats
(defn worker-heartbeats-root
  [conf id]
  (str (worker-root conf id) file-path-separator "heartbeats"))

;; workers heartbeat here with pid and timestamp
;; if supervisor stops receiving heartbeat, it kills and restarts the process
;; in local mode, keep a global map of ids to threads for simulating process management
; ${storm.local.dir}/workers/$worker-id/heartbeats/$timestamp
(defn ^LocalState worker-state  [conf id]
  (LocalState. (worker-heartbeats-root conf id)))
