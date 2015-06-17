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
(ns backtype.storm.LocalCluster
  (:use [backtype.storm testing config])      ;; 使用backtype/storm/testing.clj 和 config.clj
  (:import [java.util Map])
  (:gen-class
   :init init
   :implements [backtype.storm.ILocalCluster] ;; 接口为ILocalCluster
   :constructors {[] [] [java.util.Map] []}
   :state state ))  ;; init()的返回值会赋值给state.

(defn -init
  ;; 第一种情况参数是[]. 类似init()
  ([]
    (let [ret (mk-local-storm-cluster :daemon-conf {TOPOLOGY-ENABLE-MESSAGE-TIMEOUTS true})]
      [[] ret]		;; 返回值必须是 [ [superclass-constructor-args] state] 父类的参数类型都是[].  所以开头的:state state会被赋值为ret!
      ))
  ;; 第二种情况参数是[Map], 可以理解是参数个数不同的构造函数重载, 类似init(Map)
  ([^Map stateMap]
    [[] stateMap]))

(defn -submitTopology [this name conf topology]   ;; 第一个参数表示LocalCluster
  ;; (:nimbus map)获取map中:nimbus的值. 我们知道init的返回值是个Map, 其中包括了:nimbus这个key. 所以state就是ret!
  (submit-local-topology (:nimbus (. this state)) ;; :nimbus是init返回值ret这个Map中key=:nimbus的Nimbus对象
                      name
                      conf
                      topology))                  ;; 和StormSubmitter.submitTopology的参数一样: 计算拓扑的名字, 配置, StormTopology

(defn -submitTopologyWithOpts [this name conf topology submit-opts]
  (submit-local-topology-with-opts (:nimbus (. this state))  ;; this.state 即gen-class的:state
                      name
                      conf
                      topology
                      submit-opts))

(defn -shutdown [this]
  (kill-local-storm-cluster (. this state)))

(defn -killTopology [this name]
  (.killTopology (:nimbus (. this state)) name))

(defn -getTopologyConf [this id]
  (.getTopologyConf (:nimbus (. this state)) id))

(defn -getTopology [this id]
  (.getTopology (:nimbus (. this state)) id))

(defn -getClusterInfo [this]
  (.getClusterInfo (:nimbus (. this state))))

(defn -getTopologyInfo [this id]
  (.getTopologyInfo (:nimbus (. this state)) id))

(defn -killTopologyWithOpts [this name opts]
  (.killTopologyWithOpts (:nimbus (. this state)) name opts))

(defn -activate [this name]
  (.activate (:nimbus (. this state)) name))

(defn -deactivate [this name]
  (.deactivate (:nimbus (. this state)) name))

(defn -rebalance [this name opts]
  (.rebalance (:nimbus (. this state)) name opts))

(defn -getState [this]
  (.state this))

