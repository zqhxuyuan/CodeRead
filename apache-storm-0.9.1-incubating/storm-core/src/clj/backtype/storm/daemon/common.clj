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
(ns backtype.storm.daemon.common
  (:use [backtype.storm log config util])
  (:import [backtype.storm.generated StormTopology
            InvalidTopologyException GlobalStreamId])
  (:import [backtype.storm.utils Utils])
  (:import [backtype.storm.task WorkerTopologyContext])
  (:import [backtype.storm Constants])
  (:import [backtype.storm.metric SystemBolt])
  (:require [clojure.set :as set])  
  (:require [backtype.storm.daemon.acker :as acker])
  (:require [backtype.storm.thrift :as thrift])
  )

(defn system-id? [id]
  (Utils/isSystemId id))

(def ACKER-COMPONENT-ID acker/ACKER-COMPONENT-ID)
(def ACKER-INIT-STREAM-ID acker/ACKER-INIT-STREAM-ID)
(def ACKER-ACK-STREAM-ID acker/ACKER-ACK-STREAM-ID)
(def ACKER-FAIL-STREAM-ID acker/ACKER-FAIL-STREAM-ID)

(def SYSTEM-STREAM-ID "__system")

(def SYSTEM-COMPONENT-ID Constants/SYSTEM_COMPONENT_ID)
(def SYSTEM-TICK-STREAM-ID Constants/SYSTEM_TICK_STREAM_ID)
(def METRICS-STREAM-ID Constants/METRICS_STREAM_ID)
(def METRICS-TICK-STREAM-ID Constants/METRICS_TICK_STREAM_ID)

;; the task id is the virtual port
;; node->host is here so that tasks know who to talk to just from assignment
;; this avoid situation where node goes down and task doesn't know what to do information-wise
(defrecord Assignment [master-code-dir node->host executor->node+port executor->start-time-secs])


;; component->executors is a map from spout/bolt id to number of executors for that component
(defrecord StormBase [storm-name launch-time-secs status num-workers component->executors])

(defrecord SupervisorInfo [time-secs hostname assignment-id used-ports meta scheduler-meta uptime-secs])

(defprotocol DaemonCommon
  (waiting? [this]))

(def LS-WORKER-HEARTBEAT "worker-heartbeat")

;; LocalState constants
(def LS-ID "supervisor-id")
(def LS-LOCAL-ASSIGNMENTS "local-assignments")
(def LS-APPROVED-WORKERS "approved-workers")



(defrecord WorkerHeartbeat [time-secs storm-id executors port])

(defrecord ExecutorStats [^long processed
                          ^long acked
                          ^long emitted
                          ^long transferred
                          ^long failed])

(defn new-executor-stats []
  (ExecutorStats. 0 0 0 0 0))

(defn get-storm-id [storm-cluster-state storm-name]
  ; 集群中所有活动的storms: /storm/storms下的所有storm-id
  (let [active-storms (.active-storms storm-cluster-state)]
    (find-first   ; 类似于map函数的使用: (find-first fun[x]... colls)
      ; storm-base是序列化后的对象. 可以看做是一个Map. %是active-storms的每一个元素
      #(= storm-name (:storm-name (.storm-base storm-cluster-state % nil)))
      active-storms)
    ))

(defn topology-bases [storm-cluster-state]
  (let [active-topologies (.active-storms storm-cluster-state)]
    (into {} 
          (dofor [id active-topologies]
                 [id (.storm-base storm-cluster-state id nil)]
                 ))
    ))

(defn validate-distributed-mode! [conf]
  (if (local-mode? conf)
      (throw
        (IllegalArgumentException. "Cannot start server in local mode!"))))

;; 定义一个服务器函数的宏. 加上异常处理. 实际上就是一个defn
(defmacro defserverfn [name & body]
  `(let [exec-fn# (fn ~@body)]
    (defn ~name [& args#]
      (try-cause
        (apply exec-fn# args#)
      (catch InterruptedException e#
        (throw e#))
      (catch Throwable t#
        (log-error t# "Error on initialization of server " ~(str name))
        (halt-process! 13 "Error on initialization")
        )))))

;; 验证拓扑的id
(defn- validate-ids! [^StormTopology topology]
  ; sets中的每个元素都是一个Map. 每个Map都是topology的一个component.
  ; [<spout1-id, spout1> <bolt1-id, bolt2> <bolt2-id, bolt2> ]
  (let [sets (map #(.getFieldValue topology %) thrift/STORM-TOPOLOGY-FIELDS)
        ; 检测三种id(topo-id,spout-id,bolt-id)之间是否有交集. 这个是在一个计算拓扑中判断. 那么拓扑和拓扑之间的呢?
        offending (apply any-intersection sets)]    ; 将所有component组成一个集合进行处理, 而不是用doseq来处理.因为doseq只能一次处理一个!
    (if-not (empty? offending)
      (throw (InvalidTopologyException.
              (str "Duplicate component ids: " offending))))
    (doseq [f thrift/STORM-TOPOLOGY-FIELDS
            :let [obj-map (.getFieldValue topology f)]]   ; 对象map: <component-id, Component>
      (doseq [id (keys obj-map)]  ; component-id
        (if (system-id? id)                               ; 定义的component-id不能是系统级的id: 不能以__开头
          (throw (InvalidTopologyException.
                  (str id " is not a valid component id")))))
      (doseq [obj (vals obj-map)  ; component
              id (-> obj .get_common .get_streams keys)]  ; component对象的streams的id也不是以__开头
        (if (system-id? id)
          (throw (InvalidTopologyException.
                  (str id " is not a valid stream id"))))))
    ))

;; 获取某个计算拓扑的所有components. 其中spout和bolt都属于component.
(defn all-components [^StormTopology topology]
  (apply merge {}
         (for [f thrift/STORM-TOPOLOGY-FIELDS]
           (.getFieldValue topology f)                ; f返回值是Map, key为compnent-id, value为component对象
           )))

;; 某个component的配置信息, 定义在ComponentCommon这个struct的json_conf字段中
(defn component-conf [component]
  (->> component
      .get_common
      .get_json_conf                                  ; component.getCommon().getJsonConf()
      from-json))                                     ; 反序列化

(defn validate-basic! [^StormTopology topology]
  (validate-ids! topology)                            ; topology-id验证
  (doseq [f thrift/SPOUT-FIELDS                       ; StormTopology的SPOUTS和STATE_SPOUTS两个字段, 每次循环doseq操作赋给f
          obj (->> f (.getFieldValue topology) vals)] ; (vals topo.getFieldValue(f)) 右边是个Map. 所以obj是Map的Value, 比如SpoutSpec或者StateSpoutSpec
    (if-not (empty? (-> obj .get_common .get_inputs)) ; SpoutSpec定义了ComponentCommon common, ComponentCommon定义了inputs. obj.getCommon().getInputs()
      (throw (InvalidTopologyException. "May not declare inputs for a spout"))))  ; 必须给spout定义输入源.
  ; 假设builder.setSpout("spout", new RandomSentenceSpout(), 5); 则spout component的key=spout, value为RandomSentenceSpout对象
  (doseq [[comp-id comp] (all-components topology)    ; 该拓扑所有的components. Map中key为component-id, value为component对象
          :let [conf (component-conf comp)            ; component的配置信息, 定义在ComponentCommon的json_conf字段中, 需要反序列化
                p (-> comp .get_common thrift/parallelism-hint)]]
    (when (and (> (conf TOPOLOGY-TASKS) 0)            ; 当tasks的数量>0时, executor的数量也必须>0
               p
               (<= p 0))
      (throw (InvalidTopologyException. "Number of executors must be greater than 0 when number of tasks is greater than 0"))
      )))

;; topology, spout, bolt这三者比较好理解. WordCountTopology是一个topology, 里面定义的自定义Spout,Bolt都是Component
;; Stream其实要结合Spout和bolt的实现中的Tuple来理解. streams are composed of tuples

(defn validate-structure! [^StormTopology topology]
  ;; validate all the component subscribe from component+stream which actually exists in the topology
  ;; and if it is a fields grouping, validate the corresponding field exists  
  (let [all-components (all-components topology)]
    (doseq [[id comp] all-components
            :let [inputs (.. comp get_common get_inputs)]]
      (doseq [[global-stream-id grouping] inputs
              :let [source-component-id (.get_componentId global-stream-id)
                    source-stream-id    (.get_streamId global-stream-id)]]
        (if-not (contains? all-components source-component-id)
          (throw (InvalidTopologyException. (str "Component: [" id "] subscribes from non-existent component [" source-component-id "]")))
          (let [source-streams (-> all-components (get source-component-id) .get_common .get_streams)]
            (if-not (contains? source-streams source-stream-id)
              (throw (InvalidTopologyException. (str "Component: [" id "] subscribes from non-existent stream: [" source-stream-id "] of component [" source-component-id "]")))
              (if (= :fields (thrift/grouping-type grouping))
                (let [grouping-fields (set (.get_fields grouping))
                      source-stream-fields (-> source-streams (get source-stream-id) .get_output_fields set)
                      diff-fields (set/difference grouping-fields source-stream-fields)]
                  (when-not (empty? diff-fields)
                    (throw (InvalidTopologyException. (str "Component: [" id "] subscribes from stream: [" source-stream-id "] of component [" source-component-id "] with non-existent fields: " diff-fields)))))))))))))

(defn acker-inputs [^StormTopology topology]
  (let [bolt-ids (.. topology get_bolts keySet)   ; 该topo的所有bolt的id. 注意keys和keySet的区别
        spout-ids (.. topology get_spouts keySet)
        ; acker的机制是只要有tupler产生,就会生成一个唯一的id: stream-id
        spout-inputs (apply merge
                            (for [id spout-ids]
                              {[id ACKER-INIT-STREAM-ID] ["id"]}
                              ))
        bolt-inputs (apply merge
                           (for [id bolt-ids]
                             {[id ACKER-ACK-STREAM-ID] ["id"]
                              [id ACKER-FAIL-STREAM-ID] ["id"]}
                             ))]
    (merge spout-inputs bolt-inputs)))

(defn add-acker! [storm-conf ^StormTopology ret]
  (let [num-executors (if (nil? (storm-conf TOPOLOGY-ACKER-EXECUTORS)) (storm-conf TOPOLOGY-WORKERS) (storm-conf TOPOLOGY-ACKER-EXECUTORS))
        acker-bolt (thrift/mk-bolt-spec* (acker-inputs ret)
                                         (new backtype.storm.daemon.acker)
                                         {ACKER-ACK-STREAM-ID (thrift/direct-output-fields ["id"])
                                          ACKER-FAIL-STREAM-ID (thrift/direct-output-fields ["id"])
                                          }
                                         :p num-executors
                                         :conf {TOPOLOGY-TASKS num-executors
                                                TOPOLOGY-TICK-TUPLE-FREQ-SECS (storm-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS)})]
    (dofor [[_ bolt] (.get_bolts ret)
            :let [common (.get_common bolt)]]
           (do
             (.put_to_streams common ACKER-ACK-STREAM-ID (thrift/output-fields ["id" "ack-val"]))
             (.put_to_streams common ACKER-FAIL-STREAM-ID (thrift/output-fields ["id"]))
             ))
    (dofor [[_ spout] (.get_spouts ret)
            :let [common (.get_common spout)
                  spout-conf (merge
                               (component-conf spout)
                               {TOPOLOGY-TICK-TUPLE-FREQ-SECS (storm-conf TOPOLOGY-MESSAGE-TIMEOUT-SECS)})]]
      (do
        ;; this set up tick tuples to cause timeouts to be triggered
        (.set_json_conf common (to-json spout-conf))
        (.put_to_streams common ACKER-INIT-STREAM-ID (thrift/output-fields ["id" "init-val" "spout-task"]))
        (.put_to_inputs common
                        (GlobalStreamId. ACKER-COMPONENT-ID ACKER-ACK-STREAM-ID)
                        (thrift/mk-direct-grouping))
        (.put_to_inputs common
                        (GlobalStreamId. ACKER-COMPONENT-ID ACKER-FAIL-STREAM-ID)
                        (thrift/mk-direct-grouping))
        ))
    (.put_to_bolts ret "__acker" acker-bolt)
    ))

;; 给topology添加度量的streams. 可以给topology统计任务信息,数据点等
(defn add-metric-streams! [^StormTopology topology]
  (doseq [[_ component] (all-components topology) ; 不关心component-id的值,用_表示
          :let [common (.get_common component)]]  ; 获取ComponentCommon的引用. Spout或Bolt中有Component common字段
    (.put_to_streams common METRICS-STREAM-ID     ; 添加到ComponentCommon的stream中. METRICS-STREAM-ID为StreamInfo的key
                     (thrift/output-fields ["task-info" "data-points"]))))  ; value为StreamInfo

;; 添加系统级的streams: 事件
(defn add-system-streams! [^StormTopology topology]
  (doseq [[_ component] (all-components topology)
          :let [common (.get_common component)]]
    (.put_to_streams common SYSTEM-STREAM-ID (thrift/output-fields ["event"]))))


;; afn是个函数, 根据调用者如果有2个参数, 则在这里的实现也会有2个参数: (afn x occurs)
(defn map-occurrences [afn coll]
  (->> coll
       ; reduce的结构和map类似: (reduce fun coll)还有一种提供初始值的(reduce fun 默认值 coll)这里是第二种.
       ; 还有一个约定是: 如果提供了初始值形式. 则fun的参数要和初始值的结构一样,并且最后一个参数是coll的每个元素
       ; 比如init为一个Map:{}, 则(reduce fun[m v] {} coll) 其中m为Map, v为coll的每个元素
       ; 如果init为[{} []], 则(reduce fun[[m col] x]) 其中m是Map,col是vector, x为coll的element
       (reduce (fn [[counts new-coll] x]
                 (let [occurs (inc (get counts x 0))]     ; counts.get(x,0)如果有x,则取x在counts中的value,否则=0. occurs表示x出现的次数
                    ; 返回多个值的[]结构,第一个元素为一个map,所以初始值形式的第一个元素为{}. 第二个元素是[], (cons)表达式返回的正是vector
                    ; counts和new-coll会作为下次reduce调用的参数
                    ; 假设coll=[a,b,a], reduce第一次处理x=a, 返回值为: [{:a 1} [a]]
                    ; 第二次处理x=b, 返回值为: [{:a 1 :b 1} [a b]]
                    ; 第二次处理x=a, 返回值为: [{:a 2 :b 1} [a b a#2]]
                   [(assoc counts x occurs) (cons (afn x occurs) new-coll)]))
               [{} []]) ; 这个是默认值,一般默认值给出返回值的结构! ->> coll的coll作为reduce要迭代的数据源
       (second)     ; 很显然, 我们需要的是第二个元素, 即上面的[a b a#2]
       (reverse)))  ; 进行反转的原因是reduce实际上返回的是[a#2 b a]

(defn number-duplicates [coll]
  ; 如果出现重复且不是第一个元素, 则要在后面添加序号
  "(number-duplicates [\"a\", \"b\", \"a\"]) => [\"a\", \"b\", \"a#2\"]
   (number-duplicates [\"a\", \"b\", \"a\", \"a\"]) => [\"a\", \"b\", \"a#2\", \"a#3\"]"
  (map-occurrences (fn [x occurences] (if (>= occurences 2) (str x "#" occurences) x)) coll))

(defn metrics-consumer-register-ids [storm-conf]
  "Generates a list of component ids for each metrics consumer
   e.g. [\"__metrics_org.mycompany.MyMetricsConsumer\", ..] "
  (->> (get storm-conf TOPOLOGY-METRICS-CONSUMER-REGISTER)         
       (map #(get % "class"))
       (number-duplicates)
       (map #(str Constants/METRICS_COMPONENT_ID_PREFIX %))))

(defn metrics-consumer-bolt-specs [storm-conf topology]
  (let [component-ids-that-emit-metrics (cons SYSTEM-COMPONENT-ID (keys (all-components topology)))
        inputs (->> (for [comp-id component-ids-that-emit-metrics]
                      {[comp-id METRICS-STREAM-ID] :shuffle})
                    (into {}))
        
        mk-bolt-spec (fn [class arg p]
                       (thrift/mk-bolt-spec*
                        inputs
                        (backtype.storm.metric.MetricsConsumerBolt. class arg)
                        {} :p p :conf {TOPOLOGY-TASKS p}))]
    
    (map
     (fn [component-id register]           
       [component-id (mk-bolt-spec (get register "class")
                                   (get register "argument")
                                   (or (get register "parallelism.hint") 1))])
     
     (metrics-consumer-register-ids storm-conf)
     (get storm-conf TOPOLOGY-METRICS-CONSUMER-REGISTER))))

(defn add-metric-components! [storm-conf ^StormTopology topology]  
  (doseq [[comp-id bolt-spec] (metrics-consumer-bolt-specs storm-conf topology)]
    (.put_to_bolts topology comp-id bolt-spec)))

(defn add-system-components! [conf ^StormTopology topology]
  (let [system-bolt-spec (thrift/mk-bolt-spec*
                          {}
                          (SystemBolt.)
                          {SYSTEM-TICK-STREAM-ID (thrift/output-fields ["rate_secs"])
                           METRICS-TICK-STREAM-ID (thrift/output-fields ["interval"])}                          
                          :p 0
                          :conf {TOPOLOGY-TASKS 0})]
    (.put_to_bolts topology SYSTEM-COMPONENT-ID system-bolt-spec)))

(defn system-topology! [storm-conf ^StormTopology topology]
  (validate-basic! topology)
  (let [ret (.deepCopy topology)]
    (add-acker! storm-conf ret)
    (add-metric-components! storm-conf ret)    
    (add-system-components! storm-conf ret)
    (add-metric-streams! ret)
    (add-system-streams! ret)
    (validate-structure! ret)
    ret
    ))

;; 是否配置了acker
(defn has-ackers? [storm-conf]
  (or (nil? (storm-conf TOPOLOGY-ACKER-EXECUTORS)) (> (storm-conf TOPOLOGY-ACKER-EXECUTORS) 0)))

;; component的并行度. 即每个component启动的executors的数量
(defn num-start-executors [component]
  (thrift/parallelism-hint (.get_common component)))

;; ---------------------(topology的任务信息) Component和Task的对应关系------------------------
;; 首先读出所有components
;; 对每个component, 读出ComponentCommon中的json_conf, 然后从里面读出上面设置的TOPOLOGY_TASKS
;; 最后用递增序列产生taskid, 并最终生成component和task的对应关系
;; 如果不设置, task数等于executor数, 后面分配就很容易, 否则就涉及task分配问题
;;
;; 输出{component-id:tasknum}, 按component-id排序, 再进行mapcat
;; {c1 3, c2 2, c3 1} –> (c1,c1,c1,c2,c2,c3)
;; 再加上递增编号, into到map, {1 c1, 2 c1, 3 c1, 4 c2, 5 c2, 6 c3}
  (defn storm-task-info
  "Returns map from task -> component id"
  [^StormTopology user-topology storm-conf]
  (->> (system-topology! storm-conf user-topology)
       all-components     ; topology所有的component
       ; (map-val afn amap): 注意: amap = all-components, 而不是component-conf!
       ; afn包括两个func, 使用comp连接起来.
       ; amap={component-id, component}, amap的value会作为afn的参数=component

       ; 问题是component会作为第一个还是第二个的参数? 答案是第二个函数即component-conf(component)
       ; 1. component-conf的返回值是component的ComponentCommon对象的json_conf字段的序列化结果
       ; 在这个结果上调用json_conf.get(TOPOLOGY-TASKS). 因为TOPOLOGY-TASKS是json_conf的一个key.
       ; json_conf中TOPOLOGY-TASKS什么时候设置进来? --> nimbus.clj/normalize-topology
       ; 2. 如果component作为第一个函数的参数, 那么component.get(TOPOLOGY-TASKS)是错误的. 因为component上没有这个字段

       ; 取出component里面的ComponentCommon对象(.getcommon),并读出json_conf,返回值是一个Map. 最终读出conf中TOPOLOGY-TASKS.
       ; 再来看(map-val afn amap)的返回值: {amap.key, afn(amap.value)} 因为amap.key=component-id, 而afn(amap.vlaue)就是上面的TOPOLOGY-TASKS的值
       ; 所以下面这个表达式的返回值是: {component-id topology-tasks}. 即component的id和tasks数量的映射.
       (map-val (comp #(get % TOPOLOGY-TASKS) component-conf))
       (sort-by first)  ; 根据component-id进行排序
       ; 上面的结果会作为(mapcat fun coll)的coll,并且coll的每一个元素会被解析成fun的参数.
       ; 因为coll是一个map,所以fun的参数c=component-id, num-tasks=topology-tasks
       (mapcat (fn [[c num-tasks]] (repeat num-tasks c))) ; 根据num-tasks的值, 重复创建对应数量的component-id
       (map (fn [id comp] [id comp]) (iterate (comp int inc) (int 1)))
       (into {})
       ))

;; 方法: executor-id和它包含的所有task编号
;; executor的组成形式就是[start-task end-task],作为参数时,可以直接解构成这种形式
;; 假设executor=[1 3], 则返回值为{1 2 3}
(defn executor-id->tasks [[first-task-id last-task-id]]
  (->> (range first-task-id (inc last-task-id))
       (map int)))

;; worker的上下文对象: 包括topology级别的component<-->task数据, 以及worker自身的port, task-ids等
(defn worker-context [worker]
  (WorkerTopologyContext. ; topology相关
                          (:system-topology worker)
                          (:storm-conf worker)
                          (:task->component worker)             ; topology所有的task-id和component-id的映射,不是针对当前worker
                          (:component->sorted-tasks worker)     ; 同上, topology级别
                          (:component->stream->fields worker)
                          (:storm-id worker)

                          ; worker相关
                          (supervisor-storm-resources-path      ; ${storm.local.dir}/supervisor/stormdist/storm-id/resources
                            (supervisor-stormdist-root (:conf worker) (:storm-id worker)))
                          (worker-pids-root (:conf worker) (:worker-id worker))             ; $storm.local.dir/workers/#worker-id/pids
                          (:port worker)
                          (:task-ids worker)                    ; 运行在worker上的executors的所有task-ids
                          (:default-shared-resources worker)
                          (:user-shared-resources worker)
                          ))

;; executor->node+port 比如是{[1 3] [node1+port1]}
;; 将executor解析出所有的task-id, {1 [node1+port1], 2 [node1+port1], 3 [node1+port1]}
(defn to-task->node+port [executor->node+port]
  (->> executor->node+port
       (mapcat (fn [[e node+port]] (for [t (executor-id->tasks e)] [t node+port])))
       (into {})))
