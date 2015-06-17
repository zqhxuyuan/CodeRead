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
(ns backtype.storm.daemon.nimbus
  (:import [org.apache.thrift.server THsHaServer THsHaServer$Args])
  (:import [org.apache.thrift.protocol TBinaryProtocol TBinaryProtocol$Factory])
  (:import [org.apache.thrift.exception])
  (:import [org.apache.thrift.transport TNonblockingServerTransport TNonblockingServerSocket])
  (:import [java.nio ByteBuffer])
  (:import [java.io FileNotFoundException])
  (:import [java.nio.channels Channels WritableByteChannel])
  (:use [backtype.storm.scheduler.DefaultScheduler])
  (:import [backtype.storm.scheduler INimbus SupervisorDetails WorkerSlot TopologyDetails
            Cluster Topologies SchedulerAssignment SchedulerAssignmentImpl DefaultScheduler ExecutorDetails])
  (:use [backtype.storm bootstrap util])
  (:use [backtype.storm.config :only [validate-configs-with-schemas]])
  (:use [backtype.storm.daemon common])
  (:gen-class
    :methods [^{:static true} [launch [backtype.storm.scheduler.INimbus] void]]))

(bootstrap)

(defn file-cache-map [conf]
  (TimeCacheMap.
   (int (conf NIMBUS-FILE-COPY-EXPIRATION-SECS))
   (reify TimeCacheMap$ExpiredCallback
          (expire [this id stream]
                  (.close stream)
                  ))
   ))

(defn mk-scheduler [conf inimbus]
  (let [forced-scheduler (.getForcedScheduler inimbus)	; standalone-nimbus的INimbus匿名类getForcedScheduler的实现为空
        scheduler (cond               				; 条件表达式 (cond cond1 exp1 cond2 exp2 :else default)
                    forced-scheduler  				; INimbus指定了调度器的实现
                    (do (log-message "Using forced scheduler from INimbus " (class forced-scheduler))
                      forced-scheduler)				; do 表达式类似语句块

                    (conf STORM-SCHEDULER)  			; 使用Storm的配置. defaults.yaml中并没有storm.scheduler这个配置项, 要自定义的话写在storm.yaml中
                    (do (log-message "Using custom scheduler: " (conf STORM-SCHEDULER))
                      (-> (conf STORM-SCHEDULER) new-instance))   	; 实例化

                    :else
                    (do (log-message "Using default scheduler")
                      (DefaultScheduler.)))]  			; 默认的调度器DefaultScheduler.clj
    (.prepare scheduler conf)
    scheduler
    ))

(defn nimbus-data [conf inimbus]
  (let [forced-scheduler (.getForcedScheduler inimbus)]
    {:conf conf
     :inimbus inimbus             ; INimbus实现类, standalone-nimbus的返回值
     :submitted-count (atom 0)    ; 已经提交的计算拓扑的数量, 初始值为原子值0.当提交一个拓扑,这个数字+1
     :storm-cluster-state (cluster/mk-storm-cluster-state conf) ; 创建storm集群的状态. 保存在ZooKeeper中!
     :submit-lock (Object.)       ; 对象锁, 用于各个topology之间的互斥操作, 比如建目录
     :heartbeats-cache (atom {})  ; 心跳缓存, 记录各个Topology的heartbeats的cache
     :downloaders (file-cache-map conf) ; 文件下载和上传缓存, 是一个TimeCacheMap
     :uploaders (file-cache-map conf)
     :uptime (uptime-computer)    ; 计算上传的时间
     :validator (new-instance (conf NIMBUS-TOPOLOGY-VALIDATOR)) ; (map key)等价于(key map)
     :timer (mk-timer :kill-fn (fn [t]
                                 (log-error t "Error when processing event")
                                 (halt-process! 20 "Error when processing an event")
                                 ))
     :scheduler (mk-scheduler conf inimbus)
     }))

(defn inbox [nimbus]
  (master-inbox (:conf nimbus)))

;; 序列化storm-conf(是一个Map)发生在setup-storm-code时, 序列化成stormconf.ser
;; 读取storm-conf是反序列化的过程, 返回值是一个Map
(defn- read-storm-conf [conf storm-id]
  (let [stormroot (master-stormdist-root conf storm-id)]
    (merge conf
           (Utils/deserialize
            (FileUtils/readFileToByteArray
             (File. (master-stormconf-path stormroot))
             )))))

(defn set-topology-status! [nimbus storm-id status]
  (let [storm-cluster-state (:storm-cluster-state nimbus)]
   (.update-storm! storm-cluster-state
                   storm-id
                   {:status status})
   (log-message "Updated " storm-id " with status " status)
   ))

(declare delay-event)
(declare mk-assignments)

(defn kill-transition [nimbus storm-id]
  (fn [kill-time]
    (let [delay (if kill-time
                  kill-time
                  (get (read-storm-conf (:conf nimbus) storm-id)
                       TOPOLOGY-MESSAGE-TIMEOUT-SECS))]
      (delay-event nimbus
                   storm-id
                   delay
                   :remove)
      {:type :killed
       :kill-time-secs delay})
    ))

(defn rebalance-transition [nimbus storm-id status]
  (fn [time num-workers executor-overrides]
    (let [delay (if time
                  time
                  (get (read-storm-conf (:conf nimbus) storm-id)
                       TOPOLOGY-MESSAGE-TIMEOUT-SECS))]
      (delay-event nimbus
                   storm-id
                   delay
                   :do-rebalance)
      {:type :rebalancing
       :delay-secs delay
       :old-status status
       :num-workers num-workers
       :executor-overrides executor-overrides
       })))

(defn do-rebalance [nimbus storm-id status]
  (.update-storm! (:storm-cluster-state nimbus)
                  storm-id
                  (assoc-non-nil
                    {:component->executors (:executor-overrides status)}
                    :num-workers
                    (:num-workers status)))
  (mk-assignments nimbus :scratch-topology-id storm-id))

(defn state-transitions [nimbus storm-id status]
  {:active {:inactivate :inactive            
            :activate nil
            :rebalance (rebalance-transition nimbus storm-id status)
            :kill (kill-transition nimbus storm-id)
            }
   :inactive {:activate :active
              :inactivate nil
              :rebalance (rebalance-transition nimbus storm-id status)
              :kill (kill-transition nimbus storm-id)
              }
   :killed {:startup (fn [] (delay-event nimbus
                                         storm-id
                                         (:kill-time-secs status)
                                         :remove)
                             nil)
            :kill (kill-transition nimbus storm-id)
            :remove (fn []
                      (log-message "Killing topology: " storm-id)
                      (.remove-storm! (:storm-cluster-state nimbus)
                                      storm-id)
                      nil)
            }
   :rebalancing {:startup (fn [] (delay-event nimbus
                                              storm-id
                                              (:delay-secs status)
                                              :do-rebalance)
                                 nil)
                 :kill (kill-transition nimbus storm-id)
                 :do-rebalance (fn []
                                 (do-rebalance nimbus storm-id status)
                                 (:old-status status))
                 }})

(defn topology-status [nimbus storm-id]
  (-> nimbus :storm-cluster-state (.storm-base storm-id nil) :status))

(defn transition!
  ([nimbus storm-id event]
     (transition! nimbus storm-id event false))
  ([nimbus storm-id event error-on-no-transition?]
     (locking (:submit-lock nimbus)
       (let [system-events #{:startup}
             [event & event-args] (if (keyword? event) [event] event)
             status (topology-status nimbus storm-id)]
         ;; handles the case where event was scheduled but topology has been removed
         (if-not status
           (log-message "Cannot apply event " event " to " storm-id " because topology no longer exists")
           (let [get-event (fn [m e]
                             (if (contains? m e)
                               (m e)
                               (let [msg (str "No transition for event: " event
                                              ", status: " status,
                                              " storm-id: " storm-id)]
                                 (if error-on-no-transition?
                                   (throw-runtime msg)
                                   (do (when-not (contains? system-events event)
                                         (log-message msg))
                                       nil))
                                 )))
                 transition (-> (state-transitions nimbus storm-id status)
                                (get (:type status))
                                (get-event event))
                 transition (if (or (nil? transition)
                                    (keyword? transition))
                              (fn [] transition)
                              transition)
                 new-status (apply transition event-args)
                 new-status (if (keyword? new-status)
                              {:type new-status}
                              new-status)]
             (when new-status
               (set-topology-status! nimbus storm-id new-status)))))
       )))

(defn transition-name! [nimbus storm-name event & args]
  (let [storm-id (get-storm-id (:storm-cluster-state nimbus) storm-name)]
    (when-not storm-id
      (throw (NotAliveException. storm-name)))
    (apply transition! nimbus storm-id event args)))

(defn delay-event [nimbus storm-id delay-secs event]
  (log-message "Delaying event " event " for " delay-secs " secs for " storm-id)
  (schedule (:timer nimbus)
            delay-secs
            #(transition! nimbus storm-id event false)
            ))

;; active -> reassign in X secs

;; killed -> wait kill time then shutdown
;; active -> reassign in X secs
;; inactive -> nothing
;; rebalance -> wait X seconds then rebalance
;; swap... (need to handle kill during swap, etc.)
;; event transitions are delayed by timer... anything else that comes through (e.g. a kill) override the transition? or just disable other transitions during the transition?


(defmulti setup-jar cluster-mode)
(defmulti clean-inbox cluster-mode)

;; swapping design
;; -- need 2 ports per worker (swap port and regular port)
;; -- topology that swaps in can use all the existing topologies swap ports, + unused worker slots
;; -- how to define worker resources? port range + number of workers?


;; Monitoring (or by checking when nodes go down or heartbeats aren't received):
;; 1. read assignment
;; 2. see which executors/nodes are up
;; 3. make new assignment to fix any problems
;; 4. if a storm exists but is not taken down fully, ensure that storm takedown is launched (step by step remove executors and finally remove assignments)

(defn- assigned-slots
  "Returns a map from node-id to a set of ports"
  [storm-cluster-state]
  (let [assignments (.assignments storm-cluster-state nil)
        ]
    (defaulted
      (apply merge-with set/union
             (for [a assignments
                   [_ [node port]] (-> (.assignment-info storm-cluster-state a nil) :executor->node+port)]
               {node #{port}}
               ))
      {})
    ))

;; 3.7.1 从zk上读到每个supervisor的info, supervisor的hb, 返回{supervisor-id, info}
(defn- all-supervisor-info
  ([storm-cluster-state] (all-supervisor-info storm-cluster-state nil))   ; 调用下面的实现,其中callback=nil
  ([storm-cluster-state callback]
     (let [supervisor-ids (.supervisors storm-cluster-state callback)]    ; 从zk的superviors目录下读出所有superviors-id
       (into {}               ; []转成成map: {supervisor-id info}
             (mapcat          ; (mapcat fun[supervisor-id]... supervisor-ids)
              (fn [id]
                (if-let [info (.supervisor-info storm-cluster-state id)]  ; 从zk读取某supervisor的info
                  [[id info]] ; [supervisor-id info]还要转成map才是最终的返回值
                  ))
              supervisor-ids))
       )))
;; StormClusterState的几个方法:
;; assignments 获取/storm/assignments下的所有topology-id
;; assignment-info(storm-id) 获取指定/storm/assignments/topology-id的数据
;; supervisors 获取/storm/supervisors下的所有supervisor-id
;; supervisor-info(id) 获取指定/storm/supervisors/supervisor-id的数据

;; 3.7 all-scheduling-slots, 找出所有supervisor在conf中已配置的slots
(defn- all-scheduling-slots
  [nimbus topologies missing-assignment-topologies]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        ^INimbus inimbus (:inimbus nimbus)
        ; 3.7.1 从zk上读到每个supervisor的info, supervisor的hb, 返回{supervisorid, info}
        supervisor-infos (all-supervisor-info storm-cluster-state nil)
        ; 3.7.2 将supervisor-info封装成SupervisorDetails, (SupervisorDetails. id (:meta info)))
        supervisor-details (dofor [[id info] supervisor-infos]
                             (SupervisorDetails. id (:meta info)))
        ; 3.7.3 可以用来调度的所有slots
        ret (.allSlotsAvailableForScheduling inimbus  ; 此处inimbus的实现是standalone-nimbus
                     supervisor-details
                     topologies
                     (set missing-assignment-topologies)
                     )
        ]
    (dofor [^WorkerSlot slot ret]         ; slot是每一次循环的变量. 循环的集合是ret
      [(.getNodeId slot) (.getPort slot)] ; 每个WorkerSlot包括nodeId和port
      )))

(defn- optimize-topology [topology]
  ;; TODO: create new topology by collapsing bolts into CompoundSpout
  ;; and CompoundBolt
  ;; need to somehow maintain stream/component ids inside tuples
  topology)

;; tmp-jar-location实际上是beginFileUpload的返回值: ${storm.local.dir}/nimbus/inbox/stormjar-uuid.jar
(defn- setup-storm-code [conf storm-id tmp-jar-location storm-conf topology]
  (let [stormroot (master-stormdist-root conf storm-id)]  ; ${storm.local.dir}/nimbus/stormdist/storm-id
   (FileUtils/forceMkdir (File. stormroot))
   (FileUtils/cleanDirectory (File. stormroot))
   (setup-jar conf tmp-jar-location stormroot)  ; 将stormjar-uuid.jar从inbox下复制为stormdist/storm-id/stormjar.jar
   (FileUtils/writeByteArrayToFile (File. (master-stormcode-path stormroot)) (Utils/serialize topology))  ; topology序列化
   (FileUtils/writeByteArrayToFile (File. (master-stormconf-path stormroot)) (Utils/serialize storm-conf)); storm配置信息序列化
   ))

;; 序列化storm-topology(StormTopology对象)发生在setup-storm-code时, 序列化成stormcode.ser
;; 读取storm-topology是反序列化, 返回值为StormTopology对象!
(defn- read-storm-topology [conf storm-id]
  (let [stormroot (master-stormdist-root conf storm-id)]
    (Utils/deserialize
      (FileUtils/readFileToByteArray
        (File. (master-stormcode-path stormroot))
        ))))

(declare compute-executor->component)

;; 读出topology的更多的详细信息, 并最终封装成TopologyDetails, 其中包含关于topology的所有信息
(defn read-topology-details [nimbus storm-id]
  (let [conf (:conf nimbus)
        ; storm-base的序列化过程发生在start-storm的activate-storm!时
        ; 这里进行反序列化是为了获取其中的workers数量. 为什么不直接从下一句的topology-conf中取?
        ; 因为StormBase保存在zk中,信息比较少. 而topolog-conf的配置很多,查找的过程会慢点.
        storm-base (.storm-base (:storm-cluster-state nimbus) storm-id nil)   ;从zookeeper读出storm-base的内容
        topology-conf (read-storm-conf conf storm-id)                         ;从storm本地目录中读出topology的配置
        topology (read-storm-topology conf storm-id)                          ;从storm本地目录中读出topology的对象(反序列化)
        executor->component (->> (compute-executor->component nimbus storm-id);读出executor和component的对应关系
                                 (map-key (fn [[start-task end-task]]
                                            (ExecutorDetails. (int start-task) (int end-task)))))]  ;将executor封装成ExecutorDetials对象
    ; 将topologies信息, 封装成Topologies
    (TopologyDetails. storm-id                  ; topology-id
                      topology-conf             ; topology-conf
                      topology                  ; topology对象
                      (:num-workers storm-base) ; worker的数量. 这个是整个拓扑级别的,不是某个component级别
                      executor->component       ; component和executor关系
                      )))

;; Does not assume that clocks are synchronized. Executor heartbeat is only used so that
;; nimbus knows when it's received a new heartbeat. All timing is done by nimbus and
;; tracked through heartbeat-cache
(defn- update-executor-cache [curr hb timeout]
  (let [reported-time (:time-secs hb)
        {last-nimbus-time :nimbus-time
         last-reported-time :executor-reported-time} curr
        reported-time (cond reported-time reported-time
                            last-reported-time last-reported-time
                            :else 0)
        nimbus-time (if (or (not last-nimbus-time)
                        (not= last-reported-time reported-time))
                      (current-time-secs)
                      last-nimbus-time
                      )]
      {:is-timed-out (and
                       nimbus-time
                       (>= (time-delta nimbus-time) timeout))
       :nimbus-time nimbus-time
       :executor-reported-time reported-time}))

(defn update-heartbeat-cache [cache executor-beats all-executors timeout]
  ; all-executor: #([start-task end-task])
  ; (select-keys coll keys) 所以cache也是一个map, 它的key的结构也是: [start-task end-task]
  (let [cache (select-keys cache all-executors)]
    (into {}
      (for [executor all-executors          ; executor的结构就是: [start-task end-task]
              :let [curr (cache executor)]] ; (map key). 因为cache是一个map, 它的key就是for循环的变量executor
        [executor
         (update-executor-cache curr (get executor-beats executor) timeout)]
         ))))

;; storm-id: topology-id
;; all-executors: topology-id的所有executors
;; existing-assignment: topology-id的assignment-info
(defn update-heartbeats! [nimbus storm-id all-executors existing-assignment]
  (log-debug "Updating heartbeats for " storm-id " " (pr-str all-executors))
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        ;; executor-beats的参数为: [this storm-id executor->node+port]
        ;; executor->node+port保存在assignment-info中. 即common.clj的defrecord Assignment.
        ;; 注意这里的existing-assignment是反序列化的对象. 反序列化操作通过cluster.clj/StormClusterState/assignment-info. 序列化通过set-assignment!
        executor-beats (.executor-beats storm-cluster-state storm-id (:executor->node+port existing-assignment))
        cache (update-heartbeat-cache (@(:heartbeats-cache nimbus) storm-id)  ; :heartbeats-cache是nimbus的key. 其本身又是一个Map. storm-id是cache的一个key.
                                      executor-beats
                                      all-executors
                                      ((:conf nimbus) NIMBUS-TASK-TIMEOUT-SECS))]
      (swap! (:heartbeats-cache nimbus) assoc storm-id cache)))   ; 给nimbus的:heartbeats-cache(是一个Map)添加{k v}={storm-id cache}

;; 3.2 更新所有executors的heartbeats cache(更新nimbus-data的heartbeats-cache)
(defn- update-all-heartbeats! [nimbus existing-assignments topology->executors]
  "update all the heartbeats for all the topologies's executors"
  (doseq [[tid assignment] existing-assignments           ; existing-assignments:{topology-id assignment-info}={tid assignment}
          :let [all-executors (topology->executors tid)]] ; topology->executors: {topology-id executors}, 所以(topology-executors tid)获取topology-id的所有executors
    (update-heartbeats! nimbus tid all-executors assignment)))  ; 循环更新topology-id的executors和assignment.

;; 通过刚刚更新的heartbeats cache来判断executor是否alive
(defn- alive-executors
  [nimbus ^TopologyDetails topology-details all-executors existing-assignment]
  (log-debug "Computing alive executors for " (.getId topology-details) "\n"
             "Executors: " (pr-str all-executors) "\n"
             "Assignment: " (pr-str existing-assignment) "\n"
             "Heartbeat cache: " (pr-str (@(:heartbeats-cache nimbus) (.getId topology-details)))
             )
  ;; TODO: need to consider all executors associated with a dead executor (in same slot) dead as well,
  ;; don't just rely on heartbeat being the same
  (let [conf (:conf nimbus)
        storm-id (.getId topology-details)
        executor-start-times (:executor->start-time-secs existing-assignment)
        heartbeats-cache (@(:heartbeats-cache nimbus) storm-id)]
    (->> all-executors
        (filter (fn [executor]
          (let [start-time (get executor-start-times executor)
                is-timed-out (-> heartbeats-cache (get executor) :is-timed-out)]
            (if (and start-time
                   (or
                    (< (time-delta start-time)
                       (conf NIMBUS-TASK-LAUNCH-SECS))
                    (not is-timed-out)
                    ))
              true
              (do
                (log-message "Executor " storm-id ":" executor " not alive")
                false))
            )))
        doall)))   ;doall很重要, 确保真正filter每个executor, 否则只会产生lazy-seq


(defn- to-executor-id [task-ids]
  [(first task-ids) (last task-ids)]) ; 对于只有一个task-ids的(taskId), 则返回值里的两个元素是一样的: [taskId taskId]

;; ------------------------Topology中, Task和Executor的分配关系-----------------------
(defn- compute-executors [nimbus storm-id]
  (let [conf (:conf nimbus)
        storm-base (.storm-base (:storm-cluster-state nimbus) storm-id nil)
        ; StormBase保存了component到executors的映射: {component-id, executorNumber}
        ; 假设spout1的并行度为2. 则component->executors = {spout1-id 2}
        ; -- 现在我们要反过来求executor到component的映射.

        ; 示例: 对于c1, 2个executor, 3个tasks [1 2 3]. c2, 2个executor.  c3, 1个executor
        component->executors (:component->executors storm-base) ; {"c1" 2, "c2" 2, "c3" 1}
        storm-conf (read-storm-conf conf storm-id)
        topology (read-storm-topology conf storm-id)
        ; task和component的映射: {task编号 component-id}
        task->component (storm-task-info topology storm-conf)]
    (->> (storm-task-info topology storm-conf)        ; {1 "c1", 2 "c1", 3 "c1", 4 "c2", 5 "c2", 6 "c3"}
         reverse-map                                  ; {"c1" [1,2,3], "c2" [4,5], "c3" 6}
         (map-val sort)
         (join-maps component->executors)             ; {"c1" ‘(2 [1 2 3]), "c2" ‘(2 [4 5]), "c3" ‘(1 6)}   最后一个元素6没有放在[]里是因为只有一个元素
                                                      ;       ‘(executor [tasknumbs]) 表示c1有2个executor, 共3个tasks. tasks的编号为[1,2,3]
         (map-val (partial apply partition-fixed))    ; {"c1" ['(1 2) '(3)], "c2" ['(4) '(5)], "c3" ['(6)]}
         (mapcat second)                              ; ((1 2) (3) (4) (5) (6))
         (map to-executor-id)                         ; ([1 2] [3 3] [4 4] [5 5] [6 6])
                                                      ; 第一个executor的taskid范围为[1 2], 第二个executor的taskid范围为[3 3]...
                                                      ; 那么怎么知道第一个executor是属于哪一个component?? 这就是compute-executor->component的工作
         )))

;; --------------Topology中, Executor和component的关系----------------------
;; 根据(executor:task)关系和(task:component)关系join
;; 最终目的就是获得executor->component关系, 用于后面的assignment, 其中每个executor包含task范围[starttask, endtask]
(defn- compute-executor->component [nimbus storm-id]
  (let [conf (:conf nimbus)
        executors (compute-executors nimbus storm-id) ; ([1 2] [3 3] [4 4] [5 5] [6 6])
        topology (read-storm-topology conf storm-id)
        storm-conf (read-storm-conf conf storm-id)
        task->component (storm-task-info topology storm-conf) ; {1 "c1", 2 "c1", 3 "c1", 4 "c2", 5 "c2", 6 "c3"}
        executor->component (into {} (for [executor executors ; 每一个executors, 比如[1 2]
                                           :let [start-task (first executor)  ; 1
                                                 component (task->component start-task)]] ; (map key)="c1"
                                       {executor component}))]  ; {[1 2] "c1"}
        executor->component)) ;;{[1 2] “c1”, [3 3] “c1”, [4 4] “c2”, [5 5] “c2”, [6 6] “c3”}

;; 3.1  取出所有已经assignment的topology的executors信息
(defn- compute-topology->executors [nimbus storm-ids]
  "compute a topology-id -> executors map"
  (into {} (for [tid storm-ids]
             {tid (set (compute-executors nimbus tid))})))

;; 3.3  过滤topology->executors, 保留alive的
(defn- compute-topology->alive-executors [nimbus existing-assignments topologies topology->executors scratch-topology-id]
  "compute a topology-id -> alive executors map"
  (into {} (for [[tid assignment] existing-assignments
                 :let [topology-details (.getById topologies tid)
                       all-executors (topology->executors tid)
                       alive-executors (if (and scratch-topology-id (= scratch-topology-id tid))
                                         all-executors
                                         (set (alive-executors nimbus topology-details all-executors assignment)))]]
             {tid alive-executors})))

;; 3.4 找出dead slots
(defn- compute-supervisor->dead-ports [nimbus existing-assignments topology->executors topology->alive-executors]
  (let [dead-slots (into [] (for [[tid assignment] existing-assignments
                                  :let [all-executors (topology->executors tid)
                                        alive-executors (topology->alive-executors tid)
                                        ;; 找到dead-executors
                                        dead-executors (set/difference all-executors alive-executors)
                                        ;; 把dead-executors 对应的node+port都当成dead slots
                                        dead-slots (->> (:executor->node+port assignment)  ;[executor [node port]]
                                                        (filter #(contains? dead-executors (first %))) ; 如果上面结果的executor(即数组的第一个元素)在dead-executors中
                                                        vals)]] ; 返回所有values组成的seq
                              dead-slots))
        ;; 最终返回所有dead slots: {nodeid #{port1, port2},…}
        supervisor->dead-ports (->> dead-slots
                                    (apply concat)
                                    (map (fn [[sid port]] {sid #{port}}))
                                    (apply (partial merge-with set/union)))]
    (or supervisor->dead-ports {})))

;; 3.5 生成alive executor的SchedulerAssignment
;; 把alive executor的assignment(executor->node+port), 转化并封装为SchedulerAssignmentImpl, 便于后面scheduler使用
(defn- compute-topology->scheduler-assignment [nimbus existing-assignments topology->alive-executors]
  "convert assignment information in zk to SchedulerAssignment, so it can be used by scheduler api."
  (into {} (for [[tid assignment] existing-assignments
                 :let [alive-executors (topology->alive-executors tid)
                       executor->node+port (:executor->node+port assignment)
                       executor->slot (into {} (for [[executor [node port]] executor->node+port]  ; executor->node+port是{executor [node port]},for循环的参数进行解构
                                                 ;; filter out the dead executors 首先进行过滤,executor必须在alive-executors中
                                                 (if (contains? alive-executors executor)
                                                   ;; executor是一个数组,第一个元素为这个executor上的startTask的编号,第二个为endTask编号
                                                   {(ExecutorDetails. (first executor)
                                                                      (second executor))
                                                    (WorkerSlot. node port)}  ; 将nodeId和port封装成WorkerSlot. 一个worker由supervisor-id+port组成.
                                                   {})))]]
             ; SchedulerAssignmentImpl记录了topology中所有executor, 以及每个executor对应的workerslot, 可见executor作为assignment的单位
             ; 这里有双层for循环. 第一层是所有的topology. 使用existing-assignments作为数据. 第二层是每个topology的所有executors. 数据源为executor->node+port
             {tid (SchedulerAssignmentImpl. tid executor->slot)}))) ; executor->slot是一个{ExecutorDetails WorkerSlot}的map

;; 3.8 生成SupervisorDetails
(defn- read-all-supervisor-details [nimbus all-scheduling-slots supervisor->dead-ports]
  "return a map: {topology-id SupervisorDetails}"
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        supervisor-infos (all-supervisor-info storm-cluster-state)
        ; all-scheduling-slots是{supervisor-id slot}={node-id #{port}}
        ; supervisor-infos是zk上/storm/supervisors下的所有supervisors-id的数据={supervisor-id info}
        ; dissoc=all-scheduling-slots.remove(supervisor-infos.keySets)
        ; 在all-scheduling-slots中有, 但是在supervisor-infos(zk的hb)没有的supervisor
        ; 什么情况下会有这种case, 当前实现all-scheduling-slots本身就来自supervisor-infos, 应该不存在这种case
        nonexistent-supervisor-slots (apply dissoc all-scheduling-slots (keys supervisor-infos))  ;(keys ..)=supervisors-id
        ; 生成supervisor-details, 参考前面supervisor-info和supervisor-details的定义
        all-supervisor-details (into {} (for [[sid supervisor-info] supervisor-infos              ; {supervisor-id supervisor-info}
                                              :let [hostname (:hostname supervisor-info)
                                                    scheduler-meta (:scheduler-meta supervisor-info)
                                                    dead-ports (supervisor->dead-ports sid)
                                                    ;; hide the dead-ports from the all-ports
                                                    ;; these dead-ports can be reused in next round of assignments
                                                    all-ports (-> (get all-scheduling-slots sid)
                                                                  (set/difference dead-ports)     ; 去除dead-ports
                                                                  ((fn [ports] (map int ports))))
                                                    supervisor-details (SupervisorDetails. sid hostname scheduler-meta all-ports)]]
                                          {sid supervisor-details}))]                             ; {supervisor-id supervisor-details}
    (merge all-supervisor-details 
           (into {}
              (for [[sid ports] nonexistent-supervisor-slots]
                [sid (SupervisorDetails. sid nil ports)]))
           )))

(defn- compute-topology->executor->node+port [scheduler-assignments]
  "convert {topology-id -> SchedulerAssignment} to
           {topology-id -> {executor [node port]}}"
  (map-val (fn [^SchedulerAssignment assignment]
             (->> assignment
                  .getExecutorToSlot
                  (#(into {} (for [[^ExecutorDetails executor ^WorkerSlot slot] %]
                              {[(.getStartTask executor) (.getEndTask executor)]
                               [(.getNodeId slot) (.getPort slot)]})))))
           scheduler-assignments))

;; NEW NOTES
;; only assign to supervisors who are there and haven't timed out
;; need to reassign workers with executors that have timed out (will this make it brittle?)
;; need to read in the topology and storm-conf from disk
;; if no slots available and no slots used by this storm, just skip and do nothing
;; otherwise, package rest of executors into available slots (up to how much it needs)

;; in the future could allocate executors intelligently (so that "close" tasks reside on same machine)

;; TODO: slots that have dead executor should be reused as long as supervisor is active


;; (defn- assigned-slots-from-scheduler-assignments [topology->assignment]
;;   (->> topology->assignment
;;        vals
;;        (map (fn [^SchedulerAssignment a] (.getExecutorToSlot a)))
;;        (mapcat vals)
;;        (map (fn [^WorkerSlot s] {(.getNodeId s) #{(.getPort s)}}))
;;        (apply merge-with set/union)
;;        ))

(defn num-used-workers [^SchedulerAssignment scheduler-assignment]
  (if scheduler-assignment
    (count (.getSlots scheduler-assignment))
    0 ))

;; public so it can be mocked out
;; 3.1 ~3.6, topology assignment情况
;;   a. 从zk获取topology中executors的assignment信息, 但是assignment是静态信息.
;;      我们还需要知道, assign完后这些executor是否在工作, 更新executor的hb, 并找出alive-executors, 这部分assignment才是有效的assignment, 所以仅仅将alive-executors封装生成topology->scheduler-assignment
;;   b. 在check topology assignment中, 发现的dead slot
;;      对于那些没有hb的executor, 我们认为是slot产生了问题, 称为dead slot, 后面需要避免再往dead slot分配executor (dead slot可能有alive-executors存在)
;; 3.7~3.8, supervisor的情况
;;      根据supervisor的hb, 获取当前alive的supervisor的状况SupervisorDetails, 主要是hostname, 和allports(配置的ports – dead slots)
;; 3.9, cluster, topology的运行态信息, 包含上面的两点信息
;;      cluster (Cluster. (:inimbus nimbus) supervisors topology->scheduler-assignment)
(defn compute-new-topology->executor->node+port [nimbus existing-assignments topologies scratch-topology-id]
  (let [conf (:conf nimbus)
        storm-cluster-state (:storm-cluster-state nimbus)
        ;; 3.1  取出所有已经assignment的topology的executors信息
        ;; 所有已经assignment的Topology所包含的executor, {t1 #([1 2] [3 3]), t2 #([1 2] [3 3])}
        ;; compute-executors计算的是某个topology的executors, 这里加上topology-id形成{topology-id executors}的映射
        ;; 这里的实现有些问题, compute-topology->executors会调用compute-executors重新计算一次, 其实从topologies里面直接就可以取到
        topology->executors (compute-topology->executors nimbus (keys existing-assignments))  ;;只包含存在assignment信息的, 所以新的或scratch Topology都不算
        ;; 3.2 更新所有executors的heartbeats cache(更新nimbus-data的heartbeats-cache)
        ;; 从Zookeeper通过get-worker-heartbeat读出所有executors最新的heartbeats信息(通过executor->node+port可以对应到worker),
        ;; 并使用swap!将最新的heartbeats信息更新到nimbus的全局变量heartbeats-cache中
        ;; update the executors heartbeats first.
        _ (update-all-heartbeats! nimbus existing-assignments topology->executors)            ;;只是为了在let中提前调用update-all-heartbeats!, 所以使用'_'
        ;; 3.3  过滤topology->executors, 保留alive的
        topology->alive-executors (compute-topology->alive-executors nimbus
                                                                     existing-assignments
                                                                     topologies
                                                                     topology->executors
                                                                     scratch-topology-id)
        ;; 3.4 找出dead slots
        ;; slot就是对node+port的抽象封装, 一个slot可以运行一个worker, 所以在supervisor分配多少slot就可以运行多少worker
        ;; 而对于executor是线程, 所以往往dead executor意味着, 这个workerslot dead.
        supervisor->dead-ports (compute-supervisor->dead-ports nimbus
                                                               existing-assignments
                                                               topology->executors
                                                               topology->alive-executors)
        ;; 3.5 生成alive executor的SchedulerAssignment
        topology->scheduler-assignment (compute-topology->scheduler-assignment nimbus
                                                                               existing-assignments
                                                                               topology->alive-executors)
        ;; 3.6 找出missing-assignment-topologies, 需要重新assign
        ;;   a.topology->executors, 其中没有该topolgy, 说明该topology没有assignment信息, 新的或scratch
        ;;   b.topology->executors != topology->alive-executors, 说明有executor dead
        ;;   c.topology->scheduler-assignment中的实际worker数小于topology配置的worker数 (可能上次assign的时候可用slot不够, 也可能由于dead slot造成)
        missing-assignment-topologies (->> topologies
                                           .getTopologies         ; 返回TopologyDetials.
                                           (map (memfn getId))
                                           (filter (fn [t]        ; topology-id
                                                      (let [alle (get topology->executors t)  ; all-executors
                                                            alivee (get topology->alive-executors t)] ; alive-executors
                                                            (or (empty? alle)       ; a: this tid don't have any executors. It's new!
                                                                (not= alle alivee)  ; b: some executor in this tid dead!
                                                                (< (-> topology->scheduler-assignment
                                                                       (get t)
                                                                       num-used-workers )
                                                                   (-> topologies (.getById t) .getNumWorkers)  ; c
                                                                   ))
                                                            ))))
        ;; 3.7 all-scheduling-slots, 找出所有supervisor在conf中已配置的slots
        all-scheduling-slots (->> (all-scheduling-slots nimbus topologies missing-assignment-topologies)
                                  ; 上面的结果返回[node-id port]的集合形式, 运用到下面的map表达式中,对应了(map fun coll)的fun的参数
                                  ; 最终得到的是supervisor中配置的所有slots的nodeid+port的集合, {node1 #{port1 port2 port3}, node2 #{port1 port2}}
                                  ; node-id表示supervisor-id
                                  (map (fn [[node-id port]] {node-id #{port}}))
                                  (apply merge-with set/union))
        ;; 3.8 生成所有supervisors的SupervisorDetails
        supervisors (read-all-supervisor-details nimbus all-scheduling-slots supervisor->dead-ports)
        ;; 3.9 生成cluster
        cluster (Cluster. (:inimbus nimbus) supervisors topology->scheduler-assignment)

        ;; 3.10 call scheduler.schedule to schedule all the topologies
        ;; the new assignments for all the topologies are in the cluster object.
        _ (.schedule (:scheduler nimbus) topologies cluster)
        new-scheduler-assignments (.getAssignments cluster)

        ;; 3.11 add more information to convert SchedulerAssignment to Assignment
        new-topology->executor->node+port (compute-topology->executor->node+port new-scheduler-assignments)]
    ;; print some useful information.
    (doseq [[topology-id executor->node+port] new-topology->executor->node+port
            :let [old-executor->node+port (-> topology-id
                                          existing-assignments
                                          :executor->node+port)
                  reassignment (filter (fn [[executor node+port]]
                                         (and (contains? old-executor->node+port executor)
                                              (not (= node+port (old-executor->node+port executor)))))
                                       executor->node+port)]]
      (when-not (empty? reassignment)
        (let [new-slots-cnt (count (set (vals executor->node+port)))
              reassign-executors (keys reassignment)]
          (log-message "Reassigning " topology-id " to " new-slots-cnt " slots")
          (log-message "Reassign executors: " (vec reassign-executors)))))

    new-topology->executor->node+port))

(defn changed-executors [executor->node+port new-executor->node+port]
  (let [slot-assigned (reverse-map executor->node+port)
        new-slot-assigned (reverse-map new-executor->node+port)
        brand-new-slots (map-diff slot-assigned new-slot-assigned)]
    (apply concat (vals brand-new-slots))
    ))

(defn newly-added-slots [existing-assignment new-assignment]
  (let [old-slots (-> (:executor->node+port existing-assignment)
                      vals
                      set)
        new-slots (-> (:executor->node+port new-assignment)
                      vals
                      set)]
    (set/difference new-slots old-slots)))


(defn basic-supervisor-details-map [storm-cluster-state]
  (let [infos (all-supervisor-info storm-cluster-state)]
    (->> infos
         (map (fn [[id info]]
                 [id (SupervisorDetails. id (:hostname info) (:scheduler-meta info) nil)]))
         (into {}))))

(defn- to-worker-slot [[node port]]
  (WorkerSlot. node port))

;; 产生executor->node+port关系, 将executor分配到哪个node的哪个slot上(port代表slot, 一个slot可以run一个worker进程, 一个worker包含多个executor线程)
;; 在storm.yaml中可以配置一个supervisor的多个端口, 这个端口就是slot. 所以一个supervisor可以运行多个worker进程.
;; 假设配置一个supervisor可以运行4个worker. 但是topology并没有定义setNumWorkers. 则默认worker的数量就等于配置的4个worker.??
;; 现在假设集群中有2个supervisor,每个supervisor配置4个端口. 则一共有2*4=8个worker进程
;; 对于topology: bolt1配置了8个executor, bolt2配置了12个executor. 并且bolt2的输入来自于bolt1.
;; 则第一个bolt1运行时, 一共有8个executor同时运行, 而刚好集群有8个worker进程. 所以每个worker进程可以同时运行一个executor.
;; 当第二个bolt2运行时, 一共有12个executor同时运行, 集群有8个worker. 则有4个worker跑了2个executor, 其他4个worker只跑一个executor.
;; get existing assignment (just the executor->node+port map) -> default to {}
;; filter out ones which have a executor timeout
;; figure out available slots on cluster. add to that the used valid slots to get total slots.
;; figure out how many executors should be in each slot (e.g., 4, 4, 4, 5)
;; only keep existing slots that satisfy one of those slots. for rest, reassign them across remaining slots
;; edge case for slots with no executor timeout but with supervisor timeout...
;; just treat these as valid slots that can be reassigned to. worst comes to worse the executor will timeout and won't assign here next time around
(defnk mk-assignments [nimbus :scratch-topology-id nil]
  (let [conf (:conf nimbus)
        storm-cluster-state (:storm-cluster-state nimbus)                 ; 和zk操作相关的API通过storm-cluster-state完成
        ^INimbus inimbus (:inimbus nimbus) 
        ;; read all the topologies.
        ;; 1. 读出所有active topology信息
        ;; 先使用active-storms去zookeeper上读到所有active的topology的ids
        ;; 然后使用read-topology-details读出topology的更多的详细信息,
        ;; 并最终封装成TopologyDetails, 其中包含关于topology的所有信息
        topology-ids (.active-storms storm-cluster-state)                 ; 读出所有topology的ids
        topologies (into {} (for [tid topology-ids]                       ; tid is each elem of topology-ids
                              {tid (read-topology-details nimbus tid)}))  ; {topology-id TopologyDetails}
        topologies (Topologies. topologies)
        ;; read all the assignments
        ;; 2. 读出当前的assignemnt情况
        assigned-topology-ids (.assignments storm-cluster-state nil)      ;已经被assign的tids
        existing-assignments (into {} (for [tid assigned-topology-ids]
                                        ;; for the topology which wants rebalance (specified by the scratch-topology-id)
                                        ;; we exclude its assignment, meaning that all the slots occupied by its assignment
                                        ;; will be treated as free slot in the scheduler code.
                                        (when (or (nil? scratch-topology-id) (not= tid scratch-topology-id))
                                          {tid (.assignment-info storm-cluster-state tid nil)})))  ; 根据topologyid, 读出具体的信息
        ;; make the new assignments for topologies
        ;; 3. 根据取到的Topology和Assignement情况, 对当前topology进行新的assignment. 在真正调用scheduler.schedule 之前,需要做些准备工作
        topology->executor->node+port (compute-new-topology->executor->node+port
                                       nimbus
                                       existing-assignments ; {topology-id assignment-info}
                                       topologies           ; Topologies, 包括了{topology-id TopologyDetails}
                                       scratch-topology-id)
        
        
        now-secs (current-time-secs)
        
        basic-supervisor-details-map (basic-supervisor-details-map storm-cluster-state)
        
        ;; construct the final Assignments by adding start-times etc into it
        ;; 4. 将新的assignment结果存储到Zookeeper
        new-assignments (into {} (for [[topology-id executor->node+port] topology->executor->node+port
                                        :let [existing-assignment (get existing-assignments topology-id)  ; assignment-info
                                              all-nodes (->> executor->node+port
                                                             vals         ; node+port
                                                             (map first)  ; node
                                                             set)         ; #(node) node=supervisor-id
                                              node->host (->> all-nodes
                                                              (mapcat (fn [node]
                                                                        (if-let [host (.getHostName inimbus basic-supervisor-details-map node)]
                                                                          [[node host]]
                                                                          )))
                                                              (into {}))  ; {supervisor-id host}
                                              all-node->host (merge (:node->host existing-assignment) node->host)
                                              reassign-executors (changed-executors (:executor->node+port existing-assignment) executor->node+port)
                                              start-times (merge (:executor->start-time-secs existing-assignment)
                                                                (into {}
                                                                      (for [id reassign-executors]
                                                                        [id now-secs]
                                                                        )))]]
                                   {topology-id (Assignment.
                                                 (master-stormdist-root conf topology-id) ; 对应的nimbus上的代码目录
                                                 (select-keys all-node->host all-nodes)   ; node->host
                                                 executor->node+port                      ; 每个task与机器，端口的映射
                                                 start-times)}))]                         ; 所有task的启动时间

    ;; tasks figure out what tasks to talk to by looking at topology at runtime
    ;; only log/set when there's been a change to the assignment
    (doseq [[topology-id assignment] new-assignments  ; 第二个元素assignment就是上面的Assignment
            :let [existing-assignment (get existing-assignments topology-id)
                  topology-details (.getById topologies topology-id)]]
      (if (= existing-assignment assignment)
        (log-debug "Assignment for " topology-id " hasn't changed")
        (do
          (log-message "Setting new assignment for topology id " topology-id ": " (pr-str assignment))
          (.set-assignment! storm-cluster-state topology-id assignment)
          )))

    (->> new-assignments
          (map (fn [[topology-id assignment]]
            (let [existing-assignment (get existing-assignments topology-id)]
              [topology-id (map to-worker-slot (newly-added-slots existing-assignment assignment))] 
              )))
          (into {})
          (.assignSlots inimbus topologies))
    ))

(defn- start-storm [nimbus storm-name storm-id topology-initial-status]
  ; {:pre [(#{:active :inactive} :active)]} ==>  {:pre [:active]}
  {:pre [(#{:active :inactive} topology-initial-status)]}
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        conf (:conf nimbus)
        storm-conf (read-storm-conf conf storm-id)		; 合并conf和storm-id的配置stormconf.ser(反序列化<-在setup-storm-code时序列化)为storm-conf
        topology (system-topology! storm-conf (read-storm-topology conf storm-id))	; 第二个参数为StormTopology(stormcode.ser反序列化,同样在setup-storm-code时序列化)
        ; executors的数量. 定义在setSpout或setBolt的第三个参数,表示并行度.
        ; num-start-executors是一个函数,接受Component对象, 计算这个component设置的并行度
        ; (map-val aFun aMap). 这里的Map的key是comp-id,value是Component,正好是num-start-executors函数的参数
        ; 所以map-val工具方法的作用是接受一个aMap,其中aMap的value是aFun的参数. 最终返回aMap的key和aFun(value)组成的一个新Map
        ; 所以num-executors是Map类型, key为component-id, value为这个component的并行度值
        num-executors (->> (all-components topology) (map-val num-start-executors))]
    (log-message "Activating " storm-name ": " storm-id)
    ; 在前面我们已经见过了.setup-heartbeats! storm-cluster-state. 这里又是这样的结构
    ; 序列化StormBase对象(common.clj的defrecord StormBase)到zk的/storm/storms/topology-id节点上
    (.activate-storm! storm-cluster-state
      storm-id
      (StormBase. storm-name			        ; topology的名字
        (current-time-secs)			          ; topology开始运行的时间
        {:type topology-initial-status}		; topology的初始状态
        (storm-conf TOPOLOGY-WORKERS)	    ; worker的数量
        num-executors))))			            ; topology的每个component的component-id到并行度的映射

;; Master:
;; job submit:
;; 1. read which nodes are available
;; 2. set assignments
;; 3. start storm - necessary in case master goes down, when goes back up can remember to take down the storm (2 states: on or off)

;; 循环/storm/storms下的每个topology-id对应的storm-base方法(是个Map),找出map中:storm-name的值. 和参数storm-name比较
(defn storm-active? [storm-cluster-state storm-name]
  (not-nil? (get-storm-id storm-cluster-state storm-name)))

;; 根据名字检查拓扑是否是活动的. storm-name即submitTopology的第一个参数
(defn check-storm-active! [nimbus storm-name active?]
  (if (= (not active?)
         (storm-active? (:storm-cluster-state nimbus) storm-name))
    (if active?
      (throw (NotAliveException. (str storm-name " is not alive")))
      (throw (AlreadyAliveException. (str storm-name " is already active"))))
    ))

(defn code-ids [conf]
  (-> conf
      master-stormdist-root
      read-dir-contents
      set
      ))

(defn cleanup-storm-ids [conf storm-cluster-state]
  (let [heartbeat-ids (set (.heartbeat-storms storm-cluster-state))
        error-ids (set (.error-topologies storm-cluster-state))
        code-ids (code-ids conf)
        assigned-ids (set (.active-storms storm-cluster-state))]
    (set/difference (set/union heartbeat-ids error-ids code-ids) assigned-ids)
    ))

(defn extract-status-str [base]
  (let [t (-> base :status :type)]
    (.toUpperCase (name t))
    ))

(defn mapify-serializations [sers]
  (->> sers
       (map (fn [e] (if (map? e) e {e nil})))
       (apply merge)
       ))

(defn- component-parallelism [storm-conf component]
  (let [storm-conf (merge storm-conf (component-conf component))
        ; component的task的数量. 如果设置了topology-tasks的值. 则就是该值. 如果有没有设置, 则为executors的数量
        ; 注意component的task数量, 只有在没有设置topology-task选项(setNumTasks)时才和executors的数量相等
        num-tasks (or (storm-conf TOPOLOGY-TASKS) (num-start-executors component))
        ; component的最大并行度, 一般以topology级别来设置
        max-parallelism (storm-conf TOPOLOGY-MAX-TASK-PARALLELISM)
        ]
    ; 最后一个表达式是该方法的返回值, 即component的任务的并行度. 这个方法名有点误导,其实返回的是task的数量!
    (if max-parallelism
      (min max-parallelism num-tasks)
      num-tasks)))

;; 规范化拓扑信息. 重新设置topology一些新的属性
(defn normalize-topology [storm-conf ^StormTopology topology]
  (let [ret (.deepCopy topology)]
    (doseq [[_ component] (all-components ret)] ; 所有的component, 返回值为{component-id component}
      (.set_json_conf
        (.get_common component)
        (->> {TOPOLOGY-TASKS (component-parallelism storm-conf component)}  ; component的TOPOLOGY-TASKS数量. 新增加一个配置!
             (merge (component-conf component)) ; component-conf是component的json_conf字段! 将TOPOLOGY-TASKS设置到json_conf里!
             to-json )))  ; 最后将最新的json_conf重新设置到该component上
    ret ))

;; 规范化配置信息
(defn normalize-conf [conf storm-conf ^StormTopology topology]
  ;; ensure that serializations are same for all tasks no matter what's on
  ;; the supervisors. this also allows you to declare the serializations as a sequence
  (let [component-confs (map
                         #(-> (ThriftTopologyUtils/getComponentCommon topology %)
                              .get_json_conf
                              from-json)
                         (ThriftTopologyUtils/getComponentIds topology))
        total-conf (merge conf storm-conf)

        get-merged-conf-val (fn [k merge-fn]
                              (merge-fn
                               (concat
                                (mapcat #(get % k) component-confs)
                                (or (get storm-conf k)
                                    (get conf k)))))]
    ;; topology level serialization registrations take priority
    ;; that way, if there's a conflict, a user can force which serialization to use
    ;; append component conf to storm-conf
    (merge storm-conf
           {TOPOLOGY-KRYO-DECORATORS (get-merged-conf-val TOPOLOGY-KRYO-DECORATORS distinct)
            TOPOLOGY-KRYO-REGISTER (get-merged-conf-val TOPOLOGY-KRYO-REGISTER mapify-serializations)
            TOPOLOGY-ACKER-EXECUTORS (total-conf TOPOLOGY-ACKER-EXECUTORS)
            TOPOLOGY-MAX-TASK-PARALLELISM (total-conf TOPOLOGY-MAX-TASK-PARALLELISM)})))

(defn do-cleanup [nimbus]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        conf (:conf nimbus)
        submit-lock (:submit-lock nimbus)]
    (let [to-cleanup-ids (locking submit-lock
                           (cleanup-storm-ids conf storm-cluster-state))]
      (when-not (empty? to-cleanup-ids)
        (doseq [id to-cleanup-ids]
          (log-message "Cleaning up " id)
          (.teardown-heartbeats! storm-cluster-state id)
          (.teardown-topology-errors! storm-cluster-state id)
          (rmr (master-stormdist-root conf id))
          (swap! (:heartbeats-cache nimbus) dissoc id))
        ))))

(defn- file-older-than? [now seconds file]
  (<= (+ (.lastModified file) (to-millis seconds)) (to-millis now)))

(defn clean-inbox [dir-location seconds]
  "Deletes jar files in dir older than seconds."
  (let [now (current-time-secs)
        pred #(and (.isFile %) (file-older-than? now seconds %))
        files (filter pred (file-seq (File. dir-location)))]
    (doseq [f files]
      (if (.delete f)
        (log-message "Cleaning inbox ... deleted: " (.getName f))
        ;; This should never happen
        (log-error "Cleaning inbox ... error deleting: " (.getName f))
        ))))

(defn cleanup-corrupt-topologies! [nimbus]
  (let [storm-cluster-state (:storm-cluster-state nimbus)
        code-ids (set (code-ids (:conf nimbus)))
        active-topologies (set (.active-storms storm-cluster-state))
        corrupt-topologies (set/difference active-topologies code-ids)]
    (doseq [corrupt corrupt-topologies]
      (log-message "Corrupt topology " corrupt " has state on zookeeper but doesn't have a local dir on Nimbus. Cleaning up...")
      (.remove-storm! storm-cluster-state corrupt)
      )))

(defn- get-errors [storm-cluster-state storm-id component-id]
  (->> (.errors storm-cluster-state storm-id component-id)
       (map #(ErrorInfo. (:error %) (:time-secs %)))))

(defn- thriftify-executor-id [[first-task-id last-task-id]]
  (ExecutorInfo. (int first-task-id) (int last-task-id)))

(def DISALLOWED-TOPOLOGY-NAME-STRS #{"/" "." ":" "\\"})

(defn validate-topology-name! [name]
  (if (some #(.contains name %) DISALLOWED-TOPOLOGY-NAME-STRS)
    (throw (InvalidTopologyException.
            (str "Topology name cannot contain any of the following: " (pr-str DISALLOWED-TOPOLOGY-NAME-STRS))))
  (if (clojure.string/blank? name) 
    (throw (InvalidTopologyException. 
            ("Topology name cannot be blank"))))))

(defn- try-read-storm-conf [conf storm-id]
  (try-cause
    (read-storm-conf conf storm-id)
    (catch FileNotFoundException e
       (throw (NotAliveException. storm-id)))
  )
)

(defn- try-read-storm-topology [conf storm-id]
  (try-cause
    (read-storm-topology conf storm-id)
    (catch FileNotFoundException e
       (throw (NotAliveException. storm-id)))
  )
)

;; 在storm.thrift中的定义的Nimbus服务,其接口在 service-handler中一一得以实现
;; 注意standalone-nimbus中的INimbus和service Nimbus的区别.
;; 1. service-handler的第二个参数就是INimbus(standalone-nimbus返回INimbus的匿名类)
;; 2. service-handler的返回值是一个service Nimbus匿名实现类.
(defserverfn service-handler [conf inimbus]
  (.prepare inimbus conf (master-inimbus-dir conf))   ; 调用INimbus的prepare方法
  (log-message "Starting Nimbus with conf " conf)
  (let [nimbus (nimbus-data conf inimbus)]            ; nimbus的数据, 是一个Map.  用于存放nimbus相关配置和全局的参数
    (.prepare ^backtype.storm.nimbus.ITopologyValidator (:validator nimbus) conf) ; 校验器的准备工作,在提交时才进行验证
    (cleanup-corrupt-topologies! nimbus)              ; 清理冲突的拓扑
    (doseq [storm-id (.active-storms (:storm-cluster-state nimbus))]  ; 从storm集群拓扑中获取所有活动的计算拓扑
      (transition! nimbus storm-id :startup))         ; storm-id就是topology-id. 设置状态为启动.
    (schedule-recurring (:timer nimbus)
                        0
                        (conf NIMBUS-MONITOR-FREQ-SECS)
                        (fn []
                          (when (conf NIMBUS-REASSIGN)
                            (locking (:submit-lock nimbus)
                              (mk-assignments nimbus)))
                          (do-cleanup nimbus)
                          ))
    ; Schedule Nimbus inbox cleaner
    (schedule-recurring (:timer nimbus)
                        0
                        (conf NIMBUS-CLEANUP-INBOX-FREQ-SECS)
                        (fn []
                          (clean-inbox (inbox nimbus) (conf NIMBUS-INBOX-JAR-EXPIRATION-SECS))
                          ))
    ; 这是service-handler的最后一个表达式, reify匿名对象.
    (reify Nimbus$Iface
      (^void submitTopologyWithOpts
        ; storm名字; 上传Jar的目录; 序列化过的Conf信息; topology对象(thrift对象),由topologyBuilder产生
        [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology
         ^SubmitOptions submitOptions]
        (try
          (assert (not-nil? submitOptions))               ; 必须提供提交选项
          (validate-topology-name! storm-name)            ; 名字起的是否符合规范
          (check-storm-active! nimbus storm-name false)   ; 检查是否活动
          (let [topo-conf (from-json serializedConf)]     ; 反序列化conf:from-json. 序列化时转成json:to-json
            (try
              (validate-configs-with-schemas topo-conf)   ; 校验conf
              (catch IllegalArgumentException ex
                (throw (InvalidTopologyException. (.getMessage ex)))))
            (.validate ^backtype.storm.nimbus.ITopologyValidator (:validator nimbus)  ; 调用用户定义的validator.validate
                       storm-name
                       topo-conf
                       topology))
          (swap! (:submitted-count nimbus) inc)           ; submitted-count加1, 表示nimbus上submit的topology的数量
          (let [storm-id (str storm-name "-" @(:submitted-count nimbus) "-" (current-time-secs))  ; 生成storm-id
                storm-conf (normalize-conf                ; 转化成json,增加kv,最终生成storm-conf
                            conf
                            (-> serializedConf
                                from-json
                                (assoc STORM-ID storm-id) ; topology-id
                                (assoc TOPOLOGY-NAME storm-name))
                            topology)
                total-storm-conf (merge conf storm-conf)
                topology (normalize-topology total-storm-conf topology)   ; 规范化的topology对象
                topology (if (total-storm-conf TOPOLOGY-OPTIMIZE)
                           (optimize-topology topology)
                           topology)
                storm-cluster-state (:storm-cluster-state nimbus)]        ; 操作zk的接口
            (system-topology! total-storm-conf topology) ;; this validates the structure of the topology 1. System-topology!
            (log-message "Received topology submission for " storm-name " with conf " storm-conf)
            ;; lock protects against multiple topologies being submitted at once and
            ;; cleanup thread killing topology in b/w assignment and starting the topology
            (locking (:submit-lock nimbus)                                ; 准备完毕, 进入关键区域, 加锁!
              (setup-storm-code conf storm-id uploadedJarLocation storm-conf topology)  ; 2. 建立topology的本地目录
              (.setup-heartbeats! storm-cluster-state storm-id)           ; 3. 建立Zookeeper heartbeats
              (let [thrift-status->kw-status {TopologyInitialStatus/INACTIVE :inactive TopologyInitialStatus/ACTIVE :active}]
                (start-storm nimbus storm-name storm-id (thrift-status->kw-status (.get_initial_status submitOptions))))  ; 4. 启动storm
              (mk-assignments nimbus)))                                   ; 5. 分配任务
          (catch Throwable e
            (log-warn-error e "Topology submission exception. (topology name='" storm-name "')")
            (throw e))))
      
      (^void submitTopology
        [this ^String storm-name ^String uploadedJarLocation ^String serializedConf ^StormTopology topology]
        (.submitTopologyWithOpts this storm-name uploadedJarLocation serializedConf topology
                                 (SubmitOptions. TopologyInitialStatus/ACTIVE)))  ; 提交计算拓扑, 默认为活动的!
      
      (^void killTopology [this ^String name]
        (.killTopologyWithOpts this name (KillOptions.)))

      (^void killTopologyWithOpts [this ^String storm-name ^KillOptions options]
        (check-storm-active! nimbus storm-name true)
        (let [wait-amt (if (.is_set_wait_secs options)
                         (.get_wait_secs options)                         
                         )]
          (transition-name! nimbus storm-name [:kill wait-amt] true)
          ))

      (^void rebalance [this ^String storm-name ^RebalanceOptions options]
        (check-storm-active! nimbus storm-name true)
        (let [wait-amt (if (.is_set_wait_secs options)
                         (.get_wait_secs options))
              num-workers (if (.is_set_num_workers options)
                            (.get_num_workers options))
              executor-overrides (if (.is_set_num_executors options)
                                   (.get_num_executors options)
                                   {})]
          (doseq [[c num-executors] executor-overrides]
            (when (<= num-executors 0)
              (throw (InvalidTopologyException. "Number of executors must be greater than 0"))
              ))
          (transition-name! nimbus storm-name [:rebalance wait-amt num-workers executor-overrides] true)
          ))

      (activate [this storm-name]
        (transition-name! nimbus storm-name :activate true)
        )

      (deactivate [this storm-name]
        (transition-name! nimbus storm-name :inactivate true))

      ;; 开始上传文件: 指定topologyX.jar的文件位置, 并创建文件通道准备写数据
      (beginFileUpload [this]
        (let [fileloc (str (inbox nimbus) "/stormjar-" (uuid) ".jar")]
          (.put (:uploaders nimbus)     ;; uploaders是一个TimeCacheMap
                fileloc                 ;; map.put(fileloc, channel)
                (Channels/newChannel (FileOutputStream. fileloc)))
          (log-message "Uploading file from client to " fileloc)
          fileloc                       ;; 返回值为文件的上传路径. 这个是下面两个方法的location参数的值
          ))

      ;; 上传块: 第一个参数location是beginFileUpload的返回值, 第二个参数表示写入的数据
      (^void uploadChunk [this ^String location ^ByteBuffer chunk]
        (let [uploaders (:uploaders nimbus)   ;; 获取nimbus的:uploaders这个Map
              ^WritableByteChannel channel (.get uploaders location)] ;; 文件写入的通道 map.get(location)=channel
          (when-not channel (throw (RuntimeException. "File for that location does not exist (or timed out)")))
          (.write channel chunk)              ;; 往通道中写入块chunk
          (.put uploaders location channel)   ;; 因为分多次写, 每一次的channel对象不同
          ))

      ;; 结束上传: 参数location是beginFileUpload的返回值
      (^void finishFileUpload [this ^String location]
        (let [uploaders (:uploaders nimbus)
              ^WritableByteChannel channel (.get uploaders location)]
          (when-not channel (throw (RuntimeException. "File for that location does not exist (or timed out)")))
          (.close channel)                    ;; 关闭文件写通道
          (log-message "Finished uploading file from client: " location)
          (.remove uploaders location)        ;; uploaders.remove(location)
          ))

      ;; 开始下载
      (^String beginFileDownload [this ^String file]
        (let [is (BufferFileInputStream. file)
              id (uuid)]
          (.put (:downloaders nimbus) id is)  ;; 同样:downloaders也是一个Map: map.put(id, is)
          id                                  ;; 返回uuid, 作为后面下载时的参数
          ))

      ;; 下载块
      (^ByteBuffer downloadChunk [this ^String id]
        (let [downloaders (:downloaders nimbus)
              ^BufferFileInputStream is (.get downloaders id)]
          (when-not is (throw (RuntimeException. "Could not find input stream for that id")))
          (let [ret (.read is)]               ;; ret = is.read()
            (.put downloaders id is)
            (when (empty? ret)
              (.remove downloaders id))       ;; 类似上传结束后, 清理uploaders的location, 这里也清理put进来时的id
            (ByteBuffer/wrap ret)             ;; 将输入流读取的数据封装成ByteBuffer, 这个类型也是uploadChunk时的参数类型
            )))

      (^String getNimbusConf [this]
        (to-json (:conf nimbus)))

      (^String getTopologyConf [this ^String id]
        (to-json (try-read-storm-conf conf id)))

      (^StormTopology getTopology [this ^String id]
        (system-topology! (try-read-storm-conf conf id) (try-read-storm-topology conf id)))

      (^StormTopology getUserTopology [this ^String id]
        (try-read-storm-topology conf id))

      (^ClusterSummary getClusterInfo [this]
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              supervisor-infos (all-supervisor-info storm-cluster-state)
              ;; TODO: need to get the port info about supervisors...
              ;; in standalone just look at metadata, otherwise just say N/A?
              supervisor-summaries (dofor [[id info] supervisor-infos]
                                          (let [ports (set (:meta info)) ;;TODO: this is only true for standalone
                                                ]
                                            (SupervisorSummary. (:hostname info)
                                                                (:uptime-secs info)
                                                                (count ports)
                                                                (count (:used-ports info))
                                                                id )
                                            ))
              nimbus-uptime ((:uptime nimbus))
              bases (topology-bases storm-cluster-state)
              topology-summaries (dofor [[id base] bases]
                                        (let [assignment (.assignment-info storm-cluster-state id nil)]
                                          (TopologySummary. id
                                                            (:storm-name base)
                                                            (->> (:executor->node+port assignment)
                                                                 keys
                                                                 (mapcat executor-id->tasks)
                                                                 count) 
                                                            (->> (:executor->node+port assignment)
                                                                 keys
                                                                 count)                                                            
                                                            (->> (:executor->node+port assignment)
                                                                 vals
                                                                 set
                                                                 count)
                                                            (time-delta (:launch-time-secs base))
                                                            (extract-status-str base))
                                          ))]
          (ClusterSummary. supervisor-summaries
                           nimbus-uptime
                           topology-summaries)
          ))
      
      (^TopologyInfo getTopologyInfo [this ^String storm-id]
        (let [storm-cluster-state (:storm-cluster-state nimbus)
              task->component (storm-task-info (try-read-storm-topology conf storm-id) (try-read-storm-conf conf storm-id))
              base (.storm-base storm-cluster-state storm-id nil)
              assignment (.assignment-info storm-cluster-state storm-id nil)
              beats (.executor-beats storm-cluster-state storm-id (:executor->node+port assignment))
              all-components (-> task->component reverse-map keys)
              errors (->> all-components
                          (map (fn [c] [c (get-errors storm-cluster-state storm-id c)]))
                          (into {}))
              executor-summaries (dofor [[executor [node port]] (:executor->node+port assignment)]
                                        (let [host (-> assignment :node->host (get node))
                                              heartbeat (get beats executor)
                                              stats (:stats heartbeat)
                                              stats (if stats
                                                      (stats/thriftify-executor-stats stats))]
                                          (doto
                                              (ExecutorSummary. (thriftify-executor-id executor)
                                                                (-> executor first task->component)
                                                                host
                                                                port
                                                                (nil-to-zero (:uptime heartbeat)))
                                            (.set_stats stats))
                                          ))
              ]
          (TopologyInfo. storm-id
                         (:storm-name base)
                         (time-delta (:launch-time-secs base))
                         executor-summaries
                         (extract-status-str base)
                         errors
                         )
          ))
      
      Shutdownable
      (shutdown [this]
        (log-message "Shutting down master")
        (cancel-timer (:timer nimbus))
        (.disconnect (:storm-cluster-state nimbus))
        (.cleanup (:downloaders nimbus))
        (.cleanup (:uploaders nimbus))
        (log-message "Shut down master")
        )
      DaemonCommon
      (waiting? [this]
        (timer-waiting? (:timer nimbus))))))

;; 让nimbus作为一个thrift server运行起来
(defn launch-server! [conf nimbus]
  (validate-distributed-mode! conf)                     ;; 分布式模式下才会启动thrift server
  (let [service-handler (service-handler conf nimbus)   ;; 自定义实现类, 实现storm.thrift中service Nimbus定义的接口方法
        options (-> (TNonblockingServerSocket. (int (conf NIMBUS-THRIFT-PORT))) ;; 服务端的ServerSocket
                    (THsHaServer$Args.)   ;; TServerSocket作为TServer.Args内部类的参数. 创建了Args args对象 ->表示插入第二个位置
                    (.workerThreads 64)   ;; 上面new Args(TServerSocket)会作为这里的第二个位置, 即args.workerThreads(64)
                    (.protocolFactory (TBinaryProtocol$Factory. false true (conf NIMBUS-THRIFT-MAX-BUFFER-SIZE)))
                    (.processor (Nimbus$Processor. service-handler))  ;; args作为这里的第二个位置,即调用了args.processor
                    ;; new Nimbus.Processor(service-handler), 自定义实现类作为Nimbus.Processor的参数,
                    ;; processor会作为参数再传给args.processor()
                    ) ;; 最终返回的是TServer.AbstractServerArgs, 会作为TServer构造函数的参数
       server (THsHaServer. (do (set! (. options maxReadBufferBytes)(conf NIMBUS-THRIFT-MAX-BUFFER-SIZE)) options))]
    (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (.shutdown service-handler) (.stop server))))
    (log-message "Starting Nimbus server...") ;; 上面添加了一个关闭钩子. 类似回调函数. 当关闭Nimbus的thrift服务时, 会触发这个函数执行
    (.serve server))) ;; 启动TServer, 即启动Nimbus的thrift服务

;; distributed implementation

(defmethod setup-jar :distributed [conf tmp-jar-location stormroot]
           (let [src-file (File. tmp-jar-location)]
             (if-not (.exists src-file)
               (throw
                (IllegalArgumentException.
                 (str tmp-jar-location " to copy to " stormroot " does not exist!"))))
             ; 从${storm.local.dir}/nimbus/inbox/stormjar-uuid.jar复制到${storm.local.dir}/nimbus/stormdist/storm-id/stormjar.jar
             (FileUtils/copyFile src-file (File. (master-stormjar-path stormroot)))
             ))

;; local implementation

(defmethod setup-jar :local [conf & args]
  nil
  )

(defn -launch [nimbus]              ;; launch的参数是一个Nimbus对象, 所以上面standalone-nimbus方法的返回值是Nimbus
  (launch-server! (read-storm-config) nimbus))

;; 返回一个实现了INimbus接口的对象. 由于不想创建这种类型, 使用reify匿名对象
(defn standalone-nimbus []          ;; 没有参数. clojure中[]使用的地方有: let绑定, 方法的参数, vector
  (reify INimbus                    ;; reify: 具体化匿名数据类型: 需要一个实现了某一协议/接口的对象，但是不想创建一个命名的数据类型
    ;; 下面的方式都是INimbus接口的实现方法
    (prepare [this conf local-dir]) ;; this可以看做是一个隐式参数, prepare方法实际只有2个参数的
    ;; 这里是匿名对象,类似于回调函数, 那么这里面的方式什么时候被会调用呢?  下面的调用顺序以逆序的方式给出了调用链
    ;; 参考all-scheduling-slots -- compute-new-topology->executor->node+port -- mk-assignments -- service-handler -- submitTopologyWithOpts
    ;; 一般使用reify匿名对象,里面的方法相当于回调函数.其中的参数需要在真正调用该方法时给出.
    ;; 比如cluster.clj/mk-storm-cluster-state的reify StormClusterState
    ;; 当要调用匿名对象里的方法时,需要给出相应的参数,才能操作zookeeper.
    ;; 因此调用allSlotsAvailableForScheduling时,要给出supervisors...等的参数.在回调函数处理后将结果返回给调用者.
    (allSlotsAvailableForScheduling [this supervisors topologies topologies-missing-assignments]
      (->> supervisors
           (mapcat (fn [^SupervisorDetails s]
                     (for [p (.getMeta s)]            ;meta里面放的是conf里面配置的ports list, 对每一个封装成WorkerSlot
                       (WorkerSlot. (.getId s) p))))  ;可见nodeid就是supervisorid, nnid, 而不是ip
           set ))   ; 这个方法只用到supervisors参数, 把每个supervisor中配置的workerslot取出, 合并为set返回
    (assignSlots [this topology slots])
    (getForcedScheduler [this] nil )
    (getHostName [this supervisors node-id]
      (if-let [^SupervisorDetails supervisor (get supervisors node-id)]
        (.getHost supervisor)))
    ))

;; 启动nimbus的主方法
(defn -main []				;; main前面加上-, 表示是public的. 所以bin/storm能直接调用nimbus.clj的main方法
  (-launch (standalone-nimbus)))	;; 同样launch也是一个public方法. standalone-nimbus是一个方法, clojure对于没有参数的方法可以省略()
