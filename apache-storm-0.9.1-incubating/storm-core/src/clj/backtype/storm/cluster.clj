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
(ns backtype.storm.cluster
  (:import [org.apache.zookeeper.data Stat])
  (:import [org.apache.zookeeper KeeperException KeeperException$NoNodeException])
  (:import [backtype.storm.utils Utils])
  (:use [backtype.storm util log config])
  (:require [backtype.storm [zookeeper :as zk]])
  (:require [backtype.storm.daemon [common :as common]])
  
  )

(defprotocol ClusterState
  (set-ephemeral-node [this path data])
  (delete-node [this path])
  (create-sequential [this path data])
  (set-data [this path data])  ;; if node does not exist, create persistent with this data 
  (get-data [this path watch?])
  (get-children [this path watch?])
  (mkdirs [this path])
  (close [this])
  (register [this callback])
  (unregister [this id])
  )

(defn mk-distributed-cluster-state [conf]
  (let [zk (zk/mk-client conf (conf STORM-ZOOKEEPER-SERVERS) (conf STORM-ZOOKEEPER-PORT) :auth-conf conf)]
    (zk/mkdirs zk (conf STORM-ZOOKEEPER-ROOT))  ; defaults.yaml--> storm.zookeeper.root: "/storm"
    (.close zk))                                ; 创建完/storms后, 关闭zk
  (let [callbacks (atom {})
        active (atom true)
        zk (zk/mk-client conf   ; 因为mk-client是一个defnk函数. :key value提供的是额外的参数,会覆盖方法中的参数
                         (conf STORM-ZOOKEEPER-SERVERS)
                         (conf STORM-ZOOKEEPER-PORT)
                         :auth-conf conf
                         :root (conf STORM-ZOOKEEPER-ROOT)    ; 指定了相对路径为/storm, 则后面的mkdirs等方法均会加上这个相对路径!
                         ;; :watcher的值是一个函数!
                         :watcher (fn [state type path]   ; event.getState(), event.getType(), path. 和default-watcher函数的参数一样
                                     (when @active        ; 原子的解引用
                                       (when-not (= :connected state) ; state和type的取值见zookeeper.clj的zk-keeper-states和zk-event-types
                                         (log-warn "Received event " state ":" type ":" path " with disconnected Zookeeper."))
                                       (when-not (= :none type)
                                         (doseq [callback (vals @callbacks)]
                                           (callback type path))))
                                       ))]
    (reify ClusterState
     (register [this callback]
               (let [id (uuid)]
                 (swap! callbacks assoc id callback)  ; 给id注册一个回调函数
                 id                                   ; 返回值为id, 这样接下来的操作就可以使用这个id. 也就能取到对应的callback
                 ))
     (unregister [this id]
                 (swap! callbacks dissoc id))         ; 不注册. 参数id就是上面第一个函数register的返回值. 见怪不怪了吧.

     (set-ephemeral-node [this path data]             ; 设置短暂节点
                         (zk/mkdirs zk (parent-path path))
                         (if (zk/exists zk path false)
                           ; 已经存在节点: 直接更新数据
                           (try-cause
                             (zk/set-data zk path data) ; should verify that it's ephemeral
                             (catch KeeperException$NoNodeException e
                               (log-warn-error e "Ephemeral node disappeared between checking for existing and setting data")
                               (zk/create-node zk path data :ephemeral)
                               ))
                           ; 不存在, 直接创建一个mode=:ephemeral的节点
                           (zk/create-node zk path data :ephemeral)
                           ))
     
     (create-sequential [this path data]              ; 创建顺序节点
       (zk/create-node zk path data :sequential))
     
     (set-data [this path data]
               ;; note: this does not turn off any existing watches
               (if (zk/exists zk path false)
                 (zk/set-data zk path data)
                 (do
                   (zk/mkdirs zk (parent-path path))
                   (zk/create-node zk path data :persistent)
                   )))
     
     (delete-node [this path] (zk/delete-recursive zk path))
     
     (get-data [this path watch?] (zk/get-data zk path watch?))
     
     (get-children [this path watch?] (zk/get-children zk path watch?))
     
     (mkdirs [this path] (zk/mkdirs zk path))
     
     (close [this]
            (reset! active false)       ; 重置active=false. 在mk-client前初始值为true
            (.close zk))
     )))

(defprotocol StormClusterState
  ; 任务(get)
  (assignments [this callback])
  (assignment-info [this storm-id callback])
  (active-storms [this])
  (storm-base [this storm-id callback])

  ; worker(get)
  (get-worker-heartbeat [this storm-id node port])
  (executor-beats [this storm-id executor->node+port])
  (supervisors [this callback])
  (supervisor-info [this supervisor-id])  ;; returns nil if doesn't exist

  ; heartbeat and error
  (setup-heartbeats! [this storm-id])
  (teardown-heartbeats! [this storm-id])
  (teardown-topology-errors! [this storm-id])
  (heartbeat-storms [this])
  (error-topologies [this])

  ; update!(set and remove)
  (worker-heartbeat! [this storm-id node port info])
  (remove-worker-heartbeat! [this storm-id node port])
  (supervisor-heartbeat! [this supervisor-id info])
  (activate-storm! [this storm-id storm-base])
  (update-storm! [this storm-id new-elems])
  (remove-storm-base! [this storm-id])
  (set-assignment! [this storm-id info])
  (remove-storm! [this storm-id])
  (report-error [this storm-id task-id error])
  (errors [this storm-id task-id])

  (disconnect [this])
  )


(def ASSIGNMENTS-ROOT "assignments")
(def CODE-ROOT "code")
(def STORMS-ROOT "storms")
(def SUPERVISORS-ROOT "supervisors")
(def WORKERBEATS-ROOT "workerbeats")
(def ERRORS-ROOT "errors")

(def ASSIGNMENTS-SUBTREE (str "/" ASSIGNMENTS-ROOT))
(def STORMS-SUBTREE (str "/" STORMS-ROOT))
(def SUPERVISORS-SUBTREE (str "/" SUPERVISORS-ROOT))
(def WORKERBEATS-SUBTREE (str "/" WORKERBEATS-ROOT))
(def ERRORS-SUBTREE (str "/" ERRORS-ROOT))

;; 下面的几个路径是否包含最开始的/storm?? NO!
;; /supervisors/supervisor-id
(defn supervisor-path [id]
  (str SUPERVISORS-SUBTREE "/" id))

;; /assignments/topology-id
(defn assignment-path [id]
  (str ASSIGNMENTS-SUBTREE "/" id))

;; /storms/topology-id
(defn storm-path [id]
  (str STORMS-SUBTREE "/" id))

;; /workerbeats/topology-id
(defn workerbeat-storm-root [storm-id]
  (str WORKERBEATS-SUBTREE "/" storm-id))

;; /workerbeats/topology-id/node-port
(defn workerbeat-path [storm-id node port]
  (str (workerbeat-storm-root storm-id) "/" node "-" port))

;; /errors/topology-id
(defn error-storm-root [storm-id]
  (str ERRORS-SUBTREE "/" storm-id))

(defn error-path [storm-id component-id]
  (str (error-storm-root storm-id) "/" (url-encode component-id)))

(defn- issue-callback! [cb-atom]
  (let [cb @cb-atom]
    (reset! cb-atom nil)
    (when cb
      (cb))
    ))

(defn- issue-map-callback! [cb-atom id]
  (let [cb (@cb-atom id)]
    (swap! cb-atom dissoc id)
    (when cb
      (cb id))
    ))

(defn- maybe-deserialize [ser]
  (when ser
    (Utils/deserialize ser)))

(defstruct TaskError :error :time-secs)

(defn- parse-error-path [^String p]
  (Long/parseLong (.substring p 1)))


(defn convert-executor-beats [executors worker-hb]
  ;; ensures that we only return heartbeats for executors assigned to this worker
  (let [executor-stats (:executor-stats worker-hb)]
    (->> executors
      (map (fn [t]    ; (map fn colls), t is each ele of colls
             (if (contains? executor-stats t)
               {t {:time-secs (:time-secs worker-hb)
                    :uptime (:uptime worker-hb)
                    :stats (get executor-stats t)}})))
      (into {}))))

;; Watches should be used for optimization. When ZK is reconnecting, they're not guaranteed to be called.
;; 应该使用Watches来优化. 当ZK重新连接时, 它们(这里)并没有保证被调用.
;; nimbus.clj的nimbus-data调用参数是conf, 是一个Map. 这里定义了一个新的协议接口. satisfies?返回false
;; 什么时候返回true? 参数cluster-state-spec是ClusterState的子类时
(defn mk-storm-cluster-state [cluster-state-spec]
  ; 因为mk-distributed-cluster-state返回的是reify ClusterState, 所以let绑定的cluster-state是一个ClusterState实例!
  (let [[solo? cluster-state] (if (satisfies? ClusterState cluster-state-spec)
                                [false cluster-state-spec]
                                [true (mk-distributed-cluster-state cluster-state-spec)])
        assignment-info-callback (atom {})
        supervisors-callback (atom nil)
        assignments-callback (atom nil)
        storm-base-callback (atom {})
        state-id (register        ; 调用的ClusterState的register方法. register(this callback)
                  cluster-state   ; 第一个参数为this, 第二个参数为callback匿名函数.
                  (fn [type path] ; callback方法的参数和mk-distributed-cluster-state的:watcher的函数的(callback type path)一样.
                    (let [[subtree & args] (tokenize-path path)]  ; path为/storms/topo-id, 返回#{storms topo-id}, 则subtree=storms
                      (condp = subtree
                          ASSIGNMENTS-ROOT (if (empty? args)
                                             (issue-callback! assignments-callback)
                                             (issue-map-callback! assignment-info-callback (first args)))
                          SUPERVISORS-ROOT (issue-callback! supervisors-callback)
                          STORMS-ROOT (issue-map-callback! storm-base-callback (first args))
                          (halt-process! 30 "Unknown callback for subtree " subtree args) ;; this should never happen
                          )
                      )))]
    ; 在zookeeper上创建各个目录. ClusterState.mkdirs -> zk/mkdirs
    (doseq [p [ASSIGNMENTS-SUBTREE STORMS-SUBTREE SUPERVISORS-SUBTREE WORKERBEATS-SUBTREE ERRORS-SUBTREE]]
      (mkdirs cluster-state p))
    ; 又定义了一个协议. 不过这个协议都是通过调用ClusterState的方法来达到目的. 当然这两个协议的方法是不一样的
    (reify
     StormClusterState

      ;; 带!的方法一般都是更新方法, 序列化操作
      ;; 序列化操作是将数据写入到zookeeper上的节点
      ;; 反序列化操作是获取zookeeper上的节点的数据
      ;; get-children 会给定一个父节点id, 获取父节点下的所有子节点的名称, 并没有涉及这些子节点上的数据
      ;; deserialize 会给定一个节点id, 然后获取这个节点上的数据. 返回的数据是序列化时的数据

      ; 获取集群的所有任务. /storm/assignments下的所有topology-id
      ; 返回已经分配的所有topology的topology-id
     (assignments [this callback]
        (when callback
          (reset! assignments-callback callback))
        (get-children cluster-state ASSIGNMENTS-SUBTREE (not-nil? callback)))

      ; [反序列化] 任务信息: 序列化某一个指定的storm-id(即topology-id).  /storm/assignments/storm-id
      ; 根据topology-id得到/storm/assignments/topology-id这个节点的数据
      (assignment-info [this storm-id callback]
        (when callback
          (swap! assignment-info-callback assoc storm-id callback))
        (maybe-deserialize (get-data cluster-state (assignment-path storm-id) (not-nil? callback)))
        )

      ; 所有活动的计算拓扑. /storm/storms下的所有topology-id
      (active-storms [this]
        (get-children cluster-state STORMS-SUBTREE false)
        )

      ; 所有产生心跳的计算拓扑. /storm/workerbeats下的所有topology-id
      (heartbeat-storms [this]
        (get-children cluster-state WORKERBEATS-SUBTREE false)
        )

      ; 有错误的计算拓扑 /storm/errors
      (error-topologies [this]
         (get-children cluster-state ERRORS-SUBTREE false)
        )

      ; [反序列化] 工作进程worker的心跳信息. 工作进程在supervisor(node)的指定端口(port)上运行
      (get-worker-heartbeat [this storm-id node port]
        (-> cluster-state
            (get-data (workerbeat-path storm-id node port) false)
            maybe-deserialize))

      ;; supervisor > worker > executor > component/task(spout|bolt)
      ;; 一个supervisor有多个worker
      ;; 在storm.yaml中配置的每个node+port都组成一个worker
      ;; 一个worker可以有多个executor
      ;; 一个executor也可以有多个task. 如果没有设置task的数量, 默认一个executor对应一个task

      ; 执行线程executor的心跳信息
      ;; node实际上就是supervisor-id. port为storm.yaml配置的slot. 这样一个supervisor就有多个node+port的组合.
      (executor-beats [this storm-id executor->node+port]
        ;; need to take executor->node+port in explicitly so that we don't run into a situation where a 
        ;; long dead worker with a skewed clock overrides all the timestamps. By only checking heartbeats with an assigned node+port,
        ;; and only reading executors from that heartbeat that are actually assigned, we avoid situations like that
        (let [node+port->executors (reverse-map executor->node+port)                ; 反转map. supervisor node上配置的一个端口可以运行多个executor
              all-heartbeats (for [[[node port] executors] node+port->executors]
                                (->> (get-worker-heartbeat this storm-id node port) ; 获取worker的心跳信息
                                     (convert-executor-beats executors)             ; 上面函数的返回值作为convert的最后一个参数
                                     ))]
          (apply merge all-heartbeats)))

      ; 所有的supervisors. /storm/supervisors下的所有supervisor-id
      (supervisors [this callback]
        (when callback
          (reset! supervisors-callback callback))
        (get-children cluster-state SUPERVISORS-SUBTREE (not-nil? callback))
        )

      ; [反序列化] 序列化指定id的supervisor
      (supervisor-info [this supervisor-id]
        (maybe-deserialize (get-data cluster-state (supervisor-path supervisor-id) false))
        )

      ; [序列化] 更新worker的心跳信息
      (worker-heartbeat! [this storm-id node port info]
        (set-data cluster-state (workerbeat-path storm-id node port) (Utils/serialize info)))

      (remove-worker-heartbeat! [this storm-id node port]
        (delete-node cluster-state (workerbeat-path storm-id node port))
        )

      ; 创建一个心跳  /storm/workerbeats/storm-id
      ; 创建目录. 其他目录比如assignments,supervisors,storms下只有节点,没有目录, 所以不需要创建.
      ; 那么你会问, 节点也要创建啊. 节点的创建与否在set-data逻辑中已经完成了: 如果已经存在直接设置数据,如果不存在先创建再设值
      ; 在ClusterState -- mk-distributed-cluster-state 的三个方法中: set-ephemeral-node create-sequential set-data 都有创建节点的逻辑
      (setup-heartbeats! [this storm-id]
        (mkdirs cluster-state (workerbeat-storm-root storm-id)))

      (teardown-heartbeats! [this storm-id]
        (try-cause
         (delete-node cluster-state (workerbeat-storm-root storm-id))
         (catch KeeperException e
           (log-warn-error e "Could not teardown heartbeats for " storm-id)
           )))

      (teardown-topology-errors! [this storm-id]
        (try-cause
         (delete-node cluster-state (error-storm-root storm-id))         
         (catch KeeperException e
           (log-warn-error e "Could not teardown errors for " storm-id)
           )))

      ; [序列化] supervisor的心跳
      (supervisor-heartbeat! [this supervisor-id info]
        (set-ephemeral-node cluster-state (supervisor-path supervisor-id) (Utils/serialize info))
        )

      ; [序列化] 激活一个指定id的计算拓扑: /storm/storms/topology-id
      (activate-storm! [this storm-id storm-base]
        (set-data cluster-state (storm-path storm-id) (Utils/serialize storm-base))
        )

      ; [序列化] 使用new-elems更新一个指定的计算拓扑. 最后会进行序列化
      (update-storm! [this storm-id new-elems]
        (let [base (storm-base this storm-id nil)
              executors (:component->executors base)  ; component即task, 包括spout或者bolt. ->表示component从属于executor
              new-elems (update new-elems :component->executors (partial merge executors))] ; 偏函数, 因为executors只是其中的一部分
          (set-data cluster-state (storm-path storm-id)
                                  (-> base
                                      (merge new-elems)
                                      Utils/serialize))))

      ; [反序列化] 一个计算拓扑的基本信息, 返回StormBase对象
      (storm-base [this storm-id callback]
        (when callback
          (swap! storm-base-callback assoc storm-id callback))
        (maybe-deserialize (get-data cluster-state (storm-path storm-id) (not-nil? callback)))
        )

      (remove-storm-base! [this storm-id]
        (delete-node cluster-state (storm-path storm-id))
        )

      (set-assignment! [this storm-id info]
        (set-data cluster-state (assignment-path storm-id) (Utils/serialize info))
        )

      (remove-storm! [this storm-id]
        (delete-node cluster-state (assignment-path storm-id))
        (remove-storm-base! this storm-id))

      (report-error [this storm-id component-id error]                
         (let [path (error-path storm-id component-id)    ; component-id即builder.setSpout("spout",...)
               data {:time-secs (current-time-secs) :error (stringify-error error)}
               _ (mkdirs cluster-state path)              ; /storm/errors/topology-id/spout ...
               _ (create-sequential cluster-state (str path "/e") (Utils/serialize data))
               to-kill (->> (get-children cluster-state path false)
                            (sort-by parse-error-path)
                            reverse
                            (drop 10))]
           (doseq [k to-kill]
             (delete-node cluster-state (str path "/" k)))))

      (errors [this storm-id component-id]
         (let [path (error-path storm-id component-id)
               _ (mkdirs cluster-state path)
               children (get-children cluster-state path false)
               errors (dofor [c children]
                             (let [data (-> (get-data cluster-state (str path "/" c) false)
                                            maybe-deserialize)]
                               (when data
                                 (struct TaskError (:error data) (:time-secs data))
                                 )))
               ]
           (->> (filter not-nil? errors)
                (sort-by (comp - :time-secs)))))
      
      (disconnect [this]
        (unregister cluster-state state-id)
        (when solo?
          (close cluster-state)))
      )))

;; daemons have a single thread that will respond to events
;; start with initialize event
;; callbacks add events to the thread's queue

;; keeps in memory cache of the state, only for what client subscribes to. Any subscription is automatically kept in sync, and when there are changes, client is notified.
;; master gives orders through state, and client records status in state (ephemerally)

;; master tells nodes what workers to launch

;; master writes this. supervisors and workers subscribe to this to understand complete topology. each storm is a map from nodes to workers to tasks to ports whenever topology changes everyone will be notified
;; master includes timestamp of each assignment so that appropriate time can be given to each worker to start up
;; /assignments/{storm id}

;; which tasks they talk to, etc. (immutable until shutdown)
;; everyone reads this in full to understand structure
;; /tasks/{storm id}/{task id} ; just contains bolt id


;; supervisors send heartbeats here, master doesn't subscribe but checks asynchronously
;; /supervisors/status/{ephemeral node ids}  ;; node metadata such as port ranges are kept here 

;; tasks send heartbeats here, master doesn't subscribe, just checks asynchronously
;; /taskbeats/{storm id}/{ephemeral task id}

;; contains data about whether it's started or not, tasks and workers subscribe to specific storm here to know when to shutdown
;; master manipulates
;; /storms/{storm id}



;; Zookeeper flows:

;; Master:
;; job submit:
;; 1. read which nodes are available
;; 2. set up the worker/{storm}/{task} stuff (static)
;; 3. set assignments
;; 4. start storm - necessary in case master goes down, when goes back up can remember to take down the storm (2 states: on or off)

;; Monitoring (or by checking when nodes go down or heartbeats aren't received):
;; 1. read assignment
;; 2. see which tasks/nodes are up
;; 3. make new assignment to fix any problems
;; 4. if a storm exists but is not taken down fully, ensure that storm takedown is launched (step by step remove tasks and finally remove assignments)


;; masters only possible watches is on ephemeral nodes and tasks, and maybe not even

;; Supervisor:
;; 1. monitor /storms/* and assignments
;; 2. local state about which workers are local
;; 3. when storm is on, check that workers are running locally & start/kill if different than assignments
;; 4. when storm is off, monitor tasks for workers - when they all die or don't hearbeat, kill the process and cleanup

;; Worker:
;; 1. On startup, start the tasks if the storm is on

;; Task:
;; 1. monitor assignments, reroute when assignments change
;; 2. monitor storm (when storm turns off, error if assignments change) - take down tasks as master turns them off



;; locally on supervisor: workers write pids locally on startup, supervisor deletes it on shutdown (associates pid with worker name)
;; supervisor periodically checks to make sure processes are alive
;; {rootdir}/workers/{storm id}/{worker id}   ;; contains pid inside

;; all tasks in a worker share the same cluster state
;; workers, supervisors, and tasks subscribes to storm to know when it's started or stopped
;; on stopped, master removes records in order (tasks need to subscribe to themselves to see if they disappear)
;; when a master removes a worker, the supervisor should kill it (and escalate to kill -9)
;; on shutdown, tasks subscribe to tasks that send data to them to wait for them to die. when node disappears, they can die
