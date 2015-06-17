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
(ns backtype.storm.daemon.supervisor
  (:import [backtype.storm.scheduler ISupervisor])
  (:use [backtype.storm bootstrap])
  (:use [backtype.storm.daemon common])
  (:require [backtype.storm.daemon [worker :as worker]])
  (:gen-class
    :methods [^{:static true} [launch [backtype.storm.scheduler.ISupervisor] void]]))

(bootstrap)

(defmulti download-storm-code cluster-mode)
(defmulti launch-worker (fn [supervisor & _] (cluster-mode (:conf supervisor))))

;; used as part of a map from port to this
(defrecord LocalAssignment [storm-id executors])

(defprotocol SupervisorDaemon
  (get-id [this])
  (get-conf [this])
  (shutdown-all-workers [this])
  )

;; =========== mk-synchronize-supervisor ===================

;; 1) 从zk上读取assignments的快照信息: {storm-id assignment-info}. 并注册callback,在zk->assignment发生变化时被触发
;; assignment-info中用到了:master-code-dir, supervisor会到nimbus的这个目录下载topology代码
;; executor->node+port. supervisor会根据当前自己的节点id:supervisor-id只取node=supervisor-id的port->executors
(defn- assignments-snapshot [storm-cluster-state callback]
  (let [storm-ids (.assignments storm-cluster-state callback)]
     (->> (dofor [sid storm-ids] {sid (.assignment-info storm-cluster-state sid callback)})
          (apply merge)
          (filter-val not-nil?)
          )))

;; 2) 读取位于nimbus上的storm代码的路径,supervisor会下载到本地
;; 为什么要把:master-code-dir保存在assignment-info中. 因为nimbus和supervisor在集群中位于不同的机器.
;; supervisor要取下载nimbus上的topology代码. 如果不通过某个共享的记录来保存, supervisor就无法获取nimbus上的topology代码.
;; 这个共享的记录就是保存在zk上的assignment-info信息. 即/storm/assignments/topology-id保存的Assignment数据
(defn- read-storm-code-locations
  [assignments-snapshot]  ; {topology-id, assignment-info}
  ; (map-val aFn aMap). 其中aMap的value作为aFun的参数. 所以assignment-info会作为:master-code-dir的参数,
  ;   即(:master-code-info assignment-info) 而assignment-info定义在common.clj的Assignment记录字段
  ;   assignment-info的赋值操作是在nimbus.clj的mk-assignments里. 值为${storm.local.dir}/nimbus/stormdist/storm-id
  ; 返回值为{topology-id {:master-code-dir assignment-info}} 即topology-id和storm-code位置的映射
  (map-val :master-code-dir assignments-snapshot))

;; 3) 读取${storm.local.dir}/supervisor/stormdist下已经下载的storm-id列表
;; 如果supervisor从nimbus下载过topology代码,就会从nimbus/stormdist/topology-id下复制文件到supervisor/stormdist/topology-id下
(defn- read-downloaded-storm-ids [conf]
  ; (supervisor-stormdist-root conf) = ${storm.local.dir}/supervisor/stormdist
  ; (read-dir-contents dir)返回dir下所有的目录名称. 即所有的storm-id名称
  ; 最后对每个storm-id进行decode. 因为这是一个(map fn coll)结构.fn用#(%)简写,其中%表示coll的每个目录名称(循环的参数|变量)
  (map #(java.net.URLDecoder/decode %) (read-dir-contents (supervisor-stormdist-root conf)))
  )

;; 4) supervisor的port上被分配了哪些executors
(defn- read-assignments
  "Returns map from port to struct containing :storm-id and :executors"
  [assignments-snapshot assignment-id]
  ; for循环的[sid storm-ids]其中sid是循环每个storm-ids的变量
  ; let绑定的[sid (storm-id)]是将(storm-id)表达式的值赋值给sid
  ; (keys assignments-snapshot)返回的是storm-id集合
  (->> (dofor [sid (keys assignments-snapshot)] (read-my-executors assignments-snapshot sid assignment-id))
       ; 一个端口不能分配给多个不同的topology. 但可以分配给同一个topology的多个executor(尽管executor可以是不同的component).
       (apply merge-with (fn [& ignored] (throw-runtime "Should not have multiple topologies assigned to one port")))))

;; 从assignment中读取属于当前supervisor节点(node=assignment-id=supervisor-id)的port->executors
(defn- read-my-executors [assignments-snapshot storm-id assignment-id]
  (let [assignment (get assignments-snapshot storm-id)        ; assignment-info. 即Assignment记录
        my-executors (filter (fn [[_ [node _]]] (= node assignment-id)) ; assignment-id = supervisor-id = node
                       (:executor->node+port assignment)) ; 找出属于当前supervisor节点的executor->node+port. 根据node判断
        port-executors (apply merge-with
                         concat
                         (for [[executor [_ port]] my-executors] ; 现在我们知道my-executors就是当前节点,所以不在关心node字段
                           {port [executor]}                 ; 因为确定了当前节点,返回值不需要有node字段: port->executors
                           ))]
    (into {} (for [[port executors] port-executors]
               ;; need to cast to int b/c it might be a long (due to how yaml parses things)
               ;; doall is to avoid serialization/deserialization problems with lazy seqs
               ;; LocalAssignment是supervisor.clj中定义的一个记录. 最终的返回值:{port LocalAssignment(storm-id, executors)}
               ;; 如果是port->executors, 还无法确定这些exeuctors是属于哪个topology的,所以要加上storm-id. 并用LocalAssignment来封装
               [(Integer. port) (LocalAssignment. storm-id (doall executors))]
               ))))

;; 5) supervisor上被分配的topology id集合
;; 参数是个map: {port LocalAssignment[storm-id, executors]}
;; port1 -> LocalAssignment[storm-id1, executors]
;; port2 -> LocalAssignment[storm-id2, executors]
(defn assigned-storm-ids-from-port-assignments [assignment]
  (->> assignment
    vals             ; LocalAssignment
    (map :storm-id)  ; 取LocalAssignment的storm-id的值
    set))            ; 返回set

;; =============== sync-processes ==================

;; 获取worker的LocalState本地缓存中的WorkerHeartbeat
;; LocalState需要提供一个rootDir, 这里的rootDir就是每个${storm.local.dir}/workers/worker-id
;; 在这个目录下会有多个版本文件和token文件(.version结尾). 而每个LocalState都是一个Map类型.
;; 所以每个worker-id都会在自己的LocalState中保存key=LS-WORKER-HEARTBEAT,value=WorkerHeartbeat对象
(defn read-worker-heartbeat [conf id]
  ; 类似于保存在supervisor-data里的supervisor-state,也是一个LocalState对象
  ; ${storm.local.dir}/workers/$worker-id/heartbeats/$timestamp
  (let [local-state (worker-state conf id)]
    (.get local-state LS-WORKER-HEARTBEAT)
    ;; supervisor里没有对LS-LOCAL-ASSIGNMENTS进行set, 在worker.clj中set!
    ;; 它的值是一个WorkerHeartbeat, 也是定义在common.clj中的一个记录类型
    ))

;; 读取${storm.local.dir}/workers下的所有worker-id
(defn my-worker-ids [conf]
  (read-dir-contents (worker-root conf)))

(defn read-worker-heartbeats
  "Returns map from worker id to heartbeat"
  [conf]
  (let [ids (my-worker-ids conf)] ; 所有worker-id
    (into {}
      (dofor [id ids]             ; 对每个worker-id处理:
        [id (read-worker-heartbeat conf id)]))  ; {id WorkerHeartbeat}
    ))

;; worker-heartbeat是WorkerHeartbeat
;; assigned-executors是{port LocalAssignment}
(defn matches-an-assignment? [worker-heartbeat assigned-executors]
  ; (:port worker-heartbeat), 所以我们可以确定WorkerHeartbeat包含有:port属性
  ; 实际上这个记录定义在common.clj中: (defrecord WorkerHeartbeat [time-secs storm-id executors port])确实包含了port属性
  ; assigned-executors又是一个map, 所以(assigned-executors port)会取出LocalAssignment
  (let [local-assignment (assigned-executors (:port worker-heartbeat))]
    (and local-assignment
         ; LocalAssignment的组成是: [storm-id, executors]
         ; WorkerHeartbeat也包括了storm-id属性. 这两者要相等!
         (= (:storm-id worker-heartbeat) (:storm-id local-assignment))
         ; 同样LocalAssignment和WorkerHeartbeat也都包含了executors. 这两个值也要匹配!
         (= (disj (set (:executors worker-heartbeat)) Constants/SYSTEM_EXECUTOR_ID) ; 去除[-1 -1]
            (set (:executors local-assignment))))))
;; assigned-executors = {port LocalAssignment[storm-id, executors]}
;; WorkerHeartbeat = [time-secs storm-id executors port]  只比assigned-executors多了一个time-secs代表worker发生心跳的时间

;; 1) 读取当前已经分配的worker的状况
(defn read-allocated-workers
  "Returns map from worker id to worker heartbeat. if the heartbeat is nil, then the worker is dead (timed out or never wrote heartbeat)"
  [supervisor assigned-executors now]
  (let [conf (:conf supervisor)
        ^LocalState local-state (:local-state supervisor)
        id->heartbeat (read-worker-heartbeats conf)                         ;从local-state中读出每个worker的hb, 当然每个worker进程会不断的更新本地hb
        approved-ids (set (keys (.get local-state LS-APPROVED-WORKERS)))]   ;从local-state读出approved的worker
        ; 注意上面的local-state是supervisor的, 而read-worker-heartbeat是worker的
        ; supervisor的LocalState的rootDir是: $storm.local.dir/supervisor/localstate
        ; worker的LocalState的rootDir是: $storm.local.dir/workers/heartbeats
        ; LocalState顾名思义是本地(节点的)状态, 对于supervisor,保存的就是supervisor的状态信息. worker同理.

        ; 类似于同步topology代码之后, 会将<LS-LOCAL-ASSIGNMENTS,new-assignment>加入到supervisor的LocalState中 --> mk-synchronize-supervisor
        ; LS-APPROVED-WORKERS会在同步完worker processes后, 也加入到supervisor的LocalState中 --> 在sync-processes中put

        ; 这里只是预处理, 类似于同步supervisor时,要从supervisor的local-state中取出LS-LOCAL-ASSIGNMENTS的值: {port LocalAssignment}
        ; 因此对于worker的同步, 也要从supervisor的local-state中取出LS-APPROVED-WORKERS的值: WorkerHeartbeat
        ; 这种情况一般出现在topology重启的情况,如果是第一次任务分配,则worker都还没有开始建立呢,数据也就还没设置到local-state中.
    (into
     {}
     (dofor [[id hb] id->heartbeat]                       ;根据hb来判断worker的当前状态
            (let [state (cond                             ;state的取值就是右侧()表达式的值:有四种情况
                         (not hb)
                           :not-started                   ;无hb,没有start
                         (or (not (contains? approved-ids id))
                             (not (matches-an-assignment? hb assigned-executors)))
                           :disallowed                    ;不被允许
                         (> (- now (:time-secs hb))
                            (conf SUPERVISOR-WORKER-TIMEOUT-SECS))
                           :timed-out                     ;超时,dead
                         true
                           :valid)]
              (log-debug "Worker " id " is " state ": " (pr-str hb) " at supervisor time-secs " now)
              [id [state hb]]                             ;返回每个worker的当前state和hb
              ))
     )))

;; 等待worker执行. 只有worker启动了,才会设置LS-WORKER-HEARTBEAT到worker的LocalState中
;; check hb, 如果没有就不停的sleep, 至到超时, 打印failed to start
(defn- wait-for-worker-launch [conf id start-time]
  (let [state (worker-state conf id)]    
    (loop []
      (let [hb (.get state LS-WORKER-HEARTBEAT)]
        (when (and
               (not hb)     ; hb为空
               (<
                (- (current-time-secs) start-time)
                (conf SUPERVISOR-WORKER-START-TIMEOUT-SECS)
                ))
          (log-message id " still hasn't started")
          (Time/sleep 500)
          (recur)
          )))
    (when-not (.get state LS-WORKER-HEARTBEAT)
      (log-message "Worker " id " failed to start")
      )))

(defn- wait-for-workers-launch [conf ids]
  (let [start-time (current-time-secs)]
    (doseq [id ids]
      (wait-for-worker-launch conf id start-time))
    ))

(defn generate-supervisor-id []
  (uuid))

; 清理worker, 依次删除下列目录. 为什么不适用rm -rf $storm.local.dir/workers/$id?
; $storm.local.dir/workers/$id/heartbeats
; $storm.local.dir/workers/$id/pids
; $storm.local.dir/workers/$id
(defn try-cleanup-worker [conf id]
  (try
    ; DELETE $storm.local.dir/workers/$id/heartbeats
    (rmr (worker-heartbeats-root conf id))
    ;; this avoids a race condition with worker or subprocess writing pid around same time
    ; DELETE $storm.local.dir/workers/$id/pids
    (rmpath (worker-pids-root conf id))
    ; DELETE $storm.local.dir/workers/$id
    (rmpath (worker-root conf id))
  (catch RuntimeException e
    (log-warn-error e "Failed to cleanup worker " id ". Will retry later")
    )
  (catch java.io.FileNotFoundException e (log-message (.getMessage e)))
  (catch java.io.IOException e (log-message (.getMessage e)))
    ))

; 关闭worker, kill掉进程. id为worker的id.
(defn shutdown-worker [supervisor id]
  (log-message "Shutting down " (:supervisor-id supervisor) ":" id)
  (let [conf (:conf supervisor)
        ; $storm.local.dir/workers/$id/pids下的所有pid. 一般只有一个pid
        pids (read-dir-contents (worker-pids-root conf id))
        ; 本地模式执行worker时,会将{worker-id,pid}加入到worker-thread-pids-atom
        thread-pid (@(:worker-thread-pids-atom supervisor) id)]
    (when thread-pid
      (psim/kill-process thread-pid)) ; kill -9 pid
    (doseq [pid pids]
      (ensure-process-killed! pid)
      (try
        (rmpath (worker-pid-path conf id pid))  ; 删除$storm.local.dir/workers/#id/pids/#pid
        (catch Exception e)) ;; on windows, the supervisor may still holds the lock on the worker directory
      )
    (try-cleanup-worker conf id))     ; 清理worker
  (log-message "Shut down " (:supervisor-id supervisor) ":" id))

(defn supervisor-data [conf shared-context ^ISupervisor isupervisor]
  {:conf conf
   :shared-context shared-context
   :isupervisor isupervisor
   :active (atom true)
   :uptime (uptime-computer)
   :worker-thread-pids-atom (atom {})
   :storm-cluster-state (cluster/mk-storm-cluster-state conf)
   :local-state (supervisor-state conf)
   :supervisor-id (.getSupervisorId isupervisor)
   :assignment-id (.getAssignmentId isupervisor)
   :my-hostname (if (contains? conf STORM-LOCAL-HOSTNAME)
                  (conf STORM-LOCAL-HOSTNAME)
                  (local-hostname))
   :curr-assignment (atom nil) ;; used for reporting used ports when heartbeating
   :timer (mk-timer :kill-fn (fn [t]
                               (log-error t "Error when processing event")
                               (halt-process! 20 "Error when processing an event")
                               ))
   })

;; sync-processes用于管理workers, 比如处理不正常的worker或dead worker, 并创建新的workers
;; 首先从本地读出workers的hb, 来判断work状况, shutdown所有状态非valid的workers
;; 并为被assignment, 而worker状态非valid的slot, 创建新的worker
(defn sync-processes [supervisor]
  (let [conf (:conf supervisor)
        ^LocalState local-state (:local-state supervisor)
        ; {port LocalAssignment}, mk-synchronize-supervisor中,在下载完storm-code后,会将new-assignment设置到Map中key=LS-LOCAL-ASSIGNMENTS
        assigned-executors (defaulted (.get local-state LS-LOCAL-ASSIGNMENTS) {})
        now (current-time-secs)
        ; 1) 读取当前worker的状况, 返回值{worker-id [state WorkerHeartbeat]}
        allocated (read-allocated-workers supervisor assigned-executors now)
        keepers (filter-val                                       ;找出状态为valid的worker
                 (fn [[state _]] (= state :valid))
                 allocated)
        keep-ports (set (for [[id [_ hb]] keepers] (:port hb)))   ;keepers的ports集合

        ;;select-keys-pred(pred map), 对map中的key使用pred进行过滤
        ;;找出assigned-executors中executor的port, 哪些不属于keep-ports,
        ;;即找出新被assign的workers或那些虽被assign但状态不是valid的workers(dead或没有start)
        ;;这些executors需要重新分配到新的worker上去
        reassign-executors (select-keys-pred (complement keep-ports) assigned-executors)
        new-worker-ids (into {}
                        (for [port (keys reassign-executors)]     ;为reassign-executors的port产生新的worker-id
                          [port (uuid)]))                         ;{port worker-id}
        ]

    ;; 1. to kill are those in allocated that are dead or disallowed
    ;; 2. kill the ones that should be dead
    ;;     - read pids, kill -9 and individually remove file
    ;;     - rmr heartbeat dir, rmdir pid dir, rmdir id dir (catch exception and log)
    ;; 3. of the rest, figure out what assignments aren't yet satisfied
    ;; 4. generate new worker ids, write new "approved workers" to LS
    ;; 5. create local dir for worker id
    ;; 6. launch new workers (give worker-id, port, and supervisor-id)
    ;; 7. wait for workers launch
  
    (log-debug "Syncing processes")
    (log-debug "Assigned executors: " assigned-executors)
    (log-debug "Allocated: " allocated)
    (doseq [[id [state heartbeat]] allocated]
      (when (not= :valid state)
        (log-message
         "Shutting down and clearing state for id " id
         ". Current supervisor time: " now
         ". State: " state
         ", Heartbeat: " (pr-str heartbeat))
        (shutdown-worker supervisor id)                         ;;1.2.shutdown所有状态不是valid的worker
        ))

    ; new-worker-ids: {port worker-id}
    (doseq [id (vals new-worker-ids)]
      (local-mkdirs (worker-pids-root conf id)))                ;;5.为新的worker创建目录, 并加到local-state的LS-APPROVED-WORKERS中
    (.put local-state LS-APPROVED-WORKERS                       ;;4.更新的approved worker, 状态为valid的 + new workers
          (merge
           ; (select-keys {worker-id, port} worker-id), 返回值的结构也是{worker-id port}
           (select-keys (.get local-state LS-APPROVED-WORKERS)  ;;现有approved worker中状态为valid
                        (keys keepers))
           (zipmap (vals new-worker-ids) (keys new-worker-ids)) ;;{worker-id, port}
           ; (merge map1 map2) 两个map的结构是一样的,才可以进行合并.
           ; 所以我们可以确定LocalState中key=LS-APPROVED-WORKERS,它的value是Map: {worker-id, port}
           ))

    ;; 7.
    ;; 对reassign-executors中的每个new-work-id调用launch-worker
    ;; 最终调用wait-for-workers-launch, 等待worder被成功launch
    (wait-for-workers-launch
     conf
     (dofor [[port assignment] reassign-executors]              ;; {port LocalAssignment[storm-id,executors]}
       (let [id (new-worker-ids port)]                          ;; new-worker-ids是{port worker-id},所以id=worker-id
         (log-message "Launching worker with assignment "
                      (pr-str assignment)
                      " for this supervisor "
                      (:supervisor-id supervisor)
                      " on port "
                      port
                      " with id "
                      id
                      )
         (launch-worker supervisor                              ;; 6. 启动worker进程, 返回worker-id
                        (:storm-id assignment)
                        port
                        id)
         id)))
    ))

(defn shutdown-disallowed-workers [supervisor]
  (let [conf (:conf supervisor)
        ^LocalState local-state (:local-state supervisor)
        assigned-executors (defaulted (.get local-state LS-LOCAL-ASSIGNMENTS) {})
        now (current-time-secs)
        allocated (read-allocated-workers supervisor assigned-executors now)
        disallowed (keys (filter-val
                                  (fn [[state _]] (= state :disallowed))
                                  allocated))]
    (log-debug "Allocated workers " allocated)
    (log-debug "Disallowed workers " disallowed)
    (doseq [id disallowed]
      (shutdown-worker supervisor id))
    ))

;; mk-synchronize-supervisor, 比较特别的是内部用了一个有名字的匿名函数this来封装这个函数体
;; 刚开始看到非常诧异, 其实目的是为了可以在sync-callback中将这个函数add到event-manager里面去
;; 即每次被调用, 都需要再一次把sync-callback注册到zk, 以保证下次可以被继续触发

;; 参数说明
;; sync-processes和processes-event-manager用于worker processes的. 而这个方法从名称上看针对的是supervisor.
;; 那么为什么要把worker processes也放在这里实现呢. 因为workers是supervisor的组成部分. 执行supervisor主体部分,也要一起执行workers内部组件.
(defn mk-synchronize-supervisor [supervisor sync-processes event-manager processes-event-manager]
  (fn this []
    (let [conf (:conf supervisor)
          storm-cluster-state (:storm-cluster-state supervisor)
          ^ISupervisor isupervisor (:isupervisor supervisor)
          ^LocalState local-state (:local-state supervisor)               ;本地缓存数据库
          sync-callback (fn [& ignored] (.add event-manager this))        ;生成callback函数(后台执行mk-synchronize-supervisor)
          ;读取assignments,并注册callback,在zk->assignment发生变化时被触发
          assignments-snapshot (assignments-snapshot storm-cluster-state sync-callback)
          storm-code-map (read-storm-code-locations assignments-snapshot) ;从哪儿下载topology code
          downloaded-storm-ids (set (read-downloaded-storm-ids conf))     ;已经下载了哪些topology
          all-assignment (read-assignments                                ;supervisor的port上被分配了哪些executors
                           assignments-snapshot           ;{topology-id assignment-info}
                           (:assignment-id supervisor))   ;supervisor-id
          ;; 返回值为{port LocalAssignment[storm-id executors]}
          new-assignment (->> all-assignment                              ;new=all,因为confirmAssigned没有具体实现,always返回true
                              (filter-key #(.confirmAssigned isupervisor %)))
          assigned-storm-ids (assigned-storm-ids-from-port-assignments new-assignment)  ;supervisor上被分配的topology id集合
          existing-assignment (.get local-state LS-LOCAL-ASSIGNMENTS)]    ;从local-state数据库里面读出当前保存的local assignments
      (log-debug "Synchronizing supervisor")
      (log-debug "Storm code map: " storm-code-map)
      (log-debug "Downloaded storm ids: " downloaded-storm-ids)
      (log-debug "All assignment: " all-assignment)
      (log-debug "New assignment: " new-assignment)
      
      ;; download code first 下载新分配的topology代码
      ;; This might take awhile
      ;;   - should this be done separately from usual monitoring?
      ;;   - should we only download when topology is assigned to this supervisor?
      (doseq [[storm-id master-code-dir] storm-code-map]
        (when (and (not (downloaded-storm-ids storm-id))  ; storm-id不在已经下载的集合中
                   (assigned-storm-ids storm-id))         ; storm-id在被分配的集合中
          (log-message "Downloading code for storm id " storm-id " from " master-code-dir)
          (download-storm-code conf storm-id master-code-dir)
          (log-message "Finished downloading code for storm id " storm-id " from " master-code-dir)
          ))

      (log-debug "Writing new assignment " (pr-str new-assignment))
      (doseq [p (set/difference (set (keys existing-assignment))
                                (set (keys new-assignment)))]
        (.killedWorker isupervisor (int p)))                  ; 空的实现
      (.assigned isupervisor (keys new-assignment))           ; 空的实现

      (.put local-state                                       ;把new-assignment存到local-state数据库中
            LS-LOCAL-ASSIGNMENTS
            new-assignment)                                   ;{port LocalAssignment[storm-id,executors]}
      (reset! (:curr-assignment supervisor) new-assignment)   ;把new-assignment cache到supervisor对象中

      ;; 删除无用的topology code
      ;; remove any downloaded code that's no longer assigned or active
      ;; important that this happens after setting the local assignment so that
      ;; synchronize-supervisor doesn't try to launch workers for which the
      ;; resources don't exist
      (if on-windows? (shutdown-disallowed-workers supervisor))
      (doseq [storm-id downloaded-storm-ids]
        (when-not (assigned-storm-ids storm-id)
          (log-message "Removing code for storm id "
                       storm-id)
          (try
            (rmr (supervisor-stormdist-root conf storm-id))
            (catch Exception e (log-message (.getMessage e))))
          ))

      ;;后台执行sync-processes
      (.add processes-event-manager sync-processes)
      )))

;; in local state, supervisor stores who its current assignments are
;; another thread launches events to restart any dead processes if necessary
(defserverfn mk-supervisor [conf shared-context ^ISupervisor isupervisor]
  (log-message "Starting Supervisor with conf " conf)
  (.prepare isupervisor conf (supervisor-isupervisor-dir conf))       ;初始化supervisor-id,并存在localstate中(参考ISupervisor的实现)
  (FileUtils/cleanDirectory (File. (supervisor-tmp-dir conf)))        ;清空本机的supervisor目录
  (let [supervisor (supervisor-data conf shared-context isupervisor)
        ;; 创建两个event-manager,用于在后台执行function
        [event-manager processes-event-manager :as managers] [(event/event-manager false) (event/event-manager false)]                         
        sync-processes (partial sync-processes supervisor)
        synchronize-supervisor (mk-synchronize-supervisor supervisor sync-processes event-manager processes-event-manager)

        ;; 定义生成supervisor hb的funciton: 会往zk的/storm/supervisors/supervisor-id节点写入SupervisorInfo
        heartbeat-fn (fn [] (.supervisor-heartbeat!
                               (:storm-cluster-state supervisor)
                               (:supervisor-id supervisor)
                               ; (defrecord SupervisorInfo [time-secs hostname assignment-id used-ports meta scheduler-meta uptime-secs])
                               (SupervisorInfo. (current-time-secs)                   ;hb时间
                                                (:my-hostname supervisor)             ;机器名
                                                (:assignment-id supervisor)           ;assignment-id = supervisor-id, 每个supervisor生成的uuid
                                                (keys @(:curr-assignment supervisor)) ;supervisor上当前使用的ports (curr-assignment, port->executors)
                                                ;; used ports
                                                (.getMetadata isupervisor)            ;在conf里面配置的supervisor的ports
                                                (conf SUPERVISOR-SCHEDULER-META)      ;用户在conf里面配置的supervior相关的metadata,比如name,可以任意kv
                                                ((:uptime supervisor)))))]            ;closeover了supervisor启动时间的fn, 调用可以算出uptime, 正常运行时间
    ;; 先调用heartbeat-fn发送一次supervisor的hb
    (heartbeat-fn)
    ;; should synchronize supervisor so it doesn't launch anything after being down (optimization)
    ;; 接着使用schedule-recurring去定期调用heartbeat-fn更新hb
    (schedule-recurring (:timer supervisor)
                        0
                        (conf SUPERVISOR-HEARTBEAT-FREQUENCY-SECS)
                        heartbeat-fn)
    (when (conf SUPERVISOR-ENABLE)
      ;; This isn't strictly necessary, but it doesn't hurt and ensures that the machine stays up
      ;; to date even if callbacks don't all work exactly right
      (schedule-recurring (:timer supervisor) 0 10
        (fn [] (.add event-manager synchronize-supervisor)))
      (schedule-recurring (:timer supervisor) 0 (conf SUPERVISOR-MONITOR-FREQUENCY-SECS)
        (fn [] (.add processes-event-manager sync-processes))))
    (log-message "Starting supervisor with id " (:supervisor-id supervisor) " at host " (:my-hostname supervisor))
    (reify
     Shutdownable
     (shutdown [this]
               (log-message "Shutting down supervisor " (:supervisor-id supervisor))
               (reset! (:active supervisor) false)                ; 设置supervisor的状态为不活动
               (cancel-timer (:timer supervisor))                 ; 取消定时器
               (.shutdown event-manager)                          ; 关闭两个事件管理器(同步)
               (.shutdown processes-event-manager)
               (.disconnect (:storm-cluster-state supervisor)))   ; 和zk断开连接
     SupervisorDaemon
     (get-conf [this]
       conf)
     (get-id [this]
       (:supervisor-id supervisor))
     (shutdown-all-workers [this]
       (let [ids (my-worker-ids conf)]
         (doseq [id ids]
           (shutdown-worker supervisor id)
           )))
     DaemonCommon
     (waiting? [this]
       (or (not @(:active supervisor))
           (and
            (timer-waiting? (:timer supervisor))
            (every? (memfn waiting?) managers)))
           ))))

(defn kill-supervisor [supervisor]
  (.shutdown supervisor)
  )

;; distributed implementation

(defmethod download-storm-code
    :distributed [conf storm-id master-code-dir]
    ;; Downloading to permanent location is atomic
    (let [tmproot (str (supervisor-tmp-dir conf) file-path-separator (uuid))
          stormroot (supervisor-stormdist-root conf storm-id)]
      (FileUtils/forceMkdir (File. tmproot))
      
      (Utils/downloadFromMaster conf (master-stormjar-path master-code-dir) (supervisor-stormjar-path tmproot))
      (Utils/downloadFromMaster conf (master-stormcode-path master-code-dir) (supervisor-stormcode-path tmproot))
      (Utils/downloadFromMaster conf (master-stormconf-path master-code-dir) (supervisor-stormconf-path tmproot))
      (extract-dir-from-jar (supervisor-stormjar-path tmproot) RESOURCES-SUBDIR tmproot)
      (FileUtils/moveDirectory (File. tmproot) (File. stormroot))
      ))

;; 使用java命令启动worker进程. 会调用worker.clj的main方法
(defmethod launch-worker
    :distributed [supervisor storm-id port worker-id]
    (let [conf (:conf supervisor)
          storm-home (System/getProperty "storm.home")
          stormroot (supervisor-stormdist-root conf storm-id)
          stormjar (supervisor-stormjar-path stormroot)
          storm-conf (read-supervisor-storm-conf conf storm-id)
          classpath (add-to-classpath (current-classpath) [stormjar])
          childopts (.replaceAll (str (conf WORKER-CHILDOPTS) " " (storm-conf TOPOLOGY-WORKER-CHILDOPTS))
                                 "%ID%"
                                 (str port))
          logfilename (str "worker-" port ".log")
          command (str "java -server " childopts
                       " -Djava.library.path=" (conf JAVA-LIBRARY-PATH)
                       " -Dlogfile.name=" logfilename
                       " -Dstorm.home=" storm-home
                       " -Dlogback.configurationFile=" storm-home "/logback/cluster.xml"
                       " -Dstorm.id=" storm-id
                       " -Dworker.id=" worker-id
                       " -Dworker.port=" port
                       " -cp " classpath " backtype.storm.daemon.worker "
                       (java.net.URLEncoder/encode storm-id) " " (:assignment-id supervisor)
                       " " port " " worker-id)]
      (log-message "Launching worker with command: " command)
      ;; 在util.clj中,调用ProcessBuilder.start启动上面的shell命令
      (launch-process command :environment {"LD_LIBRARY_PATH" (conf JAVA-LIBRARY-PATH)})
      ))

;; local implementation

(defn resources-jar []
  (->> (.split (current-classpath) File/pathSeparator)
       (filter #(.endsWith  % ".jar"))
       (filter #(zip-contains-dir? % RESOURCES-SUBDIR))
       first ))

(defmethod download-storm-code
    :local [conf storm-id master-code-dir]
  ; stormroot = $storm.local.dir/supervisor/stormdist/#storm-id
  (let [stormroot (supervisor-stormdist-root conf storm-id)]
      ; master-code-dir = $storm.local.dir/nimbus/stormdist/#storm-id
      (FileUtils/copyDirectory (File. master-code-dir) (File. stormroot))
      (let [classloader (.getContextClassLoader (Thread/currentThread))
            resources-jar (resources-jar)
            url (.getResource classloader RESOURCES-SUBDIR)
            ; $storm.local.dir/supervisor/stormdist/#storm-id/resources
            target-dir (str stormroot file-path-separator RESOURCES-SUBDIR)]
            (cond
              resources-jar
              (do
                (log-message "Extracting resources from jar at " resources-jar " to " target-dir)
                (extract-dir-from-jar resources-jar RESOURCES-SUBDIR stormroot))
              url
              (do
                (log-message "Copying resources at " (str url) " to " target-dir)
                (FileUtils/copyDirectory (File. (.getFile url)) (File. target-dir))
                ))
            )))

;; 对于本地, 直接调用worker/mk-worker创建worker
(defmethod launch-worker
    :local [supervisor storm-id port worker-id]
    (let [conf (:conf supervisor)
          pid (uuid)
          worker (worker/mk-worker conf
                                   (:shared-context supervisor)
                                   storm-id
                                   (:assignment-id supervisor)
                                   port
                                   worker-id)]
      (psim/register-process pid worker)
      ;; worker-thread-pids-atom是一个原子map: {worker-id pid}
      (swap! (:worker-thread-pids-atom supervisor) assoc worker-id pid)
      ))

(defn -launch [supervisor]
  (let [conf (read-storm-config)]         ; 读取storm配置信息
    (validate-distributed-mode! conf)     ; 验证分布式模式
    (mk-supervisor conf nil supervisor))) ; 创建supervisor. 参数supervisor是ISupervisor匿名实现类

(defn standalone-supervisor []
  (let [conf-atom (atom nil)
        id-atom (atom nil)]
    (reify ISupervisor                    ; 匿名ISupervisor实现类.类似于回调.需要真正的调用者传入具体的参数
      (prepare [this conf local-dir]
        (reset! conf-atom conf)           ; conf-atom来自于conf.即prepare的参数conf
        (let [state (LocalState. local-dir) ; 保存在本地状态里
              curr-id (if-let [id (.get state LS-ID)]
                        id
                        (generate-supervisor-id))]
          (.put state LS-ID curr-id)
          (reset! id-atom curr-id))
        )
      (confirmAssigned [this port]
        true)
      (getMetadata [this]
        (doall (map int (get @conf-atom SUPERVISOR-SLOTS-PORTS))))  ; 元数据即supervisor的端口列表
      (getSupervisorId [this]
        @id-atom)
      (getAssignmentId [this]
        @id-atom)
      (killedWorker [this port]
        )
      (assigned [this ports]
        ))))

(defn -main []
  (-launch (standalone-supervisor)))
