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
(ns backtype.storm.daemon.worker
  (:use [backtype.storm.daemon common])
  (:use [backtype.storm bootstrap])
  (:require [backtype.storm.daemon [executor :as executor]])
  (:import [java.util.concurrent Executors])
  (:import [backtype.storm.messaging TransportFactory])
  (:import [backtype.storm.messaging IContext IConnection])
  (:gen-class))

(bootstrap)

(defmulti mk-suicide-fn cluster-mode)

;; ++++++++++++++++++ I. worker-data BEGIN---------------------------

;;从assignments里面找出分配给这个worker的executors, 另外加上个SYSTEM_EXECUTOR
;;port是当前worker在supervisor中的端口
;;通过supervisor-id可以确定有哪些executor->node+port. 其中node=supervisor-id
;;因为worker运行在指定端口,根据对应的端口,可以找出运行在指定端口的executors.
(defn read-worker-executors [storm-conf storm-cluster-state storm-id assignment-id port]
  (let [assignment (:executor->node+port (.assignment-info storm-cluster-state storm-id nil))]
    (doall
     (concat     
      [Constants/SYSTEM_EXECUTOR_ID]
      (mapcat (fn [[executor loc]]                ; executor->node+port. loc=node+port
                (if (= loc [assignment-id port])  ; loc = assignment+port
                  [executor]                      ; 找出assignment-info中属于指定supervisor-id,并且等于当前端口的executors
                  ))                              ; 返回值就是[executor]
              assignment)))))

;; 返回fn①, 该fn会将tuple-batch②里面的tuples, 按task所对应的executor③发送到对应的接收队列④
(defn mk-transfer-local-fn [worker]
  (let [short-executor-receive-queue-map (:short-executor-receive-queue-map worker) ; {start-task queue}
        task->short-executor (:task->short-executor worker)                         ; {task-id start-task}
        ; def fns = (comp fn1 fn2)返回的是一系列函数的组合.
        ; (fns a), 会将参数a依次赋给fn2, fn1
        task-getter (comp #(get task->short-executor %) fast-first)]
    ; ① 返回的是一个函数. 接收的参数是一个List. 比如((mk-transfer-local-fn worker) aList) = (local-transfer aList)
    (fn [tuple-batch]   ; ② 接收tuple-batch
      ; 在fast-group-by的实现中, task-getter接收tuple-batch这个list中的每个元素e. 所以e首先传给fast-first.
      ; 而fast-first接收的参数是list. 所以e也是list. 所以tuple-batch是一个List<List<>>的结构.(实际上是List<Array[task-id,tuple]>)
      ; fast-first的结果是取Array[task-id tuple]的第一个元素即task-id. 然后把task-id传给#(get task->short-executor %)的%
      ; 即task->short-executor.get(task-id), 返回的是short-executor,即executor的start-task

      ; fast-group-by的实现中e=pair, afn(e)=short-executor作为key. ret是个Map. map的key就是short-executor.
      ; map的value的默认值是ArrayList, 即curr. ArrayList里的元素是e, 即pair.
      ; 所以最终ret=grouped的结构是: {short-executor ArrayList<Array[task tuple]>}
      ; 对照原始的tuple-batch的结构: ArrayList<Array[task tuple]>. 多的这个short-executor来自于:
      ; 根据Array[task tuple]中的task 取task->short-executor中对应的short-executor! BINGO!
      ; 现在看看为什么要叫grouped, 因为一个executor里有多个task. 而一个executor的short-executor即start-task是唯一的. 所以有分组的这种概念在里面.
      ; 假设某个executor为[1 3]. 则这个executor有3个task. short-executor=start-task=1. 而每个task都对应一个tuple.
      ; 所以grouped的结果为: {1 '(pair1, pair2, pair3)} = {1 '([task1 tuple1], [task2 tuple2], [task3 tuple3])}
      (let [grouped (fast-group-by task-getter tuple-batch)]          ; ③ task对应的short-executor进行分组
        ; 其实从下面的for循环: [[short-executor pairs] grouped]也验证了grouped中的每个元素的形式是:
        ; {short-executor pairs} = {short-executor ArrayList<pair>} = {short-executor ArrayList<Array[task,tuple]>}
        ; NOW, YOU SEE TASK AND TUPLE. BUT WHAT TUPLE IS?  TUPLE RUN ON TASK. TASK RUN ON EXECUTOR --> WORKER --> SUPERVISOR ~~
        (fast-map-iter [[short-executor pairs] grouped]               ; for循环的[e list],其中e是list的每个元素
          (let [q (short-executor-receive-queue-map short-executor)]  ; (map key) --> queue
            (if q
              (disruptor/publish q pairs)                             ; ④ 发送到消息队列
              (log-warn "Received invalid messages for unknown tasks. Dropping... ")
              )))))))

;; 为了知道mk-transfer-local-fn的fn[tuple-batch]里的几个fast-groiup-by, fast-map-iter是怎么运行的. 需要知道tuple-batch的结构是什么样的
;; worker-data里保存(:transfer-local-fn mk-transfer-local-fn), 使用:transfer-local-fn的地方在mk-transfer-fn ①
;; (:transfer-local-fn worker)的返回值是一个函数. 因为mk-transfer-local-fn的返回值是(fn ..)
;; 将这个函数赋值给local-transfer. 并可以调用(local-transfer local). 也就是说local会作为mk-transfer-local-fn的fn里的tuple-batch参数. ②

;; 所以要知道local的结构. local首先是个ArrayList. ③
;; 那么list里的每个元素是什么类型? (.add local pair). 即pair是local这个list里的每个元素的类型 ④
;; pair是一个[task tuple]数组. 所以我们知道tuple-batch是List<Array[task,tuple]>结构.

;; mk-transfer-fn的local里面的每个元素pair的来源是mk-transfer-fn的tuple-batch. 而这个参数也是mk-transfer-fn的fn[tuple-batch] ⑤
;; 所以调用mk-transfer-fn的函数才是原始的数据源.  同样从worker-data里获取:transfer-fn, 使用的地方在哪里呢?
;; ? --> mk-transfer-fn --> mk-transfer-local-fn
(defn mk-transfer-fn [worker]
  (let [local-tasks (-> worker :task-ids set)
        local-transfer (:transfer-local-fn worker)                ; ① 使用worker-data里的:transfer-local-fn, 返回值是一个函数
        ^DisruptorQueue transfer-queue (:transfer-queue worker)]
    (fn [^KryoTupleSerializer serializer tuple-batch]             ; ⑤ 这里的tuple-batch来源于哪里呢?
      (let [local (ArrayList.)      ; ③ local is a list
            remote (ArrayList.)]
        (fast-list-iter [[task tuple :as pair] tuple-batch]       ; 这里的tuple-batch构成pair,即local的每个元素,进而传递给mk-transfer-local-fn的fn的tuple-batch
          (if (local-tasks task)
            (.add local pair)       ; ④ local的每个元素是一个pair. pair是一个数组:[task tuple]. 所以local的结构是:List<Array[]>
            (.add remote pair)
            ))
        (local-transfer local)      ; ② local是一个list. 调用的是mk-transfer-local-fn的fn[tuple-batch], 其中list=tuple-batch
        ;; not using map because the lazy seq shows up in perf profiles

        ; remote的结构和local类似,也是一个ArrayList. 即remote中的每个元素是[task tuple]
        ; 将tuple序列化后,传输给transfer-queue
        (let [serialized-pairs (fast-list-for [[task ^TupleImpl tuple] remote] [task (.serialize serializer tuple)])]
          (disruptor/publish transfer-queue serialized-pairs)
          )))))

(defn- mk-receive-queue-map [storm-conf executors]
  (->> executors
       ;; TODO: this depends on the type of executor
       (map (fn [e] [e (disruptor/disruptor-queue (storm-conf TOPOLOGY-EXECUTOR-RECEIVE-BUFFER-SIZE)
                                                  :wait-strategy (storm-conf TOPOLOGY-DISRUPTOR-WAIT-STRATEGY))]))
       (into {})
       ))

(defn- stream->fields [^StormTopology topology component]
  (->> (ThriftTopologyUtils/getComponentCommon topology component)
       .get_streams                                                   ; Map<string, StreamInfo> streams. key is stream-id
       ; 将StreamInfo 转为 Fields. 返回值为{stream-id Fields}
       ; stream-id是在哪里定义的? 我们知道component-id是由自定义topology中指定的. stream-id应该是系统内部自动生成的吧.
       (map (fn [[s info]] [s (Fields. (.get_output_fields info))]))  ; backtype/storm/tuple/Fileds.java
       (into {})
       (HashMap.)))

(defn component->stream->fields [^StormTopology topology]
  (->> (ThriftTopologyUtils/getComponentIds topology) ; topology的所有component的id
       (map (fn [c] [c (stream->fields topology c)])) ; 根据component-id读取对应的Fields
       (into {})
       (HashMap.)))

(defn- mk-default-resources [worker]
  (let [conf (:conf worker)
        ; The size of the shared thread pool for worker tasks to make use of. The thread pool can be accessed via the TopologyContext.
        ; 共享资源池的大小: worker中运行着多个task, 这些tasks可以使用线程池来处理消息. 可以通过TopologyContext获取这个线程池
        thread-pool-size (int (conf TOPOLOGY-WORKER-SHARED-THREAD-POOL-SIZE))]
    {WorkerTopologyContext/SHARED_EXECUTOR (Executors/newFixedThreadPool thread-pool-size)}
    ))

(defn- mk-user-resources [worker]
  ;;TODO: need to invoke a hook provided by the topology, giving it a chance to create user resources.
  ;; this would be part of the initialization hook
  ;; need to separate workertopologycontext into WorkerContext and WorkerUserContext.
  ;; actually just do it via interfaces. just need to make sure to hide setResource from tasks
  {})

(defn mk-halting-timer []
  (mk-timer :kill-fn (fn [t]
                       (log-error t "Error when processing event")
                       (halt-process! 20 "Error when processing an event")
                       )))

;; assignment-id = supervisor-id
(defn worker-data [conf mq-context storm-id assignment-id port worker-id]
  (let [cluster-state (cluster/mk-distributed-cluster-state conf)
        storm-cluster-state (cluster/mk-storm-cluster-state cluster-state)
        storm-conf (read-supervisor-storm-conf conf storm-id)
        ;;从assignments里面找出分配给这个worker的executors, 另外加上个SYSTEM_EXECUTOR
        executors (set (read-worker-executors storm-conf storm-cluster-state storm-id assignment-id port))
        ;;基于disruptor创建worker用于接收和发送messgae的buffer queue
        ;;创建基于disruptor的transfer-queue
        transfer-queue (disruptor/disruptor-queue (storm-conf TOPOLOGY-TRANSFER-BUFFER-SIZE)
                                                  :wait-strategy (storm-conf TOPOLOGY-DISRUPTOR-WAIT-STRATEGY))
        ;;对于每个executors创建receive-queue(基于disruptor-queue),并生成{executor,queue}的map返回
        executor-receive-queue-map (mk-receive-queue-map storm-conf executors)
        ;;executor可能有多个tasks,相同executor的tasks公用一个queue, 将{e,queue}转化为{t,queue}
        ; 假设某个executor=[4 6], 则executor-receive-queue-map对应元素为{[4 6] queue1}
        ; receive-queue-map则对应了三个元素: {4 queue1, 5 queue1, 6 queue1}
        receive-queue-map (->> executor-receive-queue-map
                               ; fn的[e queue]是executor-receive-queue-map的每个元素, 即{executor queue}
                               ; for中的t是executor的每个task.即从executor的start-task到end-task的每个元素
                               (mapcat (fn [[e queue]] (for [t (executor-id->tasks e)] [t queue])))
                               (into {}))
        ;;读取supervisor机器上存储的stormcode.ser (topology对象的序列化文件). supervisor同步时会从nimbus拷贝到supervisor本地
        topology (read-supervisor-topology conf storm-id)]
    ;;recursive-map,会将底下value都执行一遍, 用返回值和key生成新的map
    (recursive-map
      :conf conf
      :mq-context (if mq-context
                      mq-context
                      (TransportFactory/makeContext storm-conf))  ;;已经prepare的具有IContext接口的对象
      ;; 静态信息
      :storm-id storm-id
      :assignment-id assignment-id
      :port port
      :worker-id worker-id
      :cluster-state cluster-state
      :storm-cluster-state storm-cluster-state
      :storm-active-atom (atom false)

      ;; 动态信息
      :executors executors
      :task-ids (->> receive-queue-map keys (map int) sort)
      :storm-conf storm-conf
      :topology topology
      :system-topology (system-topology! storm-conf topology)

      ;; 定时器
      :heartbeat-timer (mk-halting-timer)             ; worker的心跳
      :refresh-connections-timer (mk-halting-timer)   ; 刷新连接
      :refresh-active-timer (mk-halting-timer)
      :executor-heartbeat-timer (mk-halting-timer)    ; executor的心跳
      :user-timer (mk-halting-timer)

      ;; component逻辑相关
      :task->component (HashMap. (storm-task-info topology storm-conf))               ; for optimized access when used in tasks later on
      :component->stream->fields (component->stream->fields (:system-topology <>))    ;;从ComponentCommon中读出steams的fields信息
      :component->sorted-tasks (->> (:task->component <>) reverse-map (map-val sort))
      :endpoint-socket-lock (mk-rw-lock)
      :cached-node+port->socket (atom {})                                             ; node+port就表示当前worker所在的节点和端口.
      :cached-task->node+port (atom {})                                               ; worker上运行了多个task, 所以会有多个Socket通道

      :transfer-queue transfer-queue
      :executor-receive-queue-map executor-receive-queue-map                          ; {[start-task end-task] queue}
      ; 单纯为了简化executor的表示, 由[first-task,last-task]变为first-task : {start-task queue}
      ; executor=[1 3], 则short-executor=(first executor)=1. 所以下面的两个short-executor都是指executor的简化版本
      ; 即short-executor和receive-queue的映射. task和short-executor的映射
      :short-executor-receive-queue-map (map-key first executor-receive-queue-map)
      :task->short-executor (->> executors                                            ; 列出task和简化后的short-executor的对应关系
                                 (mapcat (fn [e] (for [t (executor-id->tasks e)] [t (first e)])))
                                 (into {})            ; {task-id start-task}
                                 (HashMap.))          ; 假设executor=[1 3], 所有的task=[1 2 3], 返回值为:{1 1, 2 1, 3 1}

      :suicide-fn (mk-suicide-fn conf)
      :uptime (uptime-computer)
      ; -- 使用<>, 代表的是worker-data的返回值. 可以看mk-default-resources的参数就是worker. 其他同理
      ; -- 上面的(:system-topology <>) 等价于(:system-topology worker), 因为:system-topology就是worker-data的返回值的一个key
      :default-shared-resources (mk-default-resources <>)
      :user-shared-resources (mk-user-resources <>)
      :transfer-local-fn (mk-transfer-local-fn <>)    ; 接收messages并发到task对应的接收队列
      :transfer-fn (mk-transfer-fn <>)                ; 将处理过的message放到发送队列transfer-queue
      )))
;; ++++++++++++++++++ I. worker-data END---------------------------

;; ++++++++++++++++ II. heartbeat BEGIN ----------------------

; 1.2.2. 将worker hb同步到zk, 以便nimbus可以立刻知道worker已经启动
; 通过worker-heartbeat!将worker hb写入zk的workerbeats目录
(defnk do-executor-heartbeats [worker :executors nil]
  ;; stats is how we know what executors are assigned to this worker
  (let [stats (if-not executors
                (into {} (map (fn [e] {e nil}) (:executors worker)))
                (->> executors
                  (map (fn [e] {(executor/get-executor-id e) (executor/render-stats e)}))
                  (apply merge)))
        zk-hb {:storm-id (:storm-id worker)
               :executor-stats stats        ; executor.clj的RunningExecutor的两个属性
               :uptime ((:uptime worker))
               :time-secs (current-time-secs)
               }]
    ;; do the zookeeper heartbeat
    ;; worker所属的storm-id, worker所在的supervisor-id, worker占用的端口
    ;; 最后一个参数也是map. 和nimbus: StormBase,Assignment; supervisor: SupervisorInfo使用记录不同
    ;; 其中前三个参数用来确定zk上worker的路径: /storm/workerbeats/#storm-id/#node-port, 最后一个参数为worker节点的序列化数据
    (.worker-heartbeat! (:storm-cluster-state worker) (:storm-id worker) (:assignment-id worker) (:port worker) zk-hb)
    ))

; 1.2.1. 建立worker本地的hb
; 调用do-heartbeat, 将worker的hb写到本地的localState数据库中
(defn do-heartbeat [worker]
  (let [conf (:conf worker)
        hb (WorkerHeartbeat.
             (current-time-secs)
             (:storm-id worker)
             (:executors worker)
             (:port worker))
        state (worker-state conf (:worker-id worker))]
    (log-debug "Doing heartbeat " (pr-str hb))
    ;; do the local-file-system heartbeat.
    (.put state LS-WORKER-HEARTBEAT hb false)
    (.cleanup state 60) ; this is just in case supervisor is down so that disk doesn't fill up.
    ; it shouldn't take supervisor 120 seconds between listing dir and reading it

    ))

;; ++++++++++++++++ II. heartbeat END ----------------------

;; ++++++++++++++++ III. refresh BEGIN ----------------------

; a. 找出该worker下需要往其他task发送数据的task, outbound-tasks
; worker-outbound-tasks, 找出当前work中的task①属于的component②, 并找出该component的目标component③
; 最终找出目标compoennt所对应④的所有task⑤, 作为返回
(defn worker-outbound-tasks
  "Returns seq of task-ids that receive messages from this worker"
  [worker]
  (let [context (worker-context worker)           ; WorkerTopologyContext
        components (mapcat
                     (fn [task-id]
                       ; 当前worker的所有task, 每个task所属的component ②
                       (->> (.getComponentId context (int task-id)) ; 根据task-id和component-id的映射获取component-id
                         ; component的目标component. ③ 所以components返回值表示worker -> task-ids -> component -> target component
                         (.getTargets context)    ; context.getTargets(component-id) => {stream-id, {component-id, Grouping}}
                         vals                     ; {component-id, Grouping}
                         (map keys)               ; component-id
                         (apply concat)))
                     (:task-ids worker))]         ; 当前worker的所有task ①
    (-> worker
      :task->component                            ; (:task->component worker)  注意是topology级别的映射,而不是说属于worker的映射
      reverse-map                                 ; component-id->task
      ; (select-keys component-id->task components)  选择component-id->task中key在components中的数据. 注意components是worker的target component!
      ; 即选择的范围是: topology级别的topology-id->task, 选择的依据是components, 即当前worker的所有task对应的component对应的目标component.
      (select-keys components)                    ; 找出目标compoennt所对应的component-id->task ④
      vals                                        ; 取val即返回task-ids ⑤
      flatten
      set )))

(defn- endpoint->string [[node port]]
  (str port "/" node))

(defn string->endpoint [^String s]
  (let [[port-str node] (.split s "/" 2)]
    [node (Integer/valueOf port-str)]
    ))

;; ====== 1.3 维护和更新worker的发送connection
;; mk-refresh-connections定义并返回一个匿名函数, 但是这个匿名函数, 定义了函数名this, 这个情况前面也看到, 是因为这个函数本身要在函数体内被使用.
;; 并且refresh-connections是需要反复被执行的, 即当每次assignment-info发生变化的时候, 就需要refresh一次
;; 所以这里使用timer.schedule-recurring就不合适, 因为不是以时间触发. 这里使用的是zk的callback触发机制

  ;; Supervisor的mk-synchronize-supervisor, 以及worker的mk-refresh-connections, 都采用类似的机制
  ;; a. 首先需要在每次assignment改变的时候被触发, 所以都利用zk的watcher
  ;; b. 都需要将自己作为callback, 并在获取assignment时进行注册, 都使用(fn this [])
  ;; c. 因为比较耗时, 都选择后台执行callback, 但是mk-synchronize-supervisor使用的是eventmanager, mk-refresh-connections使用的是timer
  ;; 两者不同, timer是基于优先级队列, 所以更灵活, 可以设置延时时间, 而eventmanager, 就是普通队列实现, FIFO
  ;; 另外, eventmanager利用reify来封装接口, 返回的是record, 比timer的实现要优雅些

; a. 找出该worker下需要往其他task发送数据的task, outbound-tasks
;    worker-outbound-tasks, 找出当前work中的task属于的component, 并找出该component的目标component
;    最终找出目标compoennt所对应的所有task, 作为返回
; b. 找出outbound-tasks对应的tasks->node+port, my-assignment
; c. 如果outbound-tasks在同一个worker进程中, 不需要建connection, 所以排除掉, 剩下needed-assignment
;    :value –> needed-connections , :key –> needed-tasks
; d. 和当前已经创建并cache的connection集合对比一下, 找出new-connections和remove-connections
; e. 调用Icontext.connect, (.connect ^IContext (:mq-context worker) storm-id ((:node->host assignment) node) port),
;    创建新的connection, 并merge到:cached-node+port->socket中
; f. 使用my-assignment更新:cached-task->node+port (结合:cached-node+port->socket, 就可以得到task->socket)
; g. close所有remove-connections, 并从:cached-node+port->socket中删除
(defn mk-refresh-connections [worker]
  (let [outbound-tasks (worker-outbound-tasks worker)                                     ;;a.找出该woker需要向哪些component tasks发送数据,to-tasks
        conf (:conf worker)
        storm-cluster-state (:storm-cluster-state worker)
        storm-id (:storm-id worker)]
    ; 首先, 如果没有指定callback, 以(schedule (:refresh-connections-timer worker) 0 this)为callback
    ; 接着, (.assignment-info storm-cluster-state storm-id callback) 在获取assignment信息的时候, 设置callback,
    ; 也就是说当assignment发生变化时, 就会向refresh-connections-timer中发送一个'立即执行this’的event
    ; 这样就可以保证, 每次assignment发生变化, timer都会在后台做refresh-connections的操作
    (fn this
    ; 因为这个函数的返回值也是一个函数(fn this ...). 所以可以这样使用:
    ; somefun = (mk-refresh-connections worker)
    ; (somefun)           会调用第一种模式即([]), 会间接再调用第二种模式,并把当前函数this作为callback参数
    ; (somefun callback)  会调用第二种模式即([callback])
      ([]
        ; this指的是当前函数mk-refresh-connections
        ; (this (fn...)) 会将(fn...)里的函数作为callback, 调用下面第二种模式([callback])
        (this (fn [& ignored] (schedule (:refresh-connections-timer worker) 0 this))))    ;;schedule往timer里面加event
      ([callback]
        (let [assignment (.assignment-info storm-cluster-state storm-id callback)         ;;当前topology的任务分配信息
              my-assignment (-> assignment                                                ;;b.得到to-tasks的node+port
                                :executor->node+port
                                to-task->node+port                ; 选择的范围topology级别的{task-id [node+port]}. 选择之后的结构仍然不变
                                (select-keys outbound-tasks)      ; 选择的依据是outbound-tasks,即当前worker要往其他task发送数据的这些其他task-ids.
                                (#(map-val endpoint->string %)))  ; {k afn[v]}, endpoint->string([node+port]), 返回值:{task-id port/node}
              ;; we dont need a connection for the local tasks anymore
              needed-assignment (->> my-assignment
                                      (filter-key (complement (-> worker :task-ids set))))
              needed-connections (-> needed-assignment vals set)
              needed-tasks (-> needed-assignment keys)
              
              current-connections (set (keys @(:cached-node+port->socket worker)))
              new-connections (set/difference needed-connections current-connections)
              remove-connections (set/difference current-connections needed-connections)]
              (swap! (:cached-node+port->socket worker)
                     #(HashMap. (merge (into {} %1) %2))
                     (into {}
                       (dofor [endpoint-str new-connections
                               :let [[node port] (string->endpoint endpoint-str)]]
                         [endpoint-str
                          (.connect
                           ^IContext (:mq-context worker)
                           storm-id
                           ((:node->host assignment) node)
                           port)
                          ]
                         )))
              (write-locked (:endpoint-socket-lock worker)
                (reset! (:cached-task->node+port worker)
                        (HashMap. my-assignment)))
              (doseq [endpoint remove-connections]
                (.close (get @(:cached-node+port->socket worker) endpoint)))
              (apply swap!
                     (:cached-node+port->socket worker)
                     #(HashMap. (apply dissoc (into {} %1) %&))
                     remove-connections)
              
              (let [missing-tasks (->> needed-tasks
                                       (filter (complement my-assignment)))]
                (when-not (empty? missing-tasks)
                  (log-warn "Missing assignment for following tasks: " (pr-str missing-tasks))
                  )))))))

(defn refresh-storm-active
  ([worker]
    (refresh-storm-active worker (fn [& ignored] (schedule (:refresh-active-timer worker) 0 (partial refresh-storm-active worker)))))
  ([worker callback]
    (let [base (.storm-base (:storm-cluster-state worker) (:storm-id worker) callback)]
     (reset!
      (:storm-active-atom worker)
      (= :active (-> base :status :type))
      ))
     ))

;; ++++++++++++++++ III. refresh END ----------------------

;; TODO: consider having a max batch size besides what disruptor does automagically to prevent latency issues
(defn mk-transfer-tuples-handler [worker]
  (let [^DisruptorQueue transfer-queue (:transfer-queue worker)
        drainer (ArrayList.)
        node+port->socket (:cached-node+port->socket worker)
        task->node+port (:cached-task->node+port worker)
        endpoint-socket-lock (:endpoint-socket-lock worker)
        ]
    (disruptor/clojure-handler
      (fn [packets _ batch-end?]
        (.addAll drainer packets)
        (when batch-end?
          (read-locked endpoint-socket-lock
            (let [node+port->socket @node+port->socket
                  task->node+port @task->node+port]
              ;; consider doing some automatic batching here (would need to not be serialized at this point to remove per-tuple overhead)
              ;; try using multipart messages ... first sort the tuples by the target node (without changing the local ordering)
            
              (fast-list-iter [[task ser-tuple] drainer]
                ;; TODO: consider write a batch of tuples here to every target worker  
                ;; group by node+port, do multipart send              
                (let [node-port (get task->node+port task)]
                  (when node-port
                    (.send ^IConnection (get node+port->socket node-port) task ser-tuple))
                    ))))
          (.clear drainer))))))

(defn launch-receive-thread [worker]
  (log-message "Launching receive-thread for " (:assignment-id worker) ":" (:port worker))
  (msg-loader/launch-receive-thread!
    (:mq-context worker)
    (:storm-id worker)
    (:port worker)
    (:transfer-local-fn worker)
    (-> worker :storm-conf (get TOPOLOGY-RECEIVER-BUFFER-SIZE))
    :kill-fn (fn [t] (halt-process! 11))))

(defn- close-resources [worker]
  (let [dr (:default-shared-resources worker)]
    (log-message "Shutting down default resources")
    (.shutdownNow (get dr WorkerTopologyContext/SHARED_EXECUTOR))
    (log-message "Shut down default resources")))

;; TODO: should worker even take the storm-id as input? this should be
;; deducable from cluster state (by searching through assignments)
;; what about if there's inconsistency in assignments? -> but nimbus
;; should guarantee this consistency
;; TODO: consider doing worker heartbeating rather than task heartbeating to reduce the load on zookeeper
(defserverfn mk-worker [conf shared-mq-context storm-id assignment-id port worker-id]
  (log-message "Launching worker for " storm-id " on " assignment-id ":" port " with id " worker-id
               " and conf " conf)
  (if-not (local-mode? conf)
    (redirect-stdio-to-slf4j!))
  ;; because in local mode, its not a separate process. supervisor will register it in this case
  (when (= :distributed (cluster-mode conf))
    ; 创建${storm.local.dir}/workers/#worker-id/pids/#pid, 其中pid是当前worker的jvm进程id
    (touch (worker-pid-path conf worker-id (process-pid))))
  (let [worker (worker-data conf shared-mq-context storm-id assignment-id port worker-id)   ;;1.1 生成work-data
        ;;1.2 生成worker的hb
        ;; 1.2.1. 建立worker本地的hb
        heartbeat-fn #(do-heartbeat worker)
        ;; do this here so that the worker process dies if this fails
        ;; it's important that worker heartbeat to supervisor ASAP when launching so that the supervisor knows it's running (and can move on)
        _ (heartbeat-fn)
        
        ;; heartbeat immediately to nimbus so that it knows that the worker has been started
        ;; 1.2.2. 将worker hb同步到zk, 以便nimbus可以立刻知道worker已经启动
        ;; 第一次心跳,没有传递executors,因为这时worker还没有创建executors. 在接下来的定时调度中传递executors.
        _ (do-executor-heartbeats worker)

        ;; 这里的executors是executor对象, 而worker-data里的:executors是运行在当前worker的executor-id
        ;; 创建executors时根据executor-id创建对应的executor对象.
        executors (atom nil)
        ;; launch heartbeat threads immediately so that slow-loading tasks don't cause the worker to timeout to the supervisor
        ;; 1.2.3. 设定timer定期更新本地hb(LocalState)和zk hb
        _ (schedule-recurring (:heartbeat-timer worker) 0 (conf WORKER-HEARTBEAT-FREQUENCY-SECS) heartbeat-fn)
        _ (schedule-recurring (:executor-heartbeat-timer worker) 0 (conf TASK-HEARTBEAT-FREQUENCY-SECS) #(do-executor-heartbeats worker :executors @executors))

        ;;1.3 维护和更新worker的发送connection
        refresh-connections (mk-refresh-connections worker)

        ; 因为refresh-connections是一个函数(mk-refresh-connections的返回值是函数). 实际上等价于((mk-refresh-connections worker) nil). 其中callback=nil
        ; 当然也可以不传递参数:(refresh-connections),这样会调用mk-refresh-connections第一种无参数的模式, 等价于((mk-refresh-connections worker))
        _ (refresh-connections nil)
        _ (refresh-storm-active worker nil)

        ;;1.4 创建executors
        _ (reset! executors (dofor [e (:executors worker)] (executor/mk-executor worker e)))

        ;;1.5 launch接收线程,将数据从server的侦听端口, 不停的放到task对应的接收队列
        receive-thread-shutdown (launch-receive-thread worker)    ;返回值是thread的close function

        ;;1.6 定义event handler来处理transfer queue里面的数据, 并创建transfer-thread
        transfer-tuples (mk-transfer-tuples-handler worker)
        
        transfer-thread (disruptor/consume-loop* (:transfer-queue worker) transfer-tuples)

        ;;1.7 定义worker shutdown函数, 以及worker的操作接口实现
        shutdown* (fn []
                    (log-message "Shutting down worker " storm-id " " assignment-id " " port)
                    (doseq [[_ socket] @(:cached-node+port->socket worker)]
                      ;; this will do best effort flushing since the linger period
                      ;; was set on creation
                      (.close socket))
                    (log-message "Shutting down receive thread")
                    (receive-thread-shutdown)
                    (log-message "Shut down receive thread")
                    (log-message "Terminating messaging context")
                    (log-message "Shutting down executors")
                    (doseq [executor @executors] (.shutdown executor))
                    (log-message "Shut down executors")
                                        
                    ;;this is fine because the only time this is shared is when it's a local context,
                    ;;in which case it's a noop
                    (.term ^IContext (:mq-context worker))
                    (log-message "Shutting down transfer thread")
                    (disruptor/halt-with-interrupt! (:transfer-queue worker))

                    (.interrupt transfer-thread)
                    (.join transfer-thread)
                    (log-message "Shut down transfer thread")
                    (cancel-timer (:heartbeat-timer worker))
                    (cancel-timer (:refresh-connections-timer worker))
                    (cancel-timer (:refresh-active-timer worker))
                    (cancel-timer (:executor-heartbeat-timer worker))
                    (cancel-timer (:user-timer worker))
                    
                    (close-resources worker)
                    
                    ;; TODO: here need to invoke the "shutdown" method of WorkerHook
                    
                    (.remove-worker-heartbeat! (:storm-cluster-state worker) storm-id assignment-id port)
                    (log-message "Disconnecting from storm cluster state context")
                    (.disconnect (:storm-cluster-state worker))
                    (.close (:cluster-state worker))
                    (log-message "Shut down worker " storm-id " " assignment-id " " port))
        ret (reify
             Shutdownable
             (shutdown          ; worker的shutdown时机在supervisor中已经提到过了
              [this]
              (shutdown*))
             DaemonCommon
             (waiting? [this]
               (and
                 (timer-waiting? (:heartbeat-timer worker))
                 (timer-waiting? (:refresh-connections-timer worker))
                 (timer-waiting? (:refresh-active-timer worker))
                 (timer-waiting? (:executor-heartbeat-timer worker))
                 (timer-waiting? (:user-timer worker))
                 ))
             )]

    ; refresh-connections是一个函数. 因为它等于(mk-refresh-connections worker),而它的返回值是一个函数.  (refresh-connections)会调用第一种模式
    (schedule-recurring (:refresh-connections-timer worker) 0 (conf TASK-REFRESH-POLL-SECS) refresh-connections)
    ; 为什么要加partial偏函数? 如果是(refresh-storm-active worker),则它的返回值不是一个函数. 加上partial后就变成了函数. 因为schedule-recurring要接受一个函数!
    (schedule-recurring (:refresh-active-timer worker) 0 (conf TASK-REFRESH-POLL-SECS) (partial refresh-storm-active worker))

    (log-message "Worker has topology config " (:storm-conf worker))
    (log-message "Worker " worker-id " for storm " storm-id " on " assignment-id ":" port " has finished loading")
    ret
    ))

(defmethod mk-suicide-fn
  :local [conf]
  (fn [] (halt-process! 1 "Worker died")))

(defmethod mk-suicide-fn
  :distributed [conf]
  (fn [] (halt-process! 1 "Worker died")))

; assignment-id: supervisor-id
; 参数确定了zk节点: /storm/workerbeats/#worker-id/assignment-id-port
(defn -main [storm-id assignment-id port-str worker-id]  
  (let [conf (read-storm-config)]
    (validate-distributed-mode! conf)
    (mk-worker conf nil (java.net.URLDecoder/decode storm-id) assignment-id (Integer/parseInt port-str) worker-id)))
