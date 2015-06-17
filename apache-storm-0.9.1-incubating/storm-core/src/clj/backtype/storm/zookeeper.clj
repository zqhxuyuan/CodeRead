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
(ns backtype.storm.zookeeper
  (:import [com.netflix.curator.retry RetryNTimes])
  (:import [com.netflix.curator.framework.api CuratorEvent CuratorEventType CuratorListener UnhandledErrorListener])
  (:import [com.netflix.curator.framework CuratorFramework CuratorFrameworkFactory])
  (:import [org.apache.zookeeper ZooKeeper Watcher KeeperException$NoNodeException
            ZooDefs ZooDefs$Ids CreateMode WatchedEvent Watcher$Event Watcher$Event$KeeperState
            Watcher$Event$EventType KeeperException$NodeExistsException])
  (:import [org.apache.zookeeper.data Stat])
  (:import [org.apache.zookeeper.server ZooKeeperServer NIOServerCnxn$Factory])
  (:import [java.net InetSocketAddress BindException])
  (:import [java.io File])
  (:import [backtype.storm.utils Utils ZookeeperAuthInfo])
  (:use [backtype.storm util log config]))

(def zk-keeper-states
  {Watcher$Event$KeeperState/Disconnected :disconnected
   Watcher$Event$KeeperState/SyncConnected :connected
   Watcher$Event$KeeperState/AuthFailed :auth-failed
   Watcher$Event$KeeperState/Expired :expired
  })

(def zk-event-types
  {Watcher$Event$EventType/None :none
   Watcher$Event$EventType/NodeCreated :node-created
   Watcher$Event$EventType/NodeDeleted :node-deleted
   Watcher$Event$EventType/NodeDataChanged :node-data-changed
   Watcher$Event$EventType/NodeChildrenChanged :node-children-changed
  })

(defn- default-watcher [state type path]
  (log-message "Zookeeper state update: " state type path))

(defnk mk-client [conf servers port :root "" :watcher default-watcher :auth-conf nil]
  (let [fk (Utils/newCurator conf servers port root (when auth-conf (ZookeeperAuthInfo. auth-conf)))]
    (.. fk							; 两个.表示连续调用. fk = CuratorFramework
      (getCuratorListenable)				; fk.getCuratorListenable().addListener(new CuratorListener(){...})
      (addListener						; 监听器作为匿名类
        (reify CuratorListener				; 通过监听器的方式指定Watcher
          (^void eventReceived [this ^CuratorFramework _fk ^CuratorEvent e]
            (when (= (.getType e) CuratorEventType/WATCHED)	; 事件类型为WATCHED
              (let [^WatchedEvent event (.getWatchedEvent e)]
                ;; watcher即方法中的:watcher. 默认为default-watcher函数, 接受三个参数
                (watcher (zk-keeper-states (.getState event))	; zk-keeper-states是个map, event.getState()得到的是确定的key. (map key)
                  (zk-event-types (.getType event))		; zk-event-types也是一个map
                  (.getPath event))))))))			; watcher(Watcher.Event.KeeperState, Watcher.Event.EventType, path)
;;    (.. fk
;;        (getUnhandledErrorListenable)
;;        (addListener
;;         (reify UnhandledErrorListener
;;           (unhandledError [this msg error]
;;             (if (or (exception-cause? InterruptedException error)
;;                     (exception-cause? java.nio.channels.ClosedByInterruptException error))
;;               (do (log-warn-error error "Zookeeper exception " msg)
;;                   (let [to-throw (InterruptedException.)]
;;                     (.initCause to-throw error)
;;                     (throw to-throw)
;;                     ))
;;               (do (log-error error "Unrecoverable Zookeeper error " msg)
;;                   (halt-process! 1 "Unrecoverable Zookeeper error")))
;;             ))))
    (.start fk)						; CuratorFramework.start()
    fk))							; 返回值为fk. 因为操作zk的接口使用的就是这个对象

(def zk-create-modes
  {:ephemeral CreateMode/EPHEMERAL
   :persistent CreateMode/PERSISTENT
   :sequential CreateMode/PERSISTENT_SEQUENTIAL})

;; 创建节点
(defn create-node
  ([^CuratorFramework zk ^String path ^bytes data mode]
    (try
      ; 假设参数mode=:persistent, 则(zk-create-modes mode)为CreateMode/PERSISTENT
      (.. zk (create) (withMode (zk-create-modes mode)) (withACL ZooDefs$Ids/OPEN_ACL_UNSAFE) (forPath (normalize-path path) data))
      (catch Exception e (throw (wrap-in-runtime e)))))
  ([^CuratorFramework zk ^String path ^bytes data]  ; 没有mode的处理方式: 递归
    (create-node zk path data :persistent)))

;; 判断是否存在节点
(defn exists-node? [^CuratorFramework zk ^String path watch?]
  ((complement nil?)
    (try
      (if watch?
         (.. zk (checkExists) (watched) (forPath (normalize-path path))) 
         (.. zk (checkExists) (forPath (normalize-path path))))
      (catch Exception e (throw (wrap-in-runtime e))))))

;; 删除节点
(defnk delete-node [^CuratorFramework zk ^String path :force false]
  (try-cause  (.. zk (delete) (forPath (normalize-path path)))
    (catch KeeperException$NoNodeException e
      (when-not force (throw e)))
    (catch Exception e (throw (wrap-in-runtime e)))))

;; 创建目录
;; 第一个参数zk就是mk-client的返回值CuratorFramework对象
(defn mkdirs [^CuratorFramework zk ^String path]
  (let [path (normalize-path path)]
    ; path不是/ 或者不存在, 才可以新建
    (when-not (or (= path "/") (exists-node? zk path false))
      (mkdirs zk (parent-path path))
      (try-cause
        ; 最后一个参数是个keyword, 是参数mode, 因为mode的值是确定的
        (create-node zk path (barr 7) :persistent)
        (catch KeeperException$NodeExistsException e
          ;; this can happen when multiple clients doing mkdir at same time
          ))
      )))

;; 获取数据
(defn get-data [^CuratorFramework zk ^String path watch?]
  (let [path (normalize-path path)]
    (try-cause
      (if (exists-node? zk path watch?)
        (if watch?
          (.. zk (getData) (watched) (forPath path))
          (.. zk (getData) (forPath path))))
    (catch KeeperException$NoNodeException e
      ;; this is fine b/c we still have a watch from the successful exists call
      nil )
    (catch Exception e (throw (wrap-in-runtime e))))))

;; 获取所有孩子节点
(defn get-children [^CuratorFramework zk ^String path watch?]
  (try
    (if watch?
      (.. zk (getChildren) (watched) (forPath (normalize-path path)))
      (.. zk (getChildren) (forPath (normalize-path path))))
    (catch Exception e (throw (wrap-in-runtime e)))))

;; 设置节点的数据
(defn set-data [^CuratorFramework zk ^String path ^bytes data]
  (try
    ; zk.setData().forPath(path, data)
    (.. zk (setData) (forPath (normalize-path path) data))
    (catch Exception e (throw (wrap-in-runtime e)))))

(defn exists [^CuratorFramework zk ^String path watch?]
  (exists-node? zk path watch?))

;; 递归删除
(defn delete-recursive [^CuratorFramework zk ^String path]
  (let [path (normalize-path path)]
    (when (exists-node? zk path false)
      (let [children (try-cause (get-children zk path false)
                                (catch KeeperException$NoNodeException e
                                  []
                                  ))]
        (doseq [c children]
          (delete-recursive zk (full-path path c)))
        (delete-node zk path :force true)
        ))))

;; 创建一个本地进程的zookeeper
(defnk mk-inprocess-zookeeper [localdir :port nil]
  (let [localfile (File. localdir)    ; $ZK_HOME/conf/zoo.cfg的dataDir的值
        zk (ZooKeeperServer. localfile localfile 2000)    ; 以dataDir创建一个ZK服务器
        [retport factory] (loop [retport (if port port 2000)]
                            ; 通过ZK的API创建一个NIOServerCnxn.Factory
                            (if-let [factory-tmp (try-cause (NIOServerCnxn$Factory. (InetSocketAddress. retport))
                                              (catch BindException e
                                                (when (> (inc retport) (if port port 65535))
                                                  (throw (RuntimeException. "No port is available to launch an inprocess zookeeper.")))))]
                              [retport factory-tmp]
                              (recur (inc retport))))]
    (log-message "Starting inprocess zookeeper at port " retport " and dir " localdir)    
    (.startup factory zk)
    [retport factory]
    ))

(defn shutdown-inprocess-zookeeper [handle]
  (.shutdown handle))
