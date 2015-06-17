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
(ns backtype.storm.event
  (:use [backtype.storm log util])
  (:import [backtype.storm.utils Time Utils])
  (:import [java.util.concurrent LinkedBlockingQueue TimeUnit])
  )

;; 协议接口
(defprotocol EventManager
  (add [this event-fn]) ; 添加事件函数
  (waiting? [this])
  (shutdown [this]))

(defn event-manager
  "Creates a thread to respond to events. Any error will cause process to halt"
  [daemon?]
  (let [added (atom 0)      ; 已经添加到队列中的数量
        processed (atom 0)  ; 已经处理的数量
        ^LinkedBlockingQueue queue (LinkedBlockingQueue.) ; 阻塞队列
        running (atom true) ; 标志正在运行
        runner (Thread.     ; 运行线程
                  (fn []
                    (try-cause
                      (while @running
                        (let [r (.take queue)]    ; 从队列中取出第一个事件
                          (r)                     ; 执行这个事件
                          (swap! processed inc)))
                    (catch InterruptedException t
                      (log-message "Event manager interrupted"))
                    (catch Throwable t
                      (log-error t "Error when processing event")
                      (halt-process! 20 "Error when processing an event"))
                      )))]
    (.setDaemon runner daemon?)
    (.start runner)         ; 启动线程
    (reify
      EventManager
      (add [this event-fn]
        ;; should keep track of total added and processed to know if this is finished yet
        (when-not @running
          (throw (RuntimeException. "Cannot add events to a shutdown event manager")))
        (swap! added inc)       ; 队列中的事件数量+1
        (.put queue event-fn)   ; 添加到队列中
        )
      (waiting? [this]
        (or (Time/isThreadWaiting runner)
            (= @processed @added)
            ))
      (shutdown [this]
        (reset! running false)
        (.interrupt runner)
        (.join runner)
        )
        )))
