/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.scheduler;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface INimbus {
    void prepare(Map stormConf, String schedulerLocalDir);
    /**
     * Returns all slots that are available for the next round of scheduling.在下一次调度中可用的槽位
     * A slot is available for scheduling 如果槽位是空闲的且可以被分配的, 或者虽然被使用但可以被重新分配的. 都是可以被调度的
     * if it is free and can be assigned to, or if it is used and can be reassigned.
     */
    Collection<WorkerSlot> allSlotsAvailableForScheduling(Collection<SupervisorDetails> existingSupervisors, Topologies topologies, Set<String> topologiesMissingAssignments);

    // this is called after the assignment is changed in ZK
    void assignSlots(Topologies topologies, Map<String, Collection<WorkerSlot>> newSlotsByTopologyId);
    
    // map from node id to supervisor details
    String getHostName(Map<String, SupervisorDetails> existingSupervisors, String nodeId);
    
    IScheduler getForcedScheduler(); 
}
