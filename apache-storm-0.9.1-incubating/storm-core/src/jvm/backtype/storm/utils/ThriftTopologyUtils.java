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
package backtype.storm.utils;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ThriftTopologyUtils {

    // StormTopology定义在storm.thrift中. 包括了三个Map<String, Component>. 其中Component有SpoutSpec,BlotSpec,StateSpoutSpec
    // Map的key:String表示的就是component-id. Spec表示组件的声明,定义了组件的字段信息. 这些组件声明都包含ComponentObject和ComponentCommon字段
    public static Set<String> getComponentIds(StormTopology topology) {
        Set<String> ret = new HashSet<String>();
        // f就是StormTopology中声明的每个字段
        for(StormTopology._Fields f: StormTopology.metaDataMap.keySet()) {
            Map<String, Object> componentMap = (Map<String, Object>) topology.getFieldValue(f);
            ret.addAll(componentMap.keySet());
        }
        return ret;
    }

    // 以StormTopology中定义的 map<string, SpoutSpec> spouts 为例
    // SpoutSpec: ComponentObject spout_object;
    //            ComponentCommon common;
    public static ComponentCommon getComponentCommon(StormTopology topology, String componentId) {
        for(StormTopology._Fields f: StormTopology.metaDataMap.keySet()) {
            // componentMap指的就是StormTopology中定义的字段 spouts 包含的数据
            Map<String, Object> componentMap = (Map<String, Object>) topology.getFieldValue(f);
            if(componentMap.containsKey(componentId)) {
                Object component = componentMap.get(componentId);
                if(component instanceof Bolt) {
                    return ((Bolt) component).get_common();
                }
                if(component instanceof SpoutSpec) {
                    return ((SpoutSpec) component).get_common();
                }
                if(component instanceof StateSpoutSpec) {
                    return ((StateSpoutSpec) component).get_common();
                }
                throw new RuntimeException("Unreachable code! No get_common conversion for component " + component);
            }
        }
        throw new IllegalArgumentException("Could not find component common for " + componentId);
    }
}
