/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package projectx2018;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.RandomPicker;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class Main {
    static final int HUGE_PRIME = 2038079003;

    private static ILogger logger;
    static final String MAP = "projectX";

    public static void main(String[] args) {
        Config cfg = new ClasspathXmlConfig("hazelcast.xml");
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);

        logger = Logger.getLogger(Main.class);

        InfluxDB influxDB = InfluxDBFactory.connect("http://127.0.0.1:8086", "projectX", "h4z3lc4st");
        influxDB.setDatabase("projectX");
        influxDB.setRetentionPolicy("autogen");

        influxDB.enableBatch(BatchOptions.DEFAULTS);

        int loadRecords = 10000;
        IMap<Integer, Integer> map = populate(loadRecords, hz);

        int cnt = 0;
        while (true) {
            long start = System.nanoTime();
            int randomKey = RandomPicker.getInt(0, loadRecords);
            Integer value = map.get(randomKey);
            long end = System.nanoTime();
            influxDB.write(Point.measurement("map.load")
                                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                                .addField("value", end - start)
                                .build());

            int expected = randomKey * HUGE_PRIME;
            if (value != null && value != expected) {
                logger.severe("OUCH: " + value + "!=" + expected);
                System.exit(1);
            }

            {
                if (++cnt % 1000 == 0) {
                    LockSupport.parkNanos(100000);
                }
            }
        }

        //        hz.shutdown();
    }

    private static IMap<Integer, Integer> populate(int loadRecords, HazelcastInstance hz) {
        logger.info("Loading " + loadRecords + " records into the map " + MAP);

        IMap<Integer, Integer> map = hz.getMap(MAP);
        for (int key = 0; key < loadRecords; key++) {
            map.put(key, key * HUGE_PRIME);
        }

        logger.info("Map has " + map.size() + " entries");
        return map;
    }

}
