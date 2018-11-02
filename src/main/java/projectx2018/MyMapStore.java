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

import com.hazelcast.core.MapStore;
import com.hazelcast.util.RandomPicker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

import static java.lang.String.format;

public class MyMapStore implements MapStore<Integer, Integer> {

    private final Connection con;

    public MyMapStore() {
        try {
            con = DriverManager.getConnection("jdbc:postgresql://localhost/projectx", "projectx", "h4z3lc4st");
            con.createStatement().executeUpdate(
                    "create table if not exists data (key integer not null, value integer not null, primary key (key))");
            con.createStatement().executeUpdate("delete from data");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void delete(Integer key) {
        System.out.println("Delete:" + key);
        try {
            con.createStatement().executeUpdate(
                    format("delete from data where id = %s", key));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void store(Integer key, Integer value) {
        try {
            con.createStatement().executeUpdate(
                    format("insert into data values(%s,'%s')", key, value));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void storeAll(Map<Integer, Integer> map) {
        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            store(entry.getKey(), entry.getValue());
        }
    }

    public synchronized void deleteAll(Collection<Integer> keys) {
        for (Integer key : keys) {
            delete(key);
        }
    }

    public synchronized Integer load(Integer key) {
        try {
            ResultSet resultSet = con.createStatement().executeQuery(
                    format("select value from data where key=%s", key));
            try {
                if (!resultSet.next()) {
                    return null;
                }
                int randomSleep = RandomPicker.getInt(3000, 5000);
                LockSupport.parkNanos(randomSleep * 1000);
                return resultSet.getInt(1);
            } finally {
                resultSet.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized Map<Integer, Integer> loadAll(Collection<Integer> keys) {
        Map<Integer, Integer> result = new HashMap<>();
        for (Integer key : keys) {
            result.put(key, load(key));
        }
        return result;
    }

    public Iterable<Integer> loadAllKeys() {
        return Collections.emptySet();
    }
}
