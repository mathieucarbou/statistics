/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.statistics;

import org.terracotta.statistics.extended.StatisticType;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Optional.ofNullable;
import static org.terracotta.statistics.ConstantValueStatistic.instance;

/**
 * @author Mathieu Carbou
 */
public class Table implements Serializable {

  private static final long serialVersionUID = 1L;

  public static Builder newBuilder() {
    return new Builder();
  }

  private final Map<String, Map<String, ConstantValueStatistic<? extends Serializable>>> statistics = new HashMap<>();

  Table() {
  }

  public int getRowCount() {
    return statistics.size();
  }

  public Collection<String> getRowLabels() {
    return statistics.keySet();
  }

  public Collection<String> getStatisticNames(String row) {
    Map<String, ConstantValueStatistic<? extends Serializable>> map = statistics.get(row);
    return map == null ? Collections.emptyList() : map.keySet();
  }

  public Map<String, ? extends ValueStatistic<? extends Serializable>> getStatistics(String row) {
    Map<String, ConstantValueStatistic<? extends Serializable>> map = statistics.get(row);
    return map == null ? Collections.emptyMap() : map;
  }

  public Map<String, Map<String, ConstantValueStatistic<? extends Serializable>>> getStatistics() {
    return statistics;
  }

  @SuppressWarnings("unchecked")
  public <T extends Serializable> Optional<ValueStatistic<T>> getStatistic(String row, String statisticName) {
    return ofNullable(statistics.get(row)).flatMap(stats -> ofNullable((ValueStatistic<T>) stats.get(statisticName)));
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Table{");
    sb.append("statistics=").append(statistics);
    sb.append(", rowCount=").append(getRowCount());
    sb.append(", rowLabels=").append(getRowLabels());
    sb.append('}');
    return sb.toString();
  }

  public static class Builder {

    private final Table table = new Table();

    private Builder() {
    }

    public <T extends Serializable> Table.Builder setStatistic(String rowName, String statisticName, StatisticType type, T value) {
      table.statistics.computeIfAbsent(rowName, s -> new HashMap<>()).put(statisticName, instance(type, value));
      return this;
    }

    public Table.Builder withRow(String rowName, Consumer<Table.RowBuilder> c) {
      c.accept(new Table.RowBuilder() {
        @Override
        public <T extends Serializable> Table.RowBuilder setStatistic(String statisticName, StatisticType type, T value) {
          table.statistics.computeIfAbsent(rowName, s -> new HashMap<>()).put(statisticName, instance(type, value));
          return this;
        }
      });
      return this;
    }

    public Table.Builder withRows(Collection<String> rowNames, BiConsumer<String, Table.RowBuilder> c) {
      rowNames.forEach(rowName -> c.accept(rowName, new Table.RowBuilder() {
        @Override
        public <T extends Serializable> Table.RowBuilder setStatistic(String statisticName, StatisticType type, T value) {
          table.statistics.computeIfAbsent(rowName, s -> new HashMap<>()).put(statisticName, instance(type, value));
          return this;
        }
      }));
      return this;
    }

    public Table build() {
      return table;
    }
  }

  public interface RowBuilder {
    <T extends Serializable> RowBuilder setStatistic(String statisticName, StatisticType type, T value);
  }

}
