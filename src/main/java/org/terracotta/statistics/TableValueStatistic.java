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
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author Mathieu Carbou
 */
public class TableValueStatistic implements ValueStatistic<Table> {

  public static TableValueStatistic.Builder newBuilder() {
    return new TableValueStatistic.Builder();
  }

  private final Map<String, Map<String, ValueStatistic<? extends Serializable>>> innerStats = new HashMap<>();

  private TableValueStatistic() {
  }

  @Override
  public StatisticType type() {
    return StatisticType.TABLE;
  }

  @Override
  public Table value() {
    return Table.newBuilder()
        .withRows(innerStats.keySet(), (row, rowBuilder) -> innerStats.get(row)
            .forEach((k, v) -> rowBuilder.setStatistic(k, v.type(), v.value())))
        .build();
  }

  public static class Builder {

    private final TableValueStatistic stat = new TableValueStatistic();

    private Builder() {
    }

    public <T extends Serializable> Builder registerStatistic(String rowName, String statisticName, ValueStatistic<T> accessor) {
      stat.innerStats.computeIfAbsent(rowName, s -> new HashMap<>()).put(statisticName, accessor);
      return this;
    }

    public Builder withRow(String rowName, Consumer<RowBuilder> c) {
      c.accept(new RowBuilder() {
        @Override
        public <T extends Serializable> RowBuilder registerStatistic(String statisticName, ValueStatistic<T> accessor) {
          stat.innerStats.computeIfAbsent(rowName, s -> new HashMap<>()).put(statisticName, accessor);
          return this;
        }
      });
      return this;
    }

    public Builder withRows(Collection<String> rowNames, BiConsumer<String, RowBuilder> c) {
      rowNames.forEach(rowName -> c.accept(rowName, new RowBuilder() {
        @Override
        public <T extends Serializable> RowBuilder registerStatistic(String statisticName, ValueStatistic<T> accessor) {
          stat.innerStats.computeIfAbsent(rowName, s -> new HashMap<>()).put(statisticName, accessor);
          return this;
        }
      }));
      return this;
    }

    public ValueStatistic<Table> build() {
      return stat;
    }
  }

  public interface RowBuilder {
    <T extends Serializable> RowBuilder registerStatistic(String statisticName, ValueStatistic<T> accessor);
  }

}
