/*
 * All content copyright Terracotta, Inc., unless otherwise indicated.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.statistics.derived.latency;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.statistics.LongSample;
import org.terracotta.statistics.Time;
import org.terracotta.statistics.util.ConcurrentParameterized;
import org.terracotta.statistics.util.LatencyUtils;
import org.terracotta.statistics.util.Percentile;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Math.round;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofMinutes;
import static java.util.function.DoubleUnaryOperator.identity;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.terracotta.statistics.util.LatencyUtils.getPercentiles;

/**
 * @author Mathieu Carbou
 */
@RunWith(ConcurrentParameterized.class)
public class DefaultLatencyHistogramStatisticTest {

  private static final double DEFAULT_EXP_HISTOGRAM_EPSILON = 0.01;

  private static final Duration OPERATIONS_DURATION = ofMinutes(90);
  private static final Duration MEAN_LATENCY = ofMillis(100);
  private static final int OPS_PER_SEC = 15;
  private static List<LongSample> OPERATIONS;
  private static Map<String, Percentile> PERCENTILES;
  private static long SEED;

  @Parameterized.Parameters(name = "{index}: window={0}, buckets={1}, phi={2}")
  public static Iterable<Object[]> data() {

    // SETTINGS FOR: histograms
    double[] PHIS = {0.3, 0.63, 0.7, 0.8, 2.0}; // 0.7 == BarSplittingBiasedHistogram.DEFAULT_PHI
    int[] BUCKETS = {10, 20, 50};
    Duration[] WINDOWS = {ofMinutes(1), ofMinutes(5), ofMinutes(10), ofMinutes(30), ofMinutes(60), ofMinutes(120)};

    // Generates Junit's parameters
    return DoubleStream.of(PHIS).boxed().flatMap(phi ->
        IntStream.of(BUCKETS).boxed().flatMap(buckets ->
            Stream.of(WINDOWS).map(window ->
                new Object[]{window, buckets, phi}
            )))
        .collect(Collectors.toList());
  }

  @BeforeClass
  public static void simulateOperations() {
    SEED = System.nanoTime();

    //OPERATIONS = LatencyUtils.generateLatencies(new Random(seed), 15, OPERATIONS_DURATION);
    OPERATIONS = LatencyUtils.generateLatencies(new Random(SEED), OPS_PER_SEC, OPERATIONS_DURATION, MEAN_LATENCY.toNanos());

    String msg = "seed: " + SEED + "\nops: " + OPERATIONS.size();
    assertThat(msg, (double) OPERATIONS.size(), is(closeTo(OPERATIONS_DURATION.getSeconds() * OPS_PER_SEC, OPERATIONS_DURATION.getSeconds() * OPS_PER_SEC * .20)));
    OPERATIONS.forEach(sample -> {
      assertThat(msg, sample.time(), greaterThanOrEqualTo(0L));
      assertThat(msg, sample.value(), greaterThan(0L));
    });

    PERCENTILES = getPercentiles(OPERATIONS, identity());
  }

  private final DefaultLatencyHistogramStatistic statistic;
  private final long window;
  private final double phi;
  private long now = Time.time();
  private Map<String, Percentile> lastPercentiles;

  public DefaultLatencyHistogramStatisticTest(Duration window, int bucketCount, double phi) {
    statistic = new DefaultLatencyHistogramStatistic(phi, bucketCount, window, () -> now);
    this.window = window.toNanos();
    this.phi = phi;
  }

  @Before
  public void init() {
    // will do some assertions during the insertion each 50% of the window
    fillDerivedStatistic(window / 2, insertedSamples -> {

      LongSample lastInsertedSample = insertedSamples.get(insertedSamples.size() - 1);

      Map<String, Percentile> percentiles = getPercentiles(statistic, identity());
      long max = round(percentiles.get("max").value());

      long end = lastInsertedSample.time(); // inclusive
      long start = end - window; // exclusive

      String reason =
          "Seed: " + SEED +
          "\nOperations: " + OPERATIONS.size() +
          "\nStart: " + start +
          "\nEnd: " + end +
          "\nMax: " + max +
          "\nHistogram: " + statistic +
          "\nContent: " + statistic.buckets();

      long count = insertedSamples.stream()
          .filter(sample -> sample.time() > start && sample.time() <= end)
          .peek(sample -> {
            if (phi <= 0.8) {
              // with a high Phi, high percentile values are not accurate at all
              assertThat(reason + "\nSample: " + sample, sample.value(), is(lessThanOrEqualTo(max)));
            }
          })
          .count();

      assertThat(reason, count, is(greaterThanOrEqualTo(1l)));
    });

    LongSample lastInsertedSample = OPERATIONS.get(OPERATIONS.size() - 1);
    long end = lastInsertedSample.time(); // inclusive
    long start = end - window; // exclusive
    List<LongSample> lastOperations = OPERATIONS.stream()
        .filter(sample -> sample.time() > start && sample.time() <= end)
        .collect(Collectors.toList());
    lastPercentiles = getPercentiles(lastOperations, identity());
  }

  @Test
  public void minimum() {
    if (window >= OPERATIONS_DURATION.toNanos()) {
      assertThat(statistic.minimum(), equalTo((long) PERCENTILES.get("min").value()));
    }
    assertThat(statistic.minimum(), lessThanOrEqualTo((long) lastPercentiles.get("min").value()));
  }

  @Test
  public void maximum() {
    if (window >= OPERATIONS_DURATION.toNanos()) {
      assertThat(statistic.maximum(), equalTo((long) PERCENTILES.get("max").value()));
    }
    assertThat(statistic.maximum(), greaterThanOrEqualTo((long) lastPercentiles.get("max").value()));
  }

  @Test
  public void count() {
    if (window >= OPERATIONS_DURATION.toNanos()) {
      assertThat((double) statistic.count(), greaterThanOrEqualTo(OPERATIONS.size() * (1 - DEFAULT_EXP_HISTOGRAM_EPSILON)));
    }
    assertThat((double) statistic.count(), greaterThanOrEqualTo(lastPercentiles.size() * (1 - DEFAULT_EXP_HISTOGRAM_EPSILON)));
  }

  private void fillDerivedStatistic(long assertionsInterval, Consumer<List<LongSample>> assertions) {
    long start = now = Time.time();
    long nextAssertion = now + assertionsInterval;
    List<LongSample> inserted = new ArrayList<>();
    for (LongSample sample : OPERATIONS) {
      now = start + sample.time();
      statistic.event(now, sample.value());
      inserted.add(sample);
      // once the sample is inserted in the histogram, we are colling the assertions if we need
      if (now >= nextAssertion) {
        nextAssertion = now + assertionsInterval;
        assertions.accept(inserted);
      }
    }
  }

}