package com.example;

import jmh.mbr.junit4.Microbenchmark;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.runner.RunWith;
import org.openjdk.jmh.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.Banner;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 Benchmark                                       Mode  Cnt          Score           Error   Units
 LettuceNonTransactionBenchmarksTests.normal    thrpt    5         ≈ 10⁻⁵                  ops/ns
 LettuceNonTransactionBenchmarksTests.pipeline  thrpt    5         ≈ 10⁻⁷                  ops/ns
 LettuceNonTransactionBenchmarksTests.normal     avgt    5   11345842.596 ±  11492862.757   ns/op
 LettuceNonTransactionBenchmarksTests.pipeline   avgt    5  698277118.368 ± 414495664.708   ns/op
 */
@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 0, time = 1)
@BenchmarkMode({Mode.AverageTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(100)
@Fork(1)
@RunWith(Microbenchmark.class)
public class LettuceNonTransactionBenchmarksTests {

    private static final Logger logger = LoggerFactory.getLogger(LettuceNonTransactionBenchmarksTests.class);

    @Benchmark
    public void normal(BenchmarkContext context) {
        final String key = String.valueOf(context.count.incrementAndGet());
        final String value = RandomStringUtils.randomAlphabetic(20);
//        logger.info("normal. {}:{}", key, value);
        context.redisTemplate.opsForValue().set(key, value);
        context.redisTemplate.delete(key);
    }

    @Benchmark
    public void pipeline(BenchmarkContext context) {
        final String key = String.valueOf(context.count.incrementAndGet());
        final String value = RandomStringUtils.randomAlphabetic(20);
//        logger.info("pipeline. {}:{}", key, value);
        context.redisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations redisOperations) throws DataAccessException {
                redisOperations.opsForValue().set(key, value);
                redisOperations.delete(key);
                return null;
            }
        });
    }

    @State(Scope.Benchmark)
    public static class BenchmarkContext {

        volatile ConfigurableApplicationContext context;

        volatile RedisTemplate<String, String> redisTemplate;

        volatile AtomicInteger count;

        @Setup
        public void setup() {
            this.context = new SpringApplicationBuilder(Application.class)
                    .bannerMode(Banner.Mode.OFF)
                    .profiles("lettuce")
                    .build()
                    .run();
            this.redisTemplate = context.getBean("redisTemplateNonTransaction", RedisTemplate.class);
            this.count = new AtomicInteger(1);

            warmup();
        }

        private void warmup() {
            for (int i = 0; i < 5; i++) {
                redisTemplate.getConnectionFactory().getConnection().ping();
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    // ignored
                }
            }
        }

        @TearDown
        public void clean() {
            this.context.close();
            this.count.set(1);
        }
    }
}
