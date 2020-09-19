package com.example;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StopWatch;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 StopWatch 'normal': running time (millis) = 239
 -----------------------------------------
 ms     %     Task name
 -----------------------------------------
 00239  100%

 StopWatch 'pipeline': running time (millis) = 6817
 -----------------------------------------
 ms     %     Task name
 -----------------------------------------
 06817  100%
 */
@RunWith(SpringRunner.class)
@Import(LettuceConfig.class)
@ActiveProfiles("lettuce")
public class LettuceNonTransactionTests {

    @Autowired
    @Qualifier("redisTemplateNonTransaction")
    private RedisTemplate<String, String> redisTemplate;

    private static final int THREADS = 200;
    private static final int END = 1000;

    @Before
    public void sleep() {
        for (int i = 0; i < 5; i++) {
            redisTemplate.getConnectionFactory().getConnection().ping();
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                // ignored
            }
        }
    }

    @Test
    public void normal() {
        process("normal", num -> {
            final String key = String.valueOf(num);
            final String value = RandomStringUtils.randomAlphabetic(100);

            redisTemplate.opsForValue().set(key, value);
            redisTemplate.delete(key);
        });
    }

    @Test
    public void pipeline() {
        process("pipeline", num -> {
            final String key = String.valueOf(num);
            final String value = RandomStringUtils.randomAlphabetic(100);

            redisTemplate.executePipelined(new SessionCallback<Object>() {
                @Override
                public Object execute(RedisOperations redisOperations) throws DataAccessException {
                    redisOperations.opsForValue().set(key, value);
                    redisOperations.delete(key);
                    return null;
                }
            });
        });
    }

    private static void process(String title, Consumer<Integer> consumer) {
        final ExecutorService executorService = Executors.newFixedThreadPool(THREADS);

        CountDownLatch latch = new CountDownLatch(END);
        final StopWatch stopWatch = new StopWatch(title);
        stopWatch.start();
        for (int i = 0; i < END; i++) {
            final int number = i;
            executorService.submit(() -> {
                consumer.accept(number);
                latch.countDown();
            });
        }

        try {
            latch.await(20, TimeUnit.SECONDS);
            stopWatch.stop();
            System.out.println(stopWatch.prettyPrint());
        } catch (Exception e) {
            // ignored
        }
    }
}
