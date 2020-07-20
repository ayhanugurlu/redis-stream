package com.au.example.redisclient.util;

import com.au.example.redisclient.model.User;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import javax.annotation.PostConstruct;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class UserPreLoader {

    @NonNull
    private final ReactiveRedisConnectionFactory factory;

    @NonNull
    private final ReactiveRedisOperations<String, User> userOps;

    @PostConstruct
    public void loadData() {
        // Just fill our Redis with some predefined data
        factory.getReactiveConnection().serverCommands().flushAll().thenMany(
                Flux.just("a1", "a2", "a3")
                        .map(name -> new User(UUID.randomUUID().toString(), name))
                        .flatMap(user -> userOps.opsForValue().set(user.getId(), user)))
                .thenMany(userOps.keys("*")
                        .flatMap(userOps.opsForValue()::get))
                .subscribe(System.out::println);
    }

}
