package com.au.example.redisclient.controller;

import com.au.example.redisclient.model.User;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.core.ReactiveRedisOperations;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@RestController
public class UserController {


    @NonNull
    private final ReactiveRedisOperations<String, User> userOps;

    @NonNull
    private ReactiveRedisOperations<String, User> redisOperations;

    @NonNull
    private ReactiveRedisMessageListenerContainer reactiveMsgListenerContainer;

    @NonNull
    private ChannelTopic topic;



    @GetMapping("/user")
    public Flux<User> all() {
        log.info("Receiving all users from Redis.");
        Flux<User> userFlux = userOps.keys("*")
                .flatMap(userOps.opsForValue()::get);
        log.info("userFlux was created");
        return userFlux;

    }

    @PostMapping("/user/{username}")
    public Mono<Boolean> addUser(@PathVariable String username) {
        log.info("New User with username '" + username + "' added to Redis.");
        User user = new User(UUID.randomUUID().toString(), username);
        return redisOperations.opsForValue().set(user.getId(), user);
    }

    @PostMapping("/message/user/{username}")
    public Mono<Long> sendUserMessage(@PathVariable String username) {
        log.info("New User with username '" + username + "' send to Channel '" + topic.getTopic() + "'.");
        User user = new User(UUID.randomUUID().toString(), username);
        return redisOperations.convertAndSend(topic.getTopic(), user);
    }

    @GetMapping("/message/users")
    public Flux<String> receiveUserMessages() {
        log.info("Starting to receive User Messages from Channel '" + topic.getTopic() + "'.");
        return reactiveMsgListenerContainer
                .receive(topic)
                .map(ReactiveSubscription.Message::getMessage)
                .map(msg -> {
                    log.info("New Message received: '" + msg + "'.");
                    return msg + "\n";
                });
    }
}
