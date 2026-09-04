package com.atlasflow.worker;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Maps a {@code TaskDefinition.handlerName()} string to the {@link TaskHandler}
 * that executes it. Kept separate from {@link DemoTaskHandlers} so real,
 * production task handlers can register themselves here the same way
 * without touching the demo handlers at all. */
@Component
public class TaskHandlerRegistry {

    private final Map<String, TaskHandler> handlersByName = new ConcurrentHashMap<>();

    public void register(String handlerName, TaskHandler handler) {
        handlersByName.put(handlerName, handler);
    }

    public Optional<TaskHandler> get(String handlerName) {
        return Optional.ofNullable(handlersByName.get(handlerName));
    }
}
