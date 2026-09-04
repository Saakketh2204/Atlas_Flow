package com.atlasflow.worker;

/** A unit of work a worker can execute for a given task attempt. Registered
 * by name in {@link TaskHandlerRegistry} and looked up via
 * {@code TaskDefinition.handlerName()} -- see {@link DemoTaskHandlers} for
 * the example implementations used by the ProcessOrder demo workflow. */
@FunctionalInterface
public interface TaskHandler {
    /** @throws Exception on any failure -- the caller (TaskEventListener)
     * treats any thrown exception as a task failure and routes it into
     * AtlasFlow's retry/dead-letter logic. Handlers should NOT catch and
     * swallow their own errors; letting them propagate is what makes retry
     * behavior demonstrable and correct. */
    void handle(String executionId, String taskId) throws Exception;
}
