package com.home.data.processing.examples.core.queue.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yoomoney.tech.dbqueue.api.QueueConsumer;
import ru.yoomoney.tech.dbqueue.api.Task;
import ru.yoomoney.tech.dbqueue.api.TaskExecutionResult;
import ru.yoomoney.tech.dbqueue.api.TaskPayloadTransformer;
import ru.yoomoney.tech.dbqueue.api.impl.NoopPayloadTransformer;
import ru.yoomoney.tech.dbqueue.settings.QueueConfig;

public class StringQueueConsumer implements QueueConsumer<String> {

    private static final Logger log = LoggerFactory.getLogger(StringQueueConsumer.class);

    private final QueueConfig queueConfig;

    public StringQueueConsumer(QueueConfig queueConfig) {
        this.queueConfig = queueConfig;
    }

    @Override
    public TaskExecutionResult execute(Task<String> task) {
        log.info("payload={}", task.getPayloadOrThrow());
        return TaskExecutionResult.finish();
    }

    @Override
    public QueueConfig getQueueConfig() {
        return queueConfig;
    }

    @Override
    public TaskPayloadTransformer<String> getPayloadTransformer() {
        return NoopPayloadTransformer.getInstance();
    }
}
