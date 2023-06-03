package com.home.data.processing.examples.core.queue;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import ru.yoomoney.tech.dbqueue.config.QueueService;

@Component
public class DbQueueApplicationStartListener {

    private static final Logger log = LoggerFactory.getLogger(DbQueueApplicationStartListener.class);

    private final QueueService queueService;

    public DbQueueApplicationStartListener(QueueService queueService) {
        this.queueService = queueService;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startQueueService() {
        log.info("Starting DBQueue");
        queueService.start();
    }

}
