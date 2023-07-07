package com.ibm.eventstreams.connect.mqsource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MQSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(MQSourceTask.class);

    private int batchSize = MQSourceConnector.CONFIG_VALUE_MQ_BATCH_SIZE_DEFAULT;
    private CountDownLatch batchCompleteSignal = null;
    private AtomicInteger pollCycle = new AtomicInteger(1);
    private int lastCommitPollCycle = 0;
    private AtomicBoolean stopNow = new AtomicBoolean();

    private JMSReader reader;
    private OffsetStorageReader offsetStorageReader;
    private OffsetStorageWriter offsetStorageWriter;

    private Map<TopicPartition, Long> currentOffsets = new HashMap<>();

    public MQSourceTask() {
    }

    @Override
    public String version() {
        return MQSourceConnector.version;
    }

    @Override
    public void start(final Map<String, String> props) {
        log.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(), this.getClass().getName(), props);

        for (final Entry<String, String> entry : props.entrySet()) {
            final String value;
            if (entry.getKey().toLowerCase().contains("password")) {
                value = "[hidden]";
            } else {
                value = entry.getValue();
            }
            log.debug("Task props entry {}: {}", entry.getKey(), value);
        }

        final String strBatchSize = props.get(MQSourceConnector.CONFIG_NAME_MQ_BATCH_SIZE);
        if (strBatchSize != null) {
            batchSize = Integer.parseInt(strBatchSize);
        }

        reader = new JMSReader();
        reader.configure(props);
        reader.connect();

        offsetStorageReader = context.offsetStorageReader();
        offsetStorageWriter = context.offsetStorageWriter();

        // Retrieve the last committed offsets from the storage
        Map<String, Object> offsetStorage = offsetStorageReader.offsets(partitions());
        for (Entry<String, Object> entry : offsetStorage.entrySet()) {
            String topicPartition = entry.getKey();
            long offset = (long) entry.getValue();
            TopicPartition tp = parseTopicPartition(topicPartition);
            currentOffsets.put(tp, offset);
        }

        log.trace("[{}] Exit {}.start", Thread.currentThread().getId(), this.getClass().getName());
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        log.trace("[{}] Entry {}.poll", Thread.currentThread().getId(), this.getClass().getName());

        final List<SourceRecord> msgs = new ArrayList<>();
        int messageCount = 0;

        if (batchCompleteSignal != null) {
            log.debug("Awaiting batch completion signal");
            batchCompleteSignal.await();

            log.debug("Committing records");
            offsetStorageWriter.beginFlush();
            offsetStorageWriter.flush();
        }

        final int currentPollCycle = pollCycle.incrementAndGet();
        log.debug("Starting poll cycle {}", currentPollCycle);

        try {
            if (!stopNow.get()) {
                log.debug("Polling for records");
                SourceRecord src;
                do {
                    src = reader.receive(messageCount == 0, currentOffsets);
                    if (src != null) {
                        msgs.add(src);
                        messageCount++;

                        TopicPartition tp = new TopicPartition(src.topic(), src.kafkaPartition());
                        currentOffsets.put(tp, src.kafkaOffset() + 1);
                    }
                } while (src != null && messageCount < batchSize && !stopNow.get());
            } else {
                log.info("Stopping polling for records");
            }
        } catch (Exception e) {
            log.error("Error while polling records: {}", e.getMessage());
        }

        synchronized (this) {
            if (messageCount > 0) {
                if (!stopNow.get()) {
                    batchCompleteSignal = new CountDownLatch(messageCount);
                } else {
                    log.debug("Discarding a batch of {} records as the task is stopping", messageCount);
                    msgs.clear();
                    batchCompleteSignal = null;
                }
            } else {
                batchCompleteSignal = null;
            }
        }

        log.debug("Poll returning {} records", messageCount);

        log.trace("[{}] Exit {}.poll, retval={}", Thread.currentThread().getId(), this.getClass().getName(),
                messageCount);
        return msgs;
    }

    @Override
    public void commit() throws InterruptedException {
        log.trace("[{}] Entry {}.commit", Thread.currentThread().getId(), this.getClass().getName());

        synchronized (this) {
            if (batchCompleteSignal != null) {
                for (Entry<TopicPartition, Long> entry : currentOffsets.entrySet()) {
                    TopicPartition tp = entry.getKey();
                    Long offset = entry.getValue();
                    String topicPartition = formatTopicPartition(tp);
                    offsetStorageWriter.offset(topicPartition, offset);
                }
            }
        }

        log.trace("[{}] Exit {}.commit", Thread.currentThread().getId(), this.getClass().getName());
    }

    @Override
    public void stop() {
        log.trace("[{}] Entry {}.stop", Thread.currentThread().getId(), this.getClass().getName());

        stopNow.set(true);

        synchronized (this) {
            if (reader != null) {
                reader.close();
            }
        }

        log.trace("[{}] Exit {}.stop", Thread.currentThread().getId(), this.getClass().getName());
    }

    @Override
    public void commitRecord(final SourceRecord record) throws InterruptedException {
        log.trace("[{}] Entry {}.commitRecord, record={}", Thread.currentThread().getId(), this.getClass().getName(), record);

        synchronized (this) {
            batchCompleteSignal.countDown();
        }

        log.trace("[{}] Exit {}.commitRecord", Thread.currentThread().getId(), this.getClass().getName());
    }

    private TopicPartition parseTopicPartition(String topicPartition) {
        String[] parts = topicPartition.split("-");
        String topic = parts[0];
        int partition = Integer.parseInt(parts[1]);
        return new TopicPartition(topic, partition);
    }

    private String formatTopicPartition(TopicPartition tp) {
        return tp.topic() + "-" + tp.partition();
    }
}
