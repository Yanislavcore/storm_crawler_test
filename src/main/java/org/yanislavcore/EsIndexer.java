package org.yanislavcore;

import com.codahale.metrics.Counter;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.Status;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class EsIndexer implements IRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(EsIndexer.class);
    private static final DateTimeFormatter FORMAT = DateTimeFormatter.ISO_DATE;
    private final List<DocWriteRequest<?>> buffer = new ArrayList<>();
    private transient RestHighLevelClient client;
    private String indexName;
    private int bufferSize;
    private long bufferTimeoutMillis;
    private long lastFlushTimestamp;
    private Counter indexedPagesCounter;
    private Counter failedBatchesCounter;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        lastFlushTimestamp = System.currentTimeMillis();
        bufferSize = Math.toIntExact((long) stormConf.get("pagesIndexer.indexBufferSize"));
        bufferTimeoutMillis = (long) stormConf.get("pagesIndexer.indexBufferTimeoutMillis");
        indexName = (String) stormConf.get("pagesIndex");
        client = EsClientProvider.initOrGetClient(stormConf);
        indexedPagesCounter = context.registerCounter("indexedPages");
        failedBatchesCounter = context.registerCounter("failedBatches");
    }

    @Override
    public void execute(Tuple input) {
        //TODO At least once guarantee
        DocWriteRequest req = buildRequest(input);
        if (req == null) {
            return;
        }
        buffer.add(req);
        if (tryThreshold()) {
            LOG.info("Starting flush");
            flush();
        } else {
            LOG.debug("Skipping flush for now");
        }
    }

    private void flush() {
        BulkRequest req = new BulkRequest(indexName)
                .add(buffer);
        final int indexedPages = buffer.size();
        LOG.info("Sending request");
        client.bulkAsync(req, RequestOptions.DEFAULT, new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                indexedPagesCounter.inc(indexedPages);
                LOG.info("Successfully indexed {} pages to index {}", indexedPages, indexName);
            }

            @Override
            public void onFailure(Exception e) {
                LOG.error("Failed to index {} pages to index {}. {}", indexedPages, indexName, e);
                failedBatchesCounter.inc(indexedPages);
            }
        });
        lastFlushTimestamp = System.currentTimeMillis();
        buffer.clear();
    }

    private boolean tryThreshold() {
        return buffer.size() >= bufferSize || System.currentTimeMillis() - lastFlushTimestamp >= bufferTimeoutMillis;
    }

    /**
     * Build IndexRequest from input tuple.
     *
     * @param input - input tuple.
     * @return IndexRequest
     */
    @Nullable
    private DocWriteRequest buildRequest(@Nonnull Tuple input) {
        Objects.requireNonNull(input);
        Metadata md = (Metadata) input.getValueByField("metadata");
        LocalDate today = LocalDate.now();
        String todayFormatted = FORMAT.format(today);
        if (input.getFields().contains("status") && input.getValueByField("status") == Status.DISCOVERED) {
            String url = input.getStringByField("url");
            Map<String, Object> urlPart = createUrlPart(url);
            Map<String, Object> seenPart = Map.of(
                    "next", todayFormatted,
                    "last", todayFormatted,
                    "first", todayFormatted
            );
            Map<String, Object> source = Map.of(
                    "url", urlPart,
                    "seen", seenPart
            );
            return new IndexRequest(indexName)
                    .id(url)
                    .source(source, XContentType.JSON);
        } else if (md.getFirstValue("fetch.statusCode") == null) {
            LOG.warn("Unexpected value {}", md);
            return null;
        } else {
            String originUrl = input.getStringByField("url");
            Map<String, Object> urlPart = createUrlPart(originUrl);
            Map<String, Object> seenPart = Map.of(
                    "next", FORMAT.format(today.plusDays(30)),
                    "last", todayFormatted
            );
            Map<String, Object> source = Map.of(
                    "url", urlPart,
                    "seen", seenPart,
                    "title", Objects.requireNonNullElse(md.getFirstValue("parse.title"), ""),
                    "description", Objects.requireNonNullElse(md.getFirstValue("text"), ""),
                    "status", Integer.valueOf(md.getFirstValue("fetch.statusCode"))
            );

            //TODO move to constants
            return new UpdateRequest(indexName, md.getFirstValue("idUrl"))
                    .doc(source, XContentType.JSON);
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Nonnull
    private Map<String, Object> createUrlPart(@Nonnull String url) {
        Objects.requireNonNull(url);
        try {
            URL parsedUrl = new URL(url);
            return Map.of(
                    "domain", parsedUrl.getHost(),
                    "https", parsedUrl.getProtocol().equalsIgnoreCase("https"),
                    "path", Objects.requireNonNullElse(parsedUrl.getPath(), ""),
                    "query", Objects.requireNonNullElse(parsedUrl.getQuery(), "")
            );
        } catch (MalformedURLException e) {
            //TODO Mark Doc as invalid in ES
            throw new RuntimeException(e);
        }
    }
}
