package org.yanislavcore;

import com.digitalpebble.stormcrawler.Metadata;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

public class PagesSpout extends BaseRichSpout implements ActionListener<SearchResponse> {

    private static final Logger LOG = LoggerFactory
            .getLogger(PagesSpout.class);
    private final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
    private transient RestHighLevelClient client;
    private int shardID = -1;
    private int maxDocsPerShard;
    private String indexName;
    private long timeoutMillis;
    private long lastRequestTimestamp = 0;
    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        indexName = (String) conf.get("pagesIndex");
        maxDocsPerShard = Math.toIntExact((long) conf.get("pagesSpout.maxDocsPerRequestShard"));
        timeoutMillis = (long) conf.get("es.timeoutMillis");
        client = EsClientProvider.initOrGetClient(conf);
        initShardId(context);
    }

    private void initShardId(TopologyContext context) {
        int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
        if (totalTasks != 1) {
            shardID = context.getThisTaskIndex();
        } else {
            shardID = -1;
        }
    }

    private void tryRequestNextBatch() {
        if (System.currentTimeMillis() - lastRequestTimestamp > timeoutMillis) {
            LOG.debug("Timeout is passed, requesting again!");
            requestNextBatch();
            lastRequestTimestamp = System.currentTimeMillis();
        } else {
            LOG.debug("Request is in progress!");
        }
    }

    private void requestNextBatch() {
        SearchRequest request = new SearchRequest(indexName);
        BoolQueryBuilder queryBuilder = boolQuery().filter(QueryBuilders.rangeQuery("seen.next").lte("now/d"));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .fetchSource("url", null)
                .query(queryBuilder)
                .terminateAfter(maxDocsPerShard);

        request.source(sourceBuilder);
        if (shardID != -1) {
            request.preference("_shards:" + shardID);
        }
        LOG.debug("ES query {}", request);

        client.searchAsync(request, RequestOptions.DEFAULT, this);
    }

    @Override
    public void nextTuple() {
        String next = queue.poll();
        if (next != null) {
            collector.emit(new Values(next, new Metadata(new HashMap<>())));
        } else {
            tryRequestNextBatch();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "metadata"));
    }

    @Override
    public void onResponse(SearchResponse searchResponse) {
        try {
            for (SearchHit hit : searchResponse.getHits()) {
                URL buildUrl = tryBuildUrl((Map) hit.getSourceAsMap().get("url"));
                if (buildUrl == null) {
                    continue;
                }
                String url = buildUrl.toString();
                LOG.debug("Collecting url from db : {}", url);
                queue.add(url);
            }
        } finally {
            lastRequestTimestamp = 0;
        }
    }

    @Nullable
    private URL tryBuildUrl(Map urlComponents) {
        try {
            boolean isHttps = (boolean) urlComponents.get("https");
            String protocol = isHttps ? "https" : "http";
            int port = isHttps ? 443 : 80;
            return new URL(protocol, (String) urlComponents.get("domain"), port, (String) urlComponents.get("path"));
        } catch (MalformedURLException e) {
            //TODO Mark docs in DB as invalid
            LOG.error("Error, can't parse URL from DB! Data from DB: {}, exception: {}", urlComponents, e);
            return null;
        }
    }

    @Override
    public void onFailure(Exception e) {
        try {
            LOG.error("Error, while requesting new data for spout", e);
        } finally {
            lastRequestTimestamp = 0;
        }
    }
}
