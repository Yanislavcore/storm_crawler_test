package org.yanislavcore.es;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
import org.yanislavcore.es.data.PageUrlData;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

public class PagesIndexDao {
    private final static Logger LOG = LoggerFactory.getLogger(PagesIndexDao.class);
    private final RestHighLevelClient client;

    public PagesIndexDao(RestHighLevelClient client) {
        Objects.requireNonNull(client);
        this.client = client;
    }

    public void indexPages(@Nonnull String indexName,
                           @Nonnull List<DocWriteRequest<?>> pages,
                           @Nonnull BiConsumer<Void, Throwable> listener) {
        BulkRequest req = new BulkRequest(indexName)
                .add(pages);
        final int indexedPages = pages.size();
        LOG.debug("Sending request with {} pages", pages.size());
        client.bulkAsync(req, RequestOptions.DEFAULT, new ActionListener<>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                LOG.debug("Successfully indexed {} pages to index {}", indexedPages, indexName);
                listener.accept(null, null);
            }

            @Override
            public void onFailure(Exception e) {
                LOG.debug("Failed to index {} pages to index {}. {}", indexedPages, indexName, e);
                listener.accept(null, e);
            }
        });
    }

    public void searchScheduledUrls(@Nonnull String indexName,
                                    int maxDocsPerShard,
                                    int shardId,
                                    @Nonnull BiConsumer<List<PageUrlData>, Throwable> listener) {
        Objects.requireNonNull(listener);
        Objects.requireNonNull(indexName);
        SearchRequest request = new SearchRequest(indexName);
        BoolQueryBuilder queryBuilder = boolQuery().filter(QueryBuilders.rangeQuery("seen.next").lte("now/d"));
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .fetchSource("url", null)
                .query(queryBuilder)
                .terminateAfter(maxDocsPerShard)
                .size(maxDocsPerShard);

        request.source(sourceBuilder)
                .requestCache(false);
        if (shardId != -1) {
            request.preference("_shards:" + shardId);
        }
        LOG.debug("ES query {}", request);

        client.searchAsync(request, RequestOptions.DEFAULT, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                try {
                    ArrayList<PageUrlData> result = new ArrayList<>();
                    for (SearchHit hit : searchResponse.getHits()) {
                        URL buildUrl = tryBuildUrl((Map) hit.getSourceAsMap().get("url"));
                        if (buildUrl == null) {
                            continue;
                        }
                        //ID is limited to 512 bytes while real url could be longer
                        String realUrl = buildUrl.toString();
                        String idUrl = hit.getId();
                        LOG.debug("Collecting url from db : {}", realUrl);
                        result.add(new PageUrlData(realUrl, idUrl));

                    }
                    LOG.debug("Found {} urls", result.size());
                    listener.accept(result, null);
                } catch (Throwable t) {
                    listener.accept(null, new RuntimeException("Fail, while parsing response", t));
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.accept(null, e);
            }
        });
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
}
