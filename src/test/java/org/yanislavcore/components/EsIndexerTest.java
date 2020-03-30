package org.yanislavcore.components;

import com.codahale.metrics.Counter;
import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.Status;
import org.apache.storm.Testing;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.MkTupleParam;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.yanislavcore.es.PagesIndexDao;
import org.yanislavcore.utils.TimeMachine;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.mockito.Mockito.*;

class EsIndexerTest {

    private final long start = System.currentTimeMillis();
    private final LocalDate startDate = LocalDate.now(ZoneOffset.UTC);
    private PagesIndexDao daoMock;
    private TimeMachine mockTimeMachine;
    private EsIndexer indexer;

    @BeforeEach
    void setUp() {
        daoMock = mock(PagesIndexDao.class);
        mockTimeMachine = mock(TimeMachine.class);
        when(mockTimeMachine.epochMillis()).thenReturn(start);
        when(mockTimeMachine.todayUtc()).thenReturn(startDate);
        indexer = new EsIndexer((conf) -> daoMock, mockTimeMachine);
        Map<String, Object> conf = Map.of(
                "pagesIndexer.indexBufferSize", 10L,
                "pagesIndexer.indexBufferTimeoutMillis", 10L,
                "pagesIndex", "pages"
        );
        TopologyContext mockTopologyContext = mock(TopologyContext.class);
        doReturn(mock(Counter.class)).when(mockTopologyContext).registerCounter(any());
        doNothing().when(daoMock).indexPages(anyString(), anyList(), any());
        indexer.prepare(conf, mockTopologyContext, null);
    }

    @Test
    void shouldSendBatchAfterBufferIsFull() {
        MkTupleParam param = new MkTupleParam();
        param.setFields("url", "status", "metadata");
        Tuple t = Testing.testTuple(
                new Values("http://test.com/path?query=id", Status.DISCOVERED, new Metadata(new HashMap<>())),
                param
        );
        for (int i = 0; i < 100; i++) {
            indexer.execute(t);
        }
        verify(daoMock, times(10)).indexPages(anyString(), anyList(), any());
        verifyNoMoreInteractions(daoMock);
    }

    @Test
    void shouldSendBatchAfterTimeout() {
        MkTupleParam param = new MkTupleParam();
        param.setFields("url", "status", "metadata");
        Tuple t = Testing.testTuple(
                new Values("http://test.com/path?query=id", Status.DISCOVERED, new Metadata(new HashMap<>())),
                param
        );
        for (int j = 1; j <= 5; j++) {
            for (int i = 0; i < 5; i++) {
                indexer.execute(t);
            }
            verify(daoMock, times(j - 1)).indexPages(anyString(), anyList(), any());
            when(mockTimeMachine.epochMillis()).thenReturn(start + 10 * j);
            indexer.execute(t);
            verify(daoMock, times(j)).indexPages(anyString(), anyList(), any());
            verifyNoMoreInteractions(daoMock);
        }
    }
}