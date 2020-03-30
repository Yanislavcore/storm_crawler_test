package org.yanislavcore.components;

import com.digitalpebble.stormcrawler.Metadata;
import com.google.common.primitives.Ints;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;
import org.elasticsearch.client.Cancellable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.yanislavcore.es.PagesIndexDao;
import org.yanislavcore.es.data.PageUrlData;
import org.yanislavcore.utils.TimeMachine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class PagesSpoutTest {

    private PagesSpout spout;
    private PagesIndexDao daoMock;
    private TimeMachine mockTimeMachine;
    private SpoutOutputCollector mockCollector;
    private BiConsumer<List<PageUrlData>, Throwable> listener;
    private ArrayList<Values> emitedValues;
    private final long start = System.currentTimeMillis();

    @BeforeEach
    void setUp() {
        daoMock = mock(PagesIndexDao.class);
        mockTimeMachine = mock(TimeMachine.class);
        spout = new PagesSpout((any) -> daoMock, mockTimeMachine);
        TopologyContext mockTopologyContext = mock(TopologyContext.class);
        mockCollector = mock(SpoutOutputCollector.class);
        doAnswer(a -> listener = a.getArgument(3))
                .when(daoMock).searchScheduledUrls(any(), anyInt(), anyInt(), any());
        emitedValues = new ArrayList<>();
        doAnswer(a -> {
            emitedValues.add(a.getArgument(0));
            return Ints.asList(1);
        })
                .when(mockCollector).emit(any());

        when(mockTimeMachine.epochMillis()).thenReturn(start);
        when(mockTopologyContext.getThisComponentId()).thenReturn("");
        when(mockTopologyContext.getComponentTasks(any())).thenReturn(Ints.asList(1));
        Map<String, Object> confMap = Map.of(
                "pagesIndex", "pages",
                "pagesSpout.maxDocsPerRequestShard", 123L,
                "es.timeoutMillis", 321L
        );
        spout.open(confMap, mockTopologyContext, mockCollector);
    }

    @Test
    void shouldEmitResultsAfterResponse() {

        //Action
        spout.nextTuple();
        verifyNoInteractions(mockCollector);
        verify(daoMock, times(1)).searchScheduledUrls(any(), anyInt(), anyInt(), any());
        verifyNoMoreInteractions(daoMock);
        String testUrlString = "https://domain.com/path?query";
        listener.accept(List.of(new PageUrlData(testUrlString, testUrlString + "=id")), null);
        verifyNoInteractions(mockCollector);

        spout.nextTuple();
        verify(mockCollector, times(1)).emit(any());
        assertEquals(1, emitedValues.size());
        assertEquals(testUrlString, emitedValues.get(0).get(0));
        assertEquals(testUrlString + "=id", ((Metadata) emitedValues.get(0).get(1)).getFirstValue("idUrl"));
    }

    @Test
    void shouldRetryRequestAfterError() {

        //Action
        spout.nextTuple();
        verifyNoInteractions(mockCollector);

        verify(daoMock, times(1)).searchScheduledUrls(any(), anyInt(), anyInt(), any());
        verifyNoMoreInteractions(daoMock);

        listener.accept(null, new Exception("IT'S OK! TEST ERROR"));
        verifyNoInteractions(mockCollector);

        verify(daoMock, times(1)).searchScheduledUrls(any(), anyInt(), anyInt(), any());
        verifyNoMoreInteractions(daoMock);

        spout.nextTuple();
        verifyNoInteractions(mockCollector);

        verify(daoMock, times(2)).searchScheduledUrls(any(), anyInt(), anyInt(), any());
        verifyNoMoreInteractions(daoMock);
    }

    @Test
    void shouldRetryRequestAfterTimeout() {
        //Setup

        //Action
        spout.nextTuple();
        verifyNoInteractions(mockCollector);

        verify(daoMock, times(1)).searchScheduledUrls(any(), anyInt(), anyInt(), any());
        verifyNoMoreInteractions(daoMock);

        when(mockTimeMachine.epochMillis()).thenReturn(start + 100_000_000L);
        spout.nextTuple();
        verifyNoInteractions(mockCollector);

        verify(daoMock, times(2)).searchScheduledUrls(any(), anyInt(), anyInt(), any());
        verifyNoMoreInteractions(daoMock);
    }
}