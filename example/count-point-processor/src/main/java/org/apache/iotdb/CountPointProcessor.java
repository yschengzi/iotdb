package org.apache.iotdb;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.PipeProcessor;
import org.apache.iotdb.pipe.api.collector.EventCollector;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeProcessorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.tsfile.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

public class CountPointProcessor implements PipeProcessor {
  private static final String AGGREGATE_SERIES_KEY = "aggregate-series";
  private static AtomicLong writePointCount = new AtomicLong(0);

  private PartialPath aggregateSeries;

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validateRequiredAttribute(AGGREGATE_SERIES_KEY);
  }

  @Override
  public void customize(PipeParameters parameters, PipeProcessorRuntimeConfiguration configuration)
      throws Exception {
    this.aggregateSeries = new PartialPath(parameters.getString(AGGREGATE_SERIES_KEY));
  }

  @Override
  public void process(TabletInsertionEvent tabletInsertionEvent, EventCollector eventCollector)
      throws Exception {
    tabletInsertionEvent.processTablet(
        (tablet, rowCollector) -> {
          writePointCount.addAndGet(tablet.rowSize);
        });
  }

  @Override
  public void process(Event event, EventCollector eventCollector) throws Exception {
    if (event instanceof PipeHeartbeatEvent) {
      Tablet tablet =
          new Tablet(
              aggregateSeries.getDevice(),
              Collections.singletonList(
                  new MeasurementSchema(aggregateSeries.getMeasurement(), TSDataType.INT64)),
              1);
      tablet.rowSize = 1;
      tablet.addTimestamp(0, System.currentTimeMillis());
      tablet.addValue(aggregateSeries.getMeasurement(), 0, writePointCount.get());
      eventCollector.collect(new PipeRawTabletInsertionEvent(tablet, false, null, null, false));
    }
  }

  @Override
  public void close() throws Exception {}
}
