/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.apache.inlong.sort.base.metric.sub.source;

import static org.apache.inlong.sort.base.Constants.DELIMITER;
import static org.apache.inlong.sort.base.Constants.NUM_BYTES_IN;
import static org.apache.inlong.sort.base.Constants.NUM_RECORDS_IN;

import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.inlong.sort.base.Constants;
import org.apache.inlong.sort.base.metric.MetricOption;
import org.apache.inlong.sort.base.metric.MetricOption.RegisteredMetric;
import org.apache.inlong.sort.base.metric.MetricState;
import org.apache.inlong.sort.base.metric.SourceMetricData;
import org.apache.inlong.sort.base.metric.sub.AbstractSourceTableMetricData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection class for handling sub metrics of table schema type
 */
public class SourceTableMetricData extends AbstractSourceTableMetricData {

    public static final Logger LOGGER = LoggerFactory.getLogger(SourceTableMetricData.class);

    public SourceTableMetricData(MetricOption option, MetricGroup metricGroup) {
        super(option, metricGroup);
    }

    /**
     * build sub source metric data
     *
     * @param schemaInfoArray the schema info array of record
     * @param subMetricState sub metric state
     * @param sourceMetricData source metric data
     * @return sub source metric data
     */
    @Override
    public SourceMetricData buildSubSourceMetricData(String[] schemaInfoArray, MetricState subMetricState,
            SourceMetricData sourceMetricData) {
        if (sourceMetricData == null || schemaInfoArray == null) {
            return null;
        }
        // build sub source metric data
        Map<String, String> labels = sourceMetricData.getLabels();
        String metricGroupLabels = labels.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(DELIMITER));
        StringBuilder labelBuilder = new StringBuilder(metricGroupLabels);
        labelBuilder.append(DELIMITER).append(Constants.DATABASE_NAME).append("=").append(schemaInfoArray[0])
                .append(DELIMITER).append(Constants.TABLE_NAME).append("=").append(schemaInfoArray[1]);

        MetricOption metricOption = MetricOption.builder()
                .withInitRecords(subMetricState != null ? subMetricState.getMetricValue(NUM_RECORDS_IN) : 0L)
                .withInitBytes(subMetricState != null ? subMetricState.getMetricValue(NUM_BYTES_IN) : 0L)
                .withInlongLabels(labelBuilder.toString())
                .withRegisterMetric(RegisteredMetric.ALL)
                .build();
        return new SourceSchemaMetricData(metricOption, sourceMetricData.getMetricGroup());
    }

    /**
     * output metrics with estimate
     *
     * @param database the database name of record
     * @param table the table name of record
     * @param isSnapshotRecord is it snapshot record
     * @param data the data of record
     */
    public void outputMetricsWithEstimate(String database, String table, boolean isSnapshotRecord,
            Object data) {
        if (StringUtils.isBlank(database)  || StringUtils.isBlank(table)) {
            outputMetricsWithEstimate(data);
            return;
        }
        outputMetricsWithEstimate(new String[]{database, table}, isSnapshotRecord, data);
    }

    @Override
    public String toString() {
        return "SourceTableMetricData{"
                + "readPhaseMetricDataMap=" + readPhaseMetricDataMap
                + ", subSourceMetricMap=" + subSourceMetricMap
                + '}';
    }
}