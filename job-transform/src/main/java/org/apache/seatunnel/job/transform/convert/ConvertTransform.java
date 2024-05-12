/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.job.transform.convert;

import cn.hutool.core.convert.Convert;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.AbstractSeaTunnelTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowContainerGenerator;

import java.util.Locale;

@Slf4j
public class ConvertTransform extends AbstractSeaTunnelTransform {
    public static final String PLUGIN_NAME = "Convert";
    private final SeaTunnelRowContainerGenerator rowContainerGenerator;

    public ConvertTransform() {
        this.rowContainerGenerator = new SeaTunnelRowContainerGenerator() {
            @Override
            public SeaTunnelRow apply(SeaTunnelRow seaTunnelRow) {
                return seaTunnelRow.copy();
            }
        };
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow seaTunnelRow) {
        SeaTunnelRow tunnelRow = rowContainerGenerator.apply(seaTunnelRow);
        Object[] fields = tunnelRow.getFields();
        for (int i = 0; i < fields.length; i++) {
            tunnelRow.setField(i, Convert.toStr(fields[i]).toUpperCase(Locale.ROOT));
        }
        return tunnelRow;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        return CatalogTableUtil.buildSimpleTextTable();
    }
}
