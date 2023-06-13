/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.manager.common.fieldtype.datasource;

import org.apache.commons.lang3.StringUtils;

import java.util.stream.Stream;

import static org.apache.inlong.manager.common.consts.InlongConstants.LEFT_BRACKET;

/**
 * The enum of PostgreSQL field type mapping.
 */
public enum PostgreSQLFieldTypeMapping implements BaseFieldTypeMapping {

    /**
     * SMALLINT TYPE
     */
    SMALLINT("SMALLINT", "SMALLINT"),

    INT2("INT2", "SMALLINT"),

    SMALL_SERIAL("SMALLSERIAL", "SMALLINT"),

    SERIAL2("SERIAL2", "SMALLINT"),

    /**
     * INT TYPE
     */
    SERIAL("SERIAL", "INT"),

    INT4("INT4", "INT"),

    INT("INT", "INT"),

    INTEGER("INTEGER", "INT"),

    INT8("INT8", "BIGINT"),

    /**
     * BIGINT TYPE
     */
    BIGINT("BIGINT", "BIGINT"),

    BIGSERIAL("BIGSERIAL", "BIGINT"),

    /**
     * FLOAT TYPE
     */
    REAL("REAL", "FLOAT"),

    FLOAT4("FLOAT4", "FLOAT"),

    /**
     * DOUBLE TYPE
     */
    FLOAT8("FLOAT8", "DOUBLE"),

    DOUBLE("DOUBLE", "DOUBLE"),

    DOUBLE_PRECISION("DOUBLE PRECISION", "DOUBLE"),

    /**
     * DECIMAL TYPE
     */
    NUMERIC("NUMERIC", "DECIMAL"),

    DECIMAL("DECIMAL", "DECIMAL"),

    /**
     * BOOLEAN TYPE
     */
    BOOLEAN("BOOLEAN", "BOOLEAN"),

    /**
     * DATE TYPE
     */
    DATE("DATE", "DATE"),

    /**
     * TIME TYPE
     */
    TIME("TIME", "TIME"),

    TIMESTAMP("TIMESTAMP", "TIMESTAMP"),

    /**
     * STRING TYPE
     */
    CHAR("CHAR", "STRING"),

    CHARACTER("CHARACTER", "STRING"),

    VARCHAR("VARCHAR", "STRING"),

    CHARACTER_VARYING("CHARACTER VARYING", "STRING"),

    TEXT("TEXT", "STRING"),

    /**
     * BYTES TYPE
     */
    BYTEA("BYTEA", "VARBINARY"),

    /**
     * ARRAY TYPE
     */
    ARRAY("ARRAY", "ARRAY");

    /**
     * The original data type
     */
    private final String originalType;

    /**
     * The target data type
     */
    private final String targetType;

    PostgreSQLFieldTypeMapping(String originalType, String targetType) {
        this.originalType = originalType;
        this.targetType = targetType;
    }

    @Override
    public String getOriginalType() {
        return originalType;
    }

    @Override
    public String getTargetType() {
        return targetType;
    }

    /**
     * Get the field type of inlong field type mapping by the original field type.
     *
     * @param originalType the original field type
     * @return the target field type of inlong field type mapping
     */
    public static String getFieldTypeMapping(String originalType) {
        String dataType = StringUtils.substringBefore(originalType, LEFT_BRACKET).toUpperCase();
        return Stream.of(values()).filter(v -> v.getOriginalType().equals(dataType))
                .map(PostgreSQLFieldTypeMapping::getTargetType).findFirst().orElse(originalType.toUpperCase());
    }
}