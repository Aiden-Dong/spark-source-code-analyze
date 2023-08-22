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

package test.org.apache.spark.sql.sources.v2;

import java.util.List;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.types.StructType;

public class JavaSchemaRequiredDataSource implements DataSourceV2, ReadSupport {

  class Reader implements DataSourceReader {
    private final StructType schema;

    Reader(StructType schema) {
      this.schema = schema;
    }

    @Override
    public StructType readSchema() {
      return schema;
    }

    @Override
    public List<InputPartition<InternalRow>> planInputPartitions() {
      return java.util.Collections.emptyList();
    }
  }

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    throw new IllegalArgumentException("requires a user-supplied schema");
  }

  @Override
  public DataSourceReader createReader(StructType schema, DataSourceOptions options) {
    return new Reader(schema);
  }
}
