// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;

#[tokio::test]
async fn create_view_with_alias() -> Result<()> {
    let session_ctx = SessionContext::with_config(
        SessionConfig::new().with_information_schema(true),
    );

    session_ctx
        .sql("CREATE TABLE abc AS VALUES (1,2,3), (4,5,6)")
        .await?
        .collect()
        .await?;

    let view_sql = "CREATE VIEW xyz (c1, c2) AS SELECT column1, column2 FROM abc"; // Create view using alias c1 for column1 and c2 for column2
    session_ctx.sql(view_sql).await?.collect().await?;

    let results = session_ctx.sql("SELECT * FROM information_schema.tables WHERE table_type='VIEW' AND table_name = 'xyz'").await?.collect().await?;
    assert_eq!(results[0].num_rows(), 1); // View exists, and is named appropriately

    let results = session_ctx
            .sql("SELECT * FROM xyz")
            .await?
            .collect()
            .await?;

        let expected = vec![
            "+---------+---------+",
            "| column1 | column2 |",
            "+---------+---------+",
            "| 1       | 2       |",
            "| 4       | 5       |",
            "+---------+---------+",
        ];

        assert_batches_eq!(expected, &results); // View contains expected data in expected format

    Ok(())
}