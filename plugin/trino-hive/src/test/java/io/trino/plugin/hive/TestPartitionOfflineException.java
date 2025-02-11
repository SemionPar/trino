/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive;

import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import static org.testng.Assert.assertEquals;

public class TestPartitionOfflineException
{
    @Test
    public void testMessage()
    {
        assertMessage(new SchemaTableName("schema", "table"), "pk=1", false, "", "Table 'schema.table' partition 'pk=1' is offline");
        assertMessage(new SchemaTableName("schema", "table"), "pk=1", false, null, "Table 'schema.table' partition 'pk=1' is offline");
        assertMessage(new SchemaTableName("schema", "table"), "pk=1", true, "", "Table 'schema.table' partition 'pk=1' is offline for Presto");
        assertMessage(new SchemaTableName("schema", "table"), "pk=1", true, null, "Table 'schema.table' partition 'pk=1' is offline for Presto");
        assertMessage(new SchemaTableName("schema", "table"), "pk=1", false, "offline reason", "Table 'schema.table' partition 'pk=1' is offline: offline reason");
        assertMessage(new SchemaTableName("schema", "table"), "pk=1", true, "offline reason", "Table 'schema.table' partition 'pk=1' is offline for Presto: offline reason");
    }

    private static void assertMessage(SchemaTableName tableName, String partitionName, boolean forPresto, String offlineMessage, String expectedMessage)
    {
        PartitionOfflineException tableOfflineException = new PartitionOfflineException(tableName, partitionName, forPresto, offlineMessage);
        assertEquals(tableOfflineException.getMessage(), expectedMessage);
    }
}
