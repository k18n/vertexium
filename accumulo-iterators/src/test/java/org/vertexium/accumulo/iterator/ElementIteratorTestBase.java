package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.client.impl.BaseIteratorEnvironment;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import static org.vertexium.accumulo.iterator.ElementIterator.*;

public class ElementIteratorTestBase {
    protected void init(ElementIterator it, SortedMap<Key, Value> map, Map<String, String> options) throws IOException {
        SortedMapIterator source = new SortedMapIterator(map);
        it.init(source, options, new DummyIteratorEnv());
    }

    protected static final class DummyIteratorEnv extends BaseIteratorEnvironment {
        @Override
        public IteratorUtil.IteratorScope getIteratorScope() {
            return IteratorUtil.IteratorScope.scan;
        }

        @Override
        public boolean isFullMajorCompaction() {
            return false;
        }
    }

    protected boolean benchmarkEnabled() {
        return Boolean.parseBoolean(System.getProperty("benchmark", "false"));
    }

    protected Map<String, String> getDefaultOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(SETTING_FETCH_HINTS_INCLUDE_ALL_PROPERTIES, "true");
        options.put(SETTING_FETCH_HINTS_PROPERTY_NAMES_TO_INCLUDE, "");
        options.put(SETTING_FETCH_HINTS_INCLUDE_ALL_PROPERTY_METADATA, "true");
        options.put(SETTING_FETCH_HINTS_METADATA_KEYS_TO_INCLUDE, "");
        options.put(SETTING_FETCH_HINTS_INCLUDE_HIDDEN, "true");
        options.put(SETTING_FETCH_HINTS_INCLUDE_ALL_EDGE_REFS, "true");
        options.put(SETTING_FETCH_HINTS_INCLUDE_OUT_EDGE_REFS, "true");
        options.put(SETTING_FETCH_HINTS_INCLUDE_IN_EDGE_REFS, "true");
        options.put(SETTING_FETCH_HINTS_EDGE_LABELS_OF_EDGE_REFS_TO_INCLUDE, "");
        options.put(SETTING_FETCH_HINTS_INCLUDE_EDGE_LABELS_AND_COUNTS, "true");
        options.put(SETTING_FETCH_HINTS_INCLUDE_EXTENDED_DATA_TABLE_NAMES, "true");
        return options;
    }

    protected void pkv(
            SortedMap<Key, Value> map,
            String row,
            String columnFamily,
            String columnQualifier,
            String columnVisibility,
            long timestamp,
            String value
    ) {
        pkv(map, row, columnFamily, columnQualifier, columnVisibility, timestamp, value.getBytes());
    }

    protected void pkv(
            SortedMap<Key, Value> map,
            String row,
            String columnFamily,
            String columnQualifier,
            String columnVisibility,
            long timestamp,
            byte[] value
    ) {
        map.put(
                new Key(new Text(row), new Text(columnFamily), new Text(columnQualifier), new Text(columnVisibility), timestamp),
                new Value(value, true)
        );
    }
}