package org.vertexium.accumulo.iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.ArrayList;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.junit.Assume.assumeTrue;
import static org.vertexium.accumulo.iterator.ElementIterator.CF_PROPERTY_STRING;
import static org.vertexium.accumulo.iterator.VertexIterator.CF_SIGNAL_STRING;
import static org.vertexium.accumulo.iterator.model.KeyBase.VALUE_SEPARATOR;

public class VertexIteratorTest extends ElementIteratorTestBase {
    @Test
    public void benchmark() throws Exception {
        assumeTrue(benchmarkEnabled());

        int elementCount = 1000;
        int propertyCount = 1000;

        VertexIterator it = new VertexIterator();

        SortedMap<Key, Value> map = new TreeMap<>();
        for (int elementId = 0; elementId < elementCount; elementId++) {
            String row = "v" + elementId;
            pkv(map, row, CF_SIGNAL_STRING, "", "", 1, "");
            for (int propertyId = 0; propertyId < propertyCount; propertyId++) {
                pkv(map, row, CF_PROPERTY_STRING, String.format("prop%08x%sk1", propertyId, VALUE_SEPARATOR), "", 1, "test value " + propertyId);
            }
        }

        init(it, map, getDefaultOptions());

        Range range = new Range(new Text("a"), true, new Text("z"), true);
        it.seek(range, new ArrayList<>(), false);

        System.out.println("begin sleep");
        Thread.sleep(10000);
        System.out.println("end sleep");

        long startTime = System.currentTimeMillis();
        while (it.hasTop()) {
            it.next();
        }
        long endTime = System.currentTimeMillis();

        System.out.println(String.format("time: %dms", endTime - startTime));
    }
}