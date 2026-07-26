package com.ensemblu.axiom.reactive.engine;

import com.ensemblu.axiom.api.Axiom;
import com.ensemblu.axiom.core.data_structure.list.PersistentList;
import com.ensemblu.axiom.core.data_structure.map.PersistentMap;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

public interface ReactiveResultConverter {

    static PersistentList<PersistentMap<String, Object>> convert(RowSet<Row> rows) {
        var builder = Axiom.Data.<PersistentMap<String, Object>>emptyList().asTransient();

        for (final var row : rows) {
            var rowMap = Axiom.Data.<String, Object>emptyMap().asTransient();

            for (var i = 0; i < row.size(); i++) {
                final var label = row.getColumnName(i);
                final var val = row.getValue(i);

                rowMap = rowMap.put(label, val);
            }

            builder = builder.append(rowMap.freeze());
        }

        return builder.freeze();
    }
}