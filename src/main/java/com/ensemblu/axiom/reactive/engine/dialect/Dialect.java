package com.ensemblu.axiom.reactive.engine.dialect;

import io.vertx.sqlclient.SqlClient;

public enum Dialect {
    POSTGRES, GENERIC;

    public String translate(String sql) {
        return switch (this) {
            case POSTGRES -> {
                final var sb = new StringBuilder();
                var count = 1;

                for (char c : sql.toCharArray()) {
                    if (c == '?') sb.append("$").append(count++);
                    else sb.append(c);
                }

                yield sb.toString();
            }
            case GENERIC -> sql;
        };
    }
}