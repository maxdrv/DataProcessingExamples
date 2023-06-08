package com.home.data.processing.examples.core.util;

import org.springframework.lang.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DbUtil {
    private DbUtil() {
    }

    @Nullable
    public static <T extends Enum<T>> T getEnum(ResultSet rs, String column, Class<T> clazz) throws SQLException {
        String result = rs.getString(column);
        return result == null ? null : Enum.valueOf(clazz, result);

    }
}
