package com.home.data.processing.examples.core.model;

import com.home.data.processing.examples.core.util.DbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SortableMapper {

    public static Sortable toSortable(ResultSet rs, int rowNum) throws SQLException {
        return new Sortable(
                rs.getLong("id"),
                DbUtil.getEnum(rs, "status", SortableStatus.class),
                DbUtil.getEnum(rs, "type", SortableType.class)
        );
    }

}
