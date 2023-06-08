package com.home.data.processing.examples.core.model;

import com.home.data.processing.examples.util.WithDataBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.BadSqlGrammarException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PgEnumTest extends WithDataBase {

    record PgType(String typeName, String enumLabel, int enumSortOrder) {
    }
    
    private PgType toPgType(ResultSet rs, int rowNum) throws SQLException {
        return new PgType(
                rs.getString("typname"),
                rs.getString("enumlabel"),
                rs.getInt("enumsortorder")
        );
    }

    @Test
    void values() {
        List<PgType> res = jdbcTemplate.query("""
                select t.typname, e.enumlabel, e.enumsortorder
                 from pg_type t, pg_enum e
                 where t.oid = e.enumtypid and typname = 'sortable_status'
                 order by e.enumsortorder asc;
                """, this::toPgType);

        assertThat(res)
                .extracting(PgType::enumLabel)
                .containsExactly(
                        SortableStatus.ARRIVED_DIRECT.name(),
                        SortableStatus.KEEPED_DIRECT.name(),
                        SortableStatus.SORTED_DIRECT.name(),
                        SortableStatus.PREPARED_DIRECT.name(),
                        SortableStatus.SHIPPED_DIRECT.name()
                );
    }

    @Test
    void registerIsImportant() {
        DataIntegrityViolationException exception = Assertions.assertThrows(
                DataIntegrityViolationException.class,
                () -> jdbcTemplate.update("insert into sortable(status) values ('sorted_direct')")
        );

        assertThat(exception.getMessage())
                .contains("input value for enum sortable_status: \"sorted_direct\"");
    }

    @Test
    void typeSafety() {
        jdbcTemplate.update("insert into sortable(status, type) values ('SHIPPED_DIRECT', 'PLACE')"); // PLACE is working

        DataIntegrityViolationException exception = Assertions.assertThrows(
                DataIntegrityViolationException.class,
                () -> jdbcTemplate.update("insert into sortable(status) values ('PLACE')") // PLACE is not a status
        );

        assertThat(exception.getMessage())
                .contains("input value for enum sortable_status: \"PLACE\"");
    }

    @Test
    void orderAsc() {
        jdbcTemplate.update("""
                insert into sortable(status) values
                ('SHIPPED_DIRECT'),  ('SHIPPED_DIRECT'), ('KEEPED_DIRECT'),
                ('KEEPED_DIRECT'), ('ARRIVED_DIRECT'), ('SHIPPED_DIRECT')
                """
        );

        List<Sortable> res = jdbcTemplate.query("select * from sortable order by status asc", SortableMapper::toSortable);

        assertThat(res)
                .extracting(Sortable::status)
                .containsExactly(
                        SortableStatus.ARRIVED_DIRECT,
                        SortableStatus.KEEPED_DIRECT,
                        SortableStatus.KEEPED_DIRECT,
                        SortableStatus.SHIPPED_DIRECT,
                        SortableStatus.SHIPPED_DIRECT,
                        SortableStatus.SHIPPED_DIRECT
                );
    }

    @Test
    void orderDesc() {
        jdbcTemplate.update("""
                insert into sortable(status) values
                ('SHIPPED_DIRECT'),  ('SHIPPED_DIRECT'), ('KEEPED_DIRECT'),
                ('KEEPED_DIRECT'), ('ARRIVED_DIRECT'), ('SHIPPED_DIRECT')
                """
        );

        List<Sortable> res = jdbcTemplate.query("select * from sortable order by status desc", SortableMapper::toSortable);

        assertThat(res)
                .extracting(Sortable::status)
                .containsExactly(
                        SortableStatus.SHIPPED_DIRECT,
                        SortableStatus.SHIPPED_DIRECT,
                        SortableStatus.SHIPPED_DIRECT,
                        SortableStatus.KEEPED_DIRECT,
                        SortableStatus.KEEPED_DIRECT,
                        SortableStatus.ARRIVED_DIRECT
                );
    }

    @Test
    void lessThan() {
        jdbcTemplate.update("""
                insert into sortable(status) values
                ('SHIPPED_DIRECT'),  ('SHIPPED_DIRECT'), ('KEEPED_DIRECT'),
                ('KEEPED_DIRECT'), ('ARRIVED_DIRECT'), ('SHIPPED_DIRECT')
                """
        );

        List<Sortable> res = jdbcTemplate.query(
                "select * from sortable where status < 'SHIPPED_DIRECT' order by status asc", SortableMapper::toSortable
        );

        assertThat(res)
                .extracting(Sortable::status)
                .containsExactly(
                        SortableStatus.ARRIVED_DIRECT,
                        SortableStatus.KEEPED_DIRECT,
                        SortableStatus.KEEPED_DIRECT
                );
    }

    @Test
    void min() {
        jdbcTemplate.update("""
                insert into sortable(status) values
                ('SHIPPED_DIRECT'),  ('SHIPPED_DIRECT'), ('KEEPED_DIRECT'),
                ('KEEPED_DIRECT'), ('ARRIVED_DIRECT'), ('SHIPPED_DIRECT')
                """
        );

        String res = jdbcTemplate.queryForObject(
                "select min(status) from sortable", String.class
        );

        assertThat(res).isEqualTo(SortableStatus.ARRIVED_DIRECT.name());
    }

    @Test
    void insert() {
        jdbcTemplate.update("insert into sortable(status) values ('SORTED_DIRECT')");

        List<Sortable> res = jdbcTemplate.query("select * from sortable", SortableMapper::toSortable);

        assertThat(res).extracting(Sortable::status).containsExactly(SortableStatus.SORTED_DIRECT);
    }

    @Test
    void insertParams() {
        jdbcTemplate.update(
                "insert into sortable(status) values (?::sortable_status)", SortableStatus.SORTED_DIRECT.name()
        );

        List<Sortable> res = jdbcTemplate.query("select * from sortable", SortableMapper::toSortable);

        assertThat(res).extracting(Sortable::status).containsExactly(SortableStatus.SORTED_DIRECT);
    }

    @Test
    void update() {
        jdbcTemplate.update("insert into sortable(status) values ('SORTED_DIRECT')");
        jdbcTemplate.update("update sortable set status = 'SHIPPED_DIRECT'");

        List<Sortable> res = jdbcTemplate.query("select * from sortable", SortableMapper::toSortable);

        assertThat(res).extracting(Sortable::status).containsExactly(SortableStatus.SHIPPED_DIRECT);
    }

    @Test
    void updateParams() {
        jdbcTemplate.update("insert into sortable(status) values ('SORTED_DIRECT')");
        jdbcTemplate.update(
                "update sortable set status = ?::sortable_status", SortableStatus.SHIPPED_DIRECT.name()
        );

        List<Sortable> res = jdbcTemplate.query("select * from sortable", SortableMapper::toSortable);

        assertThat(res).extracting(Sortable::status).containsExactly(SortableStatus.SHIPPED_DIRECT);
    }

    /**
     * https://stackoverflow.com/questions/851758/java-enums-jpa-and-postgres-enums-how-do-i-make-them-work-together
     * TODO: победить конвертацию типов
     */
    @Test
    void problemsWithParams() {
        BadSqlGrammarException exception = Assertions.assertThrows(
                BadSqlGrammarException.class,
                () -> jdbcTemplate.update(
                        "insert into sortable(status) values (?)", SortableStatus.SORTED_DIRECT.name()
                )
        );
        assertThat(exception.getMessage())
                .contains("bad SQL grammar [insert into sortable(status) values (?)]");

        assertThat(exception.getCause().getMessage())
                .contains("column \"status\" is of type sortable_status but expression is of type character varying");

        exception = Assertions.assertThrows(
                BadSqlGrammarException.class,
                () -> jdbcTemplate.update(
                        "insert into sortable(status) values (?::text)", SortableStatus.SORTED_DIRECT.name()
                )
        );
        assertThat(exception.getMessage())
                .contains("bad SQL grammar [insert into sortable(status) values (?::text)");

        assertThat(exception.getCause().getMessage())
                .contains("column \"status\" is of type sortable_status but expression is of type text");
    }

}
