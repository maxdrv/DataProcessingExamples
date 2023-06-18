package com.home.data.processing.examples.configuration;

import io.zonky.test.db.postgres.embedded.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

@Configuration
public class TestConfig {

    @Bean
    public PreparedDbProvider preparedDbProvider() {
        DatabasePreparer preparer = LiquibasePreparer.forClasspathLocation("db/changelog/changelog.xml");

        List<Consumer<EmbeddedPostgres.Builder>> builderCustomizers = new CopyOnWriteArrayList<>();

        return PreparedDbProvider.forPreparer(preparer, builderCustomizers);
    }

    @Bean
    public ConnectionInfo connectionInfo(PreparedDbProvider provider) throws SQLException {
        return provider.createNewDatabase();
    }

    @Bean
    public DataSource dataSource(PreparedDbProvider provider, ConnectionInfo connectionInfo) throws SQLException {
        return provider.createDataSourceFromConnectionInfo(connectionInfo);
    }

}
