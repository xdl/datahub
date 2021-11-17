package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.cassandra.CassandraAspectDao;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import java.util.Map;

import javax.annotation.Nonnull;


@Configuration
public class CassandraAspectDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "cassandraAspectDao")
  @ConditionalOnProperty(name = "ENTITY_SERVICE_IMPL", havingValue = "cassandra")
  @DependsOn({"gmsCassandraServiceConfig"})
  @Nonnull
  protected CassandraAspectDao createInstance() {
    return new CassandraAspectDao((Map<String, String>) applicationContext.getBean("gmsCassandraServiceConfig"));
  }
}