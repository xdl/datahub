package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.ebean.EbeanAspectDao;
import io.ebean.config.ServerConfig;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;


@Configuration
public class EbeanAspectDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "ebeanAspectDao")
  @DependsOn({"gmsEbeanServiceConfig"})
  @ConditionalOnProperty(name = "DAO_SERVICE_LAYER", havingValue = "ebean", matchIfMissing = true)
  @Nonnull
  protected EbeanAspectDao createInstance() {
    return new EbeanAspectDao(applicationContext.getBean(ServerConfig.class));
  }
}