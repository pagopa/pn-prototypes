package it.pagopa.pn.splitcon020;

import it.pagopa.pn.commons.conf.SharedAutoConfiguration;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import javax.annotation.PostConstruct;

@Configuration
@ConfigurationProperties( prefix = "pn.splitcon020")
@Data
@Import(SharedAutoConfiguration.class)
@Slf4j
public class PnSplitCon020Configs {

    private String safeStorageBaseUrl;
    private String safeStorageUser;
    private String eventInputQueueName;
    private String archiveToUnpackageQueueName;
    private String paperEventEnrichmentTable;

    @PostConstruct
    public void init(){
        log.info("CONFIGURATION {}",this);
    }

}
