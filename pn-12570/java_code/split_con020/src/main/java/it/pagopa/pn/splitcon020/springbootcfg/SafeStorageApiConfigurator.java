package it.pagopa.pn.splitcon020.springbootcfg;

import it.pagopa.pn.commons.pnclients.RestTemplateFactory;
import it.pagopa.pn.splitcon020.PnSplitCon020Configs;
import it.pagopa.pn.splitcon020.generated.openapi.msclient.safestorage.ApiClient;
import it.pagopa.pn.splitcon020.generated.openapi.msclient.safestorage.api.FileDownloadApi;
import it.pagopa.pn.splitcon020.generated.openapi.msclient.safestorage.api.FileMetadataUpdateApi;
import it.pagopa.pn.splitcon020.generated.openapi.msclient.safestorage.api.FileUploadApi;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.web.client.RestTemplate;

import javax.validation.constraints.NotNull;

@Configuration
public class SafeStorageApiConfigurator extends RestTemplateFactory {

    @Bean @Primary
    public FileUploadApi fileUploadApi(
            @Qualifier("withTracing") RestTemplate restTemplate,
            PnSplitCon020Configs cfg
    ) {
        return new FileUploadApi( getNewApiClient( restTemplate, cfg) );
    }

    @Bean @Primary
    public FileMetadataUpdateApi fileMetadataUpdate(
            @Qualifier("withTracing") RestTemplate restTemplate,
            PnSplitCon020Configs cfg
    ) {
        return new FileMetadataUpdateApi( getNewApiClient( restTemplate, cfg) );
    }

    @Bean @Primary
    public FileDownloadApi fileDownload(
            @Qualifier("withTracing") RestTemplate restTemplate,
            PnSplitCon020Configs cfg
    ) {
        return new FileDownloadApi( getNewApiClient( restTemplate, cfg) );
    }

    @NotNull
    private ApiClient getNewApiClient(RestTemplate restTemplate, PnSplitCon020Configs cfg) {
        ApiClient newApiClient = new ApiClient(restTemplate);
        newApiClient.setBasePath( cfg.getSafeStorageBaseUrl() );
        return newApiClient;
    }
}
