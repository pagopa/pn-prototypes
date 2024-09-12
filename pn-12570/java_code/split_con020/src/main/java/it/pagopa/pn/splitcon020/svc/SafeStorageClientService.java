package it.pagopa.pn.splitcon020.svc;

import it.pagopa.pn.commons.exceptions.PnInternalException;
import it.pagopa.pn.commons.exceptions.PnRuntimeException;
import it.pagopa.pn.splitcon020.PnSplitCon020Configs;
import it.pagopa.pn.splitcon020.generated.openapi.msclient.safestorage.api.FileDownloadApi;
import it.pagopa.pn.splitcon020.generated.openapi.msclient.safestorage.api.FileUploadApi;
import it.pagopa.pn.splitcon020.generated.openapi.msclient.safestorage.model.FileCreationRequest;
import it.pagopa.pn.splitcon020.generated.openapi.msclient.safestorage.model.FileCreationResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.*;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.MessageDigest;
import java.util.Collections;

@Service
@Slf4j
public class SafeStorageClientService {

    private final FileDownloadApi fileDownloadApi;
    private final FileUploadApi fileUploadApi;

    private final PnSplitCon020Configs cfg;

    public SafeStorageClientService(
            FileDownloadApi fileDownloadApi,
            FileUploadApi fileUploadApi,
            PnSplitCon020Configs cfg
    ) {
        this.fileDownloadApi = fileDownloadApi;
        this.fileUploadApi = fileUploadApi;
        this.cfg = cfg;
    }

    public InputStream download( String fileKey ) {
        String presignedUrl = fileDownloadApi
                .getFile( fileKey, cfg.getSafeStorageUser(), false)
                .getDownload()
                .getUrl();

        if( StringUtils.hasText( presignedUrl ) ) {
            try {
                return new URL( presignedUrl ).openStream();
            } catch (IOException exc) {
                throw new PnRuntimeException( "IOEXCEPTION", exc.getMessage(), 500, Collections.emptyList(), exc );
            }
        }
        else {
            throw new PnInternalException("Download non available for safeStorageFileKey=" + fileKey);
        }
    }

    public void changeStatusToPdf(String pdfSafeStorageFileKey, String attached) {
        
    }

    public String uploadPdf(byte[] pdfBytes) {

        FileCreationRequest fileCreationRequest = new FileCreationRequest();
        fileCreationRequest.setContentType("application/pdf");
        fileCreationRequest.setDocumentType("PN_PRINTED");
        fileCreationRequest.setStatus("PRELOADED");

        String sha256 = computeSha256( pdfBytes );

        FileCreationResponse response = fileUploadApi.createFile(
                      cfg.getSafeStorageUser(), "sha256", sha256, fileCreationRequest );

        uploadContent( response, fileCreationRequest.getContentType(), sha256, pdfBytes );

        return response.getKey();
    }


    private String computeSha256( byte[] content ) {

        try{
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] encodedhash = digest.digest( content );
            return bytesToBase64( encodedhash );
        } catch (Exception exc) {
            throw new PnInternalException("cannot compute sha256", "no256", exc );
        }
    }

    private static String bytesToBase64(byte[] hash) {
        return Base64Utils.encodeToString( hash );
    }

    public void uploadContent(FileCreationResponse fileCreationResponse, String contentType, String sha256, byte[] content) {
        try {
            MultiValueMap<String, String> headers = new LinkedMultiValueMap<>();
            headers.add("Content-type", contentType);
            headers.add("x-amz-checksum-sha256", sha256);
            headers.add("x-amz-meta-secret", fileCreationResponse.getSecret());

            HttpEntity<Resource> req = new HttpEntity<>(new ByteArrayResource( content), headers);

            URI url = URI.create(fileCreationResponse.getUploadUrl());
            HttpMethod method = fileCreationResponse.getUploadMethod() == FileCreationResponse.UploadMethodEnum.POST ? HttpMethod.POST : HttpMethod.PUT;

            ResponseEntity<String> res = new RestTemplate().exchange(url, method, req, String.class);

            if (res.getStatusCodeValue() != org.springframework.http.HttpStatus.OK.value())
            {
                throw new PnInternalException("File upload failed", "UPLERR");
            }

        } catch (PnInternalException ee)
        {
            log.error("uploadContent PnInternalException uploading file", ee);
            throw ee;
        }
        catch (Exception ee)
        {
            log.error("uploadContent Exception uploading file", ee);
            throw new PnInternalException("Exception uploading file", "UPLERR", ee);
        }
    }

}
