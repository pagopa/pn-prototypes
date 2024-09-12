package it.pagopa.pn.splitcon020.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class Con020InputEventDto {

    private String clientId;
    private String eventTimestamp;
    private AnalogMail analogMail;

    public String getAttachmentUri() {
        return getAnalogMail().getAttachments()[0].getUri();
    }

    public String getAttachmentFileKey() {
        return getAttachmentUri().replaceFirst("^safestorage://", "");
    }


    @Getter
    @Setter
    @ToString
    public static class AnalogMail {

        private String requestId;
        private String registeredLetterCode;
        private String statusCode;
        private String statusDateTime;

        private Attachments[] attachments;

    }

    @Getter
    @Setter
    @ToString
    public static class Attachments {
        private String uri;

        private String sha256;
    }
}
