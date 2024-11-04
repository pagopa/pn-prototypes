package it.pagopa.pn.prj;


import it.pagopa.pn.template.client.TemplateApi;
import it.pagopa.pn.template.client.model.PECReceivedNotificationModel;
import org.springframework.stereotype.Component;

@Component
public class TemplateUsage {

    private final TemplateApi tmpl;

    public TemplateUsage(TemplateApi tmpl) {
        this.tmpl = tmpl;
    }

    public void generateAnAar() {
        PECReceivedNotificationModel model = new PECReceivedNotificationModel();
        model.setRecipientName("Ciccio Pasticcio");
        this.tmpl.generatePECReceivedNotification( "it", model );
    }

}
