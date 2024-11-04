package it.pagopa.pn.tools.customspec;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@ToString
public class TemplateManifestModel {

    private List<TemplateModel> templates = new ArrayList<>();
}
