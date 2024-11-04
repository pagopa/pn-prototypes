package it.pagopa.pn.tools.customspec;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@ToString
public class TemplateModel {

    private String name;
    private String description;

    private String version;

    private Map<String, String> src = new HashMap<>();

    private Map<String, String> lang = new HashMap<>();

    private Map<String, Object> properties = new HashMap<>();

    private TemplateOutputFormat format;
}
