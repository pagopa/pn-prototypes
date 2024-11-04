package it.pagopa.pn.commons.template;

public class TemplateGenerator {

  public byte[] generatePdfTemplate( String templateName, String language, Object model ) {
    System.out.println( "I have to render template \"" + templateName + "\" with language \"" + language + "\"");
    System.out.println( "and model " + model );
    System.out.println( "Returning PDF");
    return null;
  }

  public String generateStringTemplate( String templateName, String language, Object model ) {
    System.out.println( "I have to render template \"" + templateName + "\" with language \"" + language + "\"");
    System.out.println( "and model " + model );
    System.out.println( "Returning HTML");
    return null;
  }

}