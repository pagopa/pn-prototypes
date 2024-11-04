package it.pagopa.pn.tools.codegen.template.client;

import org.openapitools.codegen.languages.AbstractJavaCodegen;
import org.openapitools.codegen.*;
import io.swagger.models.properties.*;
import io.swagger.v3.oas.models.media.Schema;

import java.util.*;
import java.io.File;

public class TemplateClientGenGenerator extends AbstractJavaCodegen implements CodegenConfig {

  // source folder where to write the files
  protected String apiVersion = "1.0.0";

  /**
   * Configures the type of generator.
   *
   * @return  the CodegenType for this generator
   * @see     org.openapitools.codegen.CodegenType
   */
  public CodegenType getTag() {
    return CodegenType.OTHER;
  }

  /**
   * Configures a friendly name for the generator.  This will be used by the generator
   * to select the library with the -g flag.
   *
   * @return the friendly name for the generator
   */
  public String getName() {
    return "template-client-gen";
  }

  /**
   * Provides an opportunity to inspect and modify operation data before the code is generated.
   */
  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> postProcessOperationsWithModels(Map<String, Object> objs, List<Object> allModels) {

    // to try debugging your code generator:
    // set a break point on the next line.
    // then debug the JUnit test called LaunchGeneratorInDebugger

    Map<String, Object> results = super.postProcessOperationsWithModels(objs, allModels);

    Map<String, Object> ops = (Map<String, Object>)results.get("operations");
    ArrayList<CodegenOperation> opList = (ArrayList<CodegenOperation>)ops.get("operation");

    // iterate over the operation and perhaps modify something
    for(CodegenOperation co : opList){
      // example:
      // co.httpMethod = co.httpMethod.toLowerCase();
      if( "byte[]".equals( co.returnType )) {
        co.vendorExtensions.put("x-return-pdf", Boolean.TRUE);
      }
      else {
        co.vendorExtensions.put("x-return-string", Boolean.TRUE);
      }
      
    }

    return results;
  }

  /**
   * Returns human-friendly help for the generator.  Provide the consumer with help
   * tips, parameters here
   *
   * @return A string value for the help message
   */
  public String getHelp() {
    return "Generates a template-client-gen client library.";
  }

  public TemplateClientGenGenerator() {
    super();

    // set the output folder here
    outputFolder = "generated-code/template-client-gen";

    
    apiTestTemplateFiles.clear();

    /**
     * Template Location.  This is the location which templates will be read from.  The generator
     * will use the resource stream to attempt to read the templates.
     */
    templateDir = "template-client-gen";

    /**
     * Api Package.  Optional, if needed, this can be used in templates
     */
    apiPackage = "org.openapitools.api";

    /**
     * Model Package.  Optional, if needed, this can be used in templates
     */
    modelPackage = "org.openapitools.model";

    /**
     * Additional Properties.  These values can be passed to the templates and
     * are available in models, apis, and supporting files
     */
    additionalProperties.put("apiVersion", apiVersion);
    
  }

  @Override
  public void processOpts() {
    super.processOpts();
    additionalProperties.put("annotationLibrary", "none");
    importMapping.remove("ApiModelProperty");
    importMapping.remove("ApiModel");
  }

  @Override
  public CodegenModel fromModel(String name, Schema model) {
    CodegenModel result = super.fromModel( name, model );
    result.imports.remove("ApiModelProperty");
    result.imports.remove("ApiModel");
    return result;
  }

  @Override
  public void postProcessModelProperty(CodegenModel model, CodegenProperty property) {
    model.imports.remove("ApiModelProperty");
    model.imports.remove("ApiModel");
  }

}