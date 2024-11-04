package it.pagopa.pn.tools.customspec;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class TemplateManifest2OpenAPI implements Runnable {

    @Override
    public void run() {
        try {
            runWithThrow();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void runWithThrow() throws Exception {
        String fromFile = System.getProperty("fromFile");
        String toFile = System.getProperty("toFile");
        System.out.println("HAVE TO TRANSFORM FILES " + fromFile + " --> " + toFile );

        Path fromPath = Paths.get( fromFile );
        String openApi = generateOpenApiFromManifest( fromPath );

        Path toPath = Paths.get( toFile );
        Files.createDirectories( toPath.getParent() );
        Files.writeString(toPath, openApi, new OpenOption[]{});
    }

    protected String generateOpenApiFromManifest(Path fromPath) throws IOException {
        try( InputStream inStrm = Files.newInputStream(fromPath)) {
            StringBuilder openApi = new StringBuilder();
            openApi.append("openapi: \"3.0.0\"\n")
                   .append("info:\n")
                   .append("  version: 1.0.0\n")
                   .append("  title: Template example\n")
                   .append("  license:\n")
                   .append("    name: MIT\n")
                   .append("paths:\n");

            Yaml yamlParser = new Yaml();
            TemplateManifestModel yamlData = yamlParser.loadAs( inStrm, TemplateManifestModel.class );

            for( TemplateModel t: yamlData.getTemplates() ) {
                openApi.append( writeOperation( t ));
            }

            openApi.append("components:\n")
                   .append("  schemas:\n");

            String absolutizeRelatives = fromPath.getParent().toString();
            for( TemplateModel t: yamlData.getTemplates() ) {
                openApi.append( writeType( t, absolutizeRelatives ));
            }

            return openApi.toString();
        }
    }

    protected String writeOperation( TemplateModel t ) {
        StringBuilder openApi = new StringBuilder();
        openApi.append("  '/templates/").append( t.getName() ).append("':\n")
               .append("    post:\n")
               .append("      operationId: generate").append( t.getName() ).append("\n")
               .append("      description:").append( t.getDescription() ).append("\n")
               .append("      tags:\n")
               .append("        - template\n")
               .append("      parameters:\n")
               .append("        - name: language\n")
               .append("          in: query\n")
               .append("          description: language two letter code\n")
               .append("          required: true\n")
               .append("          schema:\n")
               .append("            type: string\n")
               .append("      requestBody:\n")
               .append("        required: true\n")
               .append("        content:\n")
               .append("          application/json:\n")
               .append("            schema:\n")
               .append("              $ref: \"#/components/schemas/").append( t.getName() ).append("Model\"\n")
               .append("      responses:\n")
               .append("        '200':\n")
               .append("          description: Tamplate Generated\n")
               .append("          content:\n")
               .append("            application/json:\n")
               .append("              schema:\n")
               .append("                type: string\n");
        if( TemplateOutputFormat.PDF == t.getFormat() ) {
            openApi
               .append("              format: byte\n");
        }
        return  openApi.toString();
    }

    protected String writeType( TemplateModel t, String fromFolder ) {
        Yaml yamlWriter = new Yaml();

        StringBuilder openApi = new StringBuilder();
        openApi.append("    ").append( t.getName() ).append("Model:\n")
               .append("      type: object\n")
               .append("      properties:\n");

        for( Map.Entry<String, Object> entry: t.getProperties().entrySet() ) {
            String propertyType = yamlWriter.dump( entry.getValue() );
            propertyType = Arrays.asList( propertyType.split("\n")).stream()
                    .map( el -> "          " + el )
                            .collect(Collectors.joining("\n"));

            openApi
               .append("        ").append( entry.getKey() ).append(":\n")
               .append(  propertyType ).append("\n");
        }

        return openApi.toString().replaceAll("\\$ref: \\./", "\\$ref: " + fromFolder + "/" );
    }
}
