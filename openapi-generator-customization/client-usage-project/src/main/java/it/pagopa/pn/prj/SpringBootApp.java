package it.pagopa.pn.prj;

import it.pagopa.pn.commons.template.TemplateGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication( scanBasePackages = {"it.pagopa.pn.prj", "it.pagopa.pn.template.client"})
public class SpringBootApp {
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(SpringBootApp.class);
        app.run(args);
    }

    @Bean
    public TemplateGenerator templateGenerator() {
        return new TemplateGenerator();
    }
}
