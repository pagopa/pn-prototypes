package it.pagopa.pn.splitcon020;

import it.pagopa.pn.commons.configs.listeners.TaskIdApplicationListener;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class PnSplitCon020Application {

    public static void main(String[] args) {
        System.setProperty("aws.region", "eu-central-1");
        SpringApplication app = new SpringApplication(PnSplitCon020Application.class);
        app.addListeners(new TaskIdApplicationListener());
        app.run(args);
    }

    @RestController
    public static class HomeController {

        @GetMapping("")
        public String home() {
            return "Sono Vivo";
        }
    }

}
