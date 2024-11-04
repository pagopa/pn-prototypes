package it.pagopa.pn.prj;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;


@SpringBootTest()
class TemplateUsageTest {

    @Autowired
    private TemplateUsage toTest;

    @Test
    public void doOneTest() {
        this.toTest.generateAnAar();
    }
}