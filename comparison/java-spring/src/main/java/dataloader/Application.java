package dataloader;

import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import study.contract.Contract;
import study.contract.Metrics;

/**
 * The Java contender "with" grouping (study 2.1, 2.6): study.Application
 * whose unit reads go through a DataLoader. A package of its own, so that
 * neither application's component scan finds the other's controller; the
 * image starts this class instead of the jar's own.
 */
@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        Map<String, String> env = System.getenv();
        Map<String, Object> props;
        try {
            props = Contract.properties(env);
        } catch (Contract.Refused e) {
            System.err.println(e.getMessage());
            System.exit(1);
            return;
        }
        System.err.println("java-spring-dataloader: " + env.getOrDefault("STUDY_PROTOCOL", "h1") + " on "
                + props.get("server.address") + ":" + props.get("server.port") + " ("
                + Contract.summary(env) + ")");
        Metrics.start();
        SpringApplication app = new SpringApplication(Application.class);
        app.setDefaultProperties(props);
        app.run(args);
    }
}
