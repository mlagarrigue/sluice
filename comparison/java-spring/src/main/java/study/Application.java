package study;

import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import study.contract.Contract;
import study.contract.Metrics;

/**
 * The study's Java contender: Spring Boot, Spring MVC on Tomcat, JdbcTemplate
 * on HikariCP (study 2.5, 2.6). The standard and optimised images run this
 * same code; what the optimised one changes is the runtime's settings,
 * listed with their sources in comparison/README.md.
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
        System.err.println("java-spring: " + env.getOrDefault("STUDY_PROTOCOL", "h1") + " on "
                + props.get("server.address") + ":" + props.get("server.port") + " ("
                + Contract.summary(env) + ")");
        Metrics.start();
        SpringApplication app = new SpringApplication(Application.class);
        app.setDefaultProperties(props);
        app.run(args);
    }
}
