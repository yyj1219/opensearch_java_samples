import com.fasterxml.jackson.core.JsonFactory;
import org.opensearch.client.json.jackson.JacksonJsonpGenerator;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.json.JsonpSerializable;

import java.io.StringWriter;

public class OpenSearchUtil {

    public static <T extends JsonpSerializable> String convertToJson(T serializable) {
        final StringWriter writer = new StringWriter();
        try (final JacksonJsonpGenerator generator = new JacksonJsonpGenerator(new JsonFactory().createGenerator(writer))) {
            serializable.serialize(generator, new JacksonJsonpMapper());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return writer.toString();
    }

}
