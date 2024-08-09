import com.fasterxml.jackson.core.JsonFactory;
import org.opensearch.client.json.JsonpSerializable;
import org.opensearch.client.json.jackson.JacksonJsonpGenerator;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch._types.FieldValue;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class OpenSearchUtil {

    /**
     * JsonpSerializable 객체를 JSON 문자열로 변환하여 반환
     * @param serializable
     * @return
     * @param <T>
     */
    public static <T extends JsonpSerializable> String convertToJson(T serializable) {
        final StringWriter writer = new StringWriter();
        try (final JacksonJsonpGenerator generator = new JacksonJsonpGenerator(new JsonFactory().createGenerator(writer))) {
            serializable.serialize(generator, new JacksonJsonpMapper());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return writer.toString();
    }

    /**
     * JsonpSerializable 객체를 JSON 문자열로 변환하여 출력
     * @param serializable
     * @param <T>
     */
    public static <T extends JsonpSerializable> void printJson(T serializable) {
        final StringWriter writer = new StringWriter();
        try (final JacksonJsonpGenerator generator = new JacksonJsonpGenerator(new JsonFactory().createGenerator(writer))) {
            serializable.serialize(generator, new JacksonJsonpMapper());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        System.out.println(writer.toString());
    }

    /**
     * Object 타입의 값을 FieldValue 타입으로 변환하여 List로 반환 <br><br>
     * OpenSearch에서는 terms query의 field에 대해 FieldValue 타입으로 된 List를 요구합니다. <br>
     * Elasticsearch는 terms query에서 Object 타입을 허용했지만, OpenSearch는 FieldValue 타입만 허용합니다. <br>
     * 쿼리에서 필터링에 사용할 값이 Object 타입이면 FieldValue로 변환하여 List로 모아서 사용해야 합니다. <br>
     *
     * @param value
     * @return List<FieldValue>
     *
     * @see FieldValue
     */
    public static List<FieldValue> getFieldValueList(Object value) {
        List<FieldValue> fieldValueList = new ArrayList<>();
        // integer, long, double, string, boolean, Consumer<FieldValue>...
        if (value instanceof Integer) {
            fieldValueList.add(FieldValue.of((Integer) value));
        } else if (value instanceof Long) {
            fieldValueList.add(FieldValue.of((Long) value));
        } else if (value instanceof Double) {
            fieldValueList.add(FieldValue.of((Double) value));
        } else if (value instanceof String) {
            fieldValueList.add(FieldValue.of((String) value));
        } else if (value instanceof Boolean) {
            fieldValueList.add(FieldValue.of((Boolean) value));
        } else if (value instanceof List<?> list) {
            for (Object obj : list) {
                if (obj instanceof Integer) {
                    fieldValueList.add(FieldValue.of((Integer) obj));
                } else if (obj instanceof Long) {
                    fieldValueList.add(FieldValue.of((Long) obj));
                } else if (obj instanceof Double) {
                    fieldValueList.add(FieldValue.of((Double) obj));
                } else if (obj instanceof String) {
                    fieldValueList.add(FieldValue.of((String) obj));
                } else if (obj instanceof Boolean) {
                    fieldValueList.add(FieldValue.of((Boolean) obj));
                }
            }
        } else if (value instanceof int[]) {
            for (int i : (int[]) value) {
                fieldValueList.add(FieldValue.of(i));
            }
        } else if (value instanceof long[]) {
            for (long l : (long[]) value) {
                fieldValueList.add(FieldValue.of(l));
            }
        } else if (value instanceof double[]) {
            for (double d : (double[]) value) {
                fieldValueList.add(FieldValue.of(d));
            }
        } else if (value instanceof String[]) {
            for (String str : (String[]) value) {
                fieldValueList.add(FieldValue.of(str));
            }
        } else if (value instanceof Consumer<?>) {
            ((Consumer<FieldValue>) value).accept(FieldValue.of(value.toString()));
        } else {
            fieldValueList.add(FieldValue.of(value.toString()));
        }
        return fieldValueList;
    }
}
