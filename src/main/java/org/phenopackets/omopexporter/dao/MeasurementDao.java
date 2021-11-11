package org.phenopackets.omopexporter.dao;

import org.phenopackets.phenotools.builder.builders.*;
import org.phenopackets.schema.v2.core.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Component
public class MeasurementDao {

    private final JdbcTemplate jdbcTemplate;

    public MeasurementDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Measurement> getMeasurements(int personId) {
        String query = "select m.person_id,\n" +
                "       m.measurement_concept_id,\n" +
                "       concat(c.vocabulary_id,':',c.concept_code) as assay_id,\n" +
                "       c.concept_name as assay_label,\n" +
                "    -- value_as_number maps to a phenopacket Value\n" +
                "       m.value_as_number,\n" +
                "    -- value_id and value_label map to a phenopacket OntologyClass\n" +
                "       concat(c3.vocabulary_id,':',c3.concept_code) as value_id,\n" +
                "       m.value_source_value as value_label,\n" +
                "       m.measurement_datetime,\n" +
                "    -- phenopacket Quantity\n" +
                "       concat(c2.vocabulary_id,':',c2.concept_code) as unit_id,\n" +
                "       c2.concept_name as unit_label,\n" +
                "    -- if range_low and range_high are not null, create a ReferenceRange\n" +
                "       m.range_low,\n" +
                "       m.range_high,\n" +
                "       c2.concept_id,\n" +
                "       m.unit_source_value,\n" +
                "       m.visit_occurrence_id,\n" +
                "/*        c3.concept_name as procedure, */\n" +
                "       row_number() over (partition by m.person_id, m.measurement_datetime, m.visit_occurrence_id)\n" +
                "FROM measurement m\n" +
                "         left join concept c on c.concept_id = m.measurement_concept_id\n" +
                "         left join concept c2 on c2.concept_id = m.unit_concept_id\n" +
                "         left join concept c3 on c3.concept_id = m.value_as_concept_id\n" +
                "WHERE m.person_id = ?";

        return jdbcTemplate.query(query, this::mapRowToMeasurement, personId);
    }

    private Measurement mapRowToMeasurement(ResultSet resultSet, int i) throws SQLException {
        OntologyClass assay = OntologyClassBuilder.ontologyClass(resultSet.getString("assay_id"), resultSet.getString("assay_label"));
        double valueAsNumber = resultSet.getDouble("value_as_number");
        Value value;
        if (valueAsNumber != 0) {
            String unitLabel = resultSet.getString("unit_label");
            if (unitLabel != null && unitLabel.equals("No matching concept")) {
                // hack to try and add in something meaningful
                unitLabel = resultSet.getString("unit_source_value");
            }
            Quantity quantity = QuantityBuilder.quantity(resultSet.getString("unit_id"), unitLabel, valueAsNumber);
            value = ValueBuilder.value(quantity);
        } else {
            String valueId = resultSet.getString("value_id");
            String valueLabel = resultSet.getString("value_label");
            OntologyClass ontologyClass = OntologyClassBuilder.ontologyClass(valueId == null ? "" : valueId, valueLabel == null ? "" : valueLabel);
            value = ValueBuilder.value(ontologyClass);
        }
        TimeElement timeObserved = TimeElements.timestamp(resultSet.getTimestamp("measurement_datetime").toInstant());
        return Measurement.newBuilder()
                .setAssay(assay)
                .setValue(value)
                .setTimeObserved(timeObserved)
                .build();
    }

}
