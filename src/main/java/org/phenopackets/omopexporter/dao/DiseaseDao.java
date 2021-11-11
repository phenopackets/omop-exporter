package org.phenopackets.omopexporter.dao;

import org.phenopackets.phenotools.builder.builders.OntologyClassBuilder;
import org.phenopackets.phenotools.builder.builders.TimeElements;
import org.phenopackets.schema.v2.core.OntologyClass;
import org.phenopackets.schema.v2.core.TimeElement;
import org.springframework.jdbc.core.JdbcTemplate;
import org.phenopackets.schema.v2.core.Disease;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

@Component
public class DiseaseDao {

    private final JdbcTemplate jdbcTemplate;

    public DiseaseDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Disease> getDiseases(int personId) {
        String query = "select co.person_id,\n" +
                "       concat(c.vocabulary_id, ':', c.concept_code) as term_id,\n" +
                "       c.concept_name as term_label,\n" +
                "       co.condition_source_value,\n" +
                "       FALSE as excluded,\n" +
                "       co.condition_start_date as onset_timestamp,\n" +
                "       co.condition_end_date as resolution,\n" +
                "       case when c2.concept_id is null then null\n" +
                "       else concat(c2.vocabulary_id, ':', c2.concept_code) end as clinical_tnm_finding_id,\n" +
                "       c2.concept_name as clinical_tnm_finding_label,\n" +
                "       case when c3.concept_id is null then null\n" +
                "       else concat(c3.vocabulary_id, ':', c3.concept_code) end as primary_site_id,\n" +
                "       c3.concept_name as primary_site_label\n" +
                "from condition_occurrence co\n" +
                "left join concept c on c.concept_id = co.condition_concept_id\n" +
                "left join concept_relationship cr on cr.concept_id_1 = co.condition_concept_id and cr.relationship_id = 'Has asso morph' \n" +
                "left join concept c2 on c2.concept_id = cr.concept_id_2 \n" +
                "left join concept_relationship cr2 on cr2.concept_id_1 = co.condition_concept_id and cr.relationship_id = 'Has finding site' \n" +
                "left join concept c3 on c3.concept_id = cr2.concept_id_2\n" +
                "where co.person_id = ?";
        return jdbcTemplate.query(query, this::mapRowToDisease, personId);
    }

    private Disease mapRowToDisease(ResultSet resultSet, int i) throws SQLException {
        Disease.Builder diseaseBuilder = Disease.newBuilder();
        OntologyClass term = OntologyClassBuilder.ontologyClass(resultSet.getString("term_id"), resultSet.getString("term_label"));
        diseaseBuilder.setTerm(term);

        TimeElement onset = TimeElements.timestamp(resultSet.getTimestamp("onset_timestamp").toInstant());
        diseaseBuilder.setOnset(onset);

        Timestamp resolutionTimestamp = resultSet.getTimestamp("resolution");
        if (resolutionTimestamp != null) {
            diseaseBuilder.setResolution(TimeElements.timestamp(resolutionTimestamp.toInstant()));
        }


        return diseaseBuilder.build();
    }
}
