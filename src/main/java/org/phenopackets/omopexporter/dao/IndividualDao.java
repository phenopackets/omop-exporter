package org.phenopackets.omopexporter.dao;

import com.google.protobuf.Timestamp;
import org.phenopackets.phenotools.builder.builders.OntologyClassBuilder;
import org.phenopackets.phenotools.builder.builders.TimeElements;
import org.phenopackets.schema.v2.core.Individual;
import org.phenopackets.schema.v2.core.Sex;
import org.phenopackets.schema.v2.core.VitalStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;

@Component
public class IndividualDao {

    private final JdbcTemplate jdbcTemplate;

    public IndividualDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<Individual> getIndividual(int personId) {

        String query = "SELECT p.person_id::text as id,\n" +
                "        null as alternate_ids,\n" +
                "       p.birth_datetime as date_of_birth,\n" +
                "       max(vo.visit_start_date) as time_at_last_encounter,\n" +
                "       (case when d.person_id is null then 0 else 2 end) as vital_status,\n" +
                "       (case when p.gender_concept_id is null then 0\n" +
                "             when p.gender_concept_id = 8532 then 1\n" +
                "             when p.gender_concept_id = 8507 then 2\n" +
                "             else 3 end) as sex,\n" +
                "       null as karyotypic_sex,\n" +
                "       null as gender,\n" +
                "    'NCBITaxon:9606' as taxonomy_id,\n" +
                "    'human' as taxonomy_label\n" +
                "FROM person p\n" +
                "    LEFT JOIN visit_occurrence vo on vo.person_id = p.person_id\n" +
                "    LEFT JOIN death d on d.person_id = p.person_id\n" +
                "WHERE p.person_id = ?" +
                "GROUP BY p.person_id, p.birth_datetime, vital_status, sex";

        var individual = jdbcTemplate.queryForObject(query, this::mapRowToIndividual, personId);
        return individual == null ? Optional.empty() : Optional.of(individual);
    }

    private Individual mapRowToIndividual(ResultSet rs, int rowNum) throws SQLException {
        Individual.Builder builder = Individual.newBuilder();

        builder.setId(rs.getString("id"));
        Instant dateOfBirth = rs.getTimestamp("date_of_birth").toInstant();
        builder.setDateOfBirth(Timestamp.newBuilder().setSeconds(dateOfBirth.getEpochSecond()).setNanos(dateOfBirth.getNano()).build());
        builder.setSex(Sex.forNumber(rs.getInt("sex")));

        if (rs.getInt("vital_status") > 0) {
            VitalStatus vitalStatus = getVitalStatus(rs.getInt("id"));
            builder.setVitalStatus(vitalStatus);
        }

        return builder.build();
    }

    private VitalStatus getVitalStatus(int personId) {
            String query = "SELECT d.person_id,\n" +
                    "       (case when d.person_id is null then 0 else 2 end) as vital_status,\n" +
                    "       d.death_datetime as time_of_death,\n" +
                    "       (case when c.concept_code is null then null else concat(c.vocabulary_id,':',c.concept_code) end) as cause_of_death_id,\n" +
                    "       c.concept_name as cause_of_death_label\n" +
                    "FROM person p\n" +
                    "         LEFT JOIN death d on d.person_id = p.person_id\n" +
                    "         LEFT JOIN condition_occurrence co on co.person_id = p.person_id and co.condition_concept_id = d.cause_concept_id\n" +
                    "         LEFT JOIN concept c on c.concept_id = d.cause_concept_id\n" +
                    "WHERE d.person_id = ?" +
                    "GROUP BY d.person_id, vital_status, time_of_death, cause_of_death_id, cause_of_death_label\n"
                    ;
        return jdbcTemplate.queryForObject(query, this::mapRowToVitalStatus, personId);
    }

    private VitalStatus mapRowToVitalStatus(ResultSet rs, int i) throws SQLException {
        return VitalStatus.newBuilder()
                .setStatus(VitalStatus.Status.forNumber(rs.getInt("vital_status")))
                .setTimeOfDeath(TimeElements.timestamp(rs.getTimestamp("time_of_death").toInstant()))
                .setCauseOfDeath(OntologyClassBuilder.ontologyClass(rs.getString("cause_of_death_id"), rs.getString("cause_of_death_label")))
                .build();
    }
}
