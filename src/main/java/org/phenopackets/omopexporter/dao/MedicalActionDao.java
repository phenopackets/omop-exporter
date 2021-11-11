package org.phenopackets.omopexporter.dao;

import org.phenopackets.phenotools.builder.builders.*;
import org.phenopackets.schema.v2.core.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Component
public class MedicalActionDao {

    private static final OntologyClass NO_CONCEPT = OntologyClassBuilder.ontologyClass("None:No matching concept", "No matching concept");

    private final JdbcTemplate jdbcTemplate;

    public MedicalActionDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<MedicalAction> getMedicalActions(int personId) {
        List<MedicalAction> medicalActions = new ArrayList<>();
        List<Treatment> treatments = getTreatments(personId);
        for (Treatment treatment : treatments) {
            // TODO: group all treatments with same agent and combine all the dose intervals?
            medicalActions.add(MedicalActionBuilder.treatment(treatment));
        }

        List<Procedure> procedures = getProcedures(personId);
        for (Procedure procedure : procedures) {
            medicalActions.add(MedicalActionBuilder.procedure(procedure));
        }

        return List.copyOf(medicalActions);
    }

    public List<Treatment> getTreatments(int personId) {
        String query = "SELECT de.person_id,\n" +
                "       concat(c.vocabulary_id, ':', c.concept_code) as agent_id,\n" +
                "       c.concept_name as agent_label,\n" +
                "       concat(c2.vocabulary_id, ':', c2.concept_code) as route_of_administration_id,\n" +
                "       c2.concept_name as route_of_administration_label,\n" +
                "       concat(c3.vocabulary_id, ':', c3.concept_code) as quantity_unit_id,\n" +
                "       c3.concept_name as quantity_unit_label,\n" +
                "       ds.amount_value as quantity_value,\n" +
                "       de.drug_exposure_start_date as interval_start,\n" +
                "       de.drug_exposure_start_date + de.days_supply as interval_end\n" +
                "FROM drug_exposure de \n" +
                "LEFT JOIN drug_strength ds on ds.drug_concept_id = de.drug_concept_id\n" +
                "LEFT JOIN concept c on c.concept_id = de.drug_concept_id\n" +
                "LEFT JOIN concept c2 on c2.concept_id = de.route_concept_id\n" +
                "LEFT JOIN concept c3 on c3.concept_id = ds.amount_unit_concept_id\n" +
                "LEFT JOIN concept c4 on c4.concept_id = de.drug_type_concept_id\n" +
                "WHERE de.person_id = ?";
        return jdbcTemplate.query(query, this::mapRowToTreatment, personId);
    }

    private Treatment mapRowToTreatment(ResultSet resultSet, int i) throws SQLException {
        Treatment.Builder builder = Treatment.newBuilder();
        builder.setAgent(OntologyClassBuilder.ontologyClass(resultSet.getString("agent_id"), resultSet.getString("agent_label")));
        builder.setRouteOfAdministration(OntologyClassBuilder.ontologyClass(resultSet.getString("route_of_administration_id"), resultSet.getString("route_of_administration_label")));
        String quantityUnitId = resultSet.getString("quantity_unit_id");
        String quantityUnitLabel = resultSet.getString("quantity_unit_label");
        OntologyClass unit = OntologyClassBuilder.ontologyClass(quantityUnitId == null ? "" : quantityUnitId, quantityUnitLabel == null ? "" : quantityUnitLabel);
        Quantity quantity = QuantityBuilder.quantity(unit, resultSet.getDouble("quantity_value"));
        Instant intervalStart = resultSet.getTimestamp("interval_start").toInstant();
        Instant intervalEnd = resultSet.getTimestamp("interval_end").toInstant();
        TimeInterval timeInterval = TimeIntervalBuilder.timeInterval(TimestampBuilder.fromInstant(intervalStart), TimestampBuilder.fromInstant(intervalEnd));
        builder.addDoseIntervals(DoseIntervalBuilder.doseInterval(quantity, NO_CONCEPT, timeInterval));
        return builder.build();
    }

    public List<Procedure> getProcedures(int personId) {
        String query = "select po.person_id as id,\n" +
                "       concat(c.vocabulary_id, ':', c.concept_code) as procedure_code_id,\n" +
                "       c.concept_name as procedure_code_label,\n" +
                "       case when c2.concept_id is null then null\n" +
                "       else concat(c2.vocabulary_id, ':', c2.concept_code) end as body_site_id,\n" +
                "       c2.concept_name as body_site_label,\n" +
                "       date_part('year',po.procedure_datetime) - date_part('year',p.birth_datetime) as performed_age,\n" +
                "       po.procedure_datetime as performed_datetime\n" +
                "from procedure_occurrence po\n" +
                "left join concept c on c.concept_id = po.procedure_concept_id\n" +
                "left join person p on p.person_id = po.person_id\n" +
                "left join concept_relationship cr on cr.concept_id_1 = po.procedure_concept_id and cr.relationship_id = 'Has proc site'\n" +
                "left join concept c2 on c2.concept_id = cr.concept_id_2\n" +
                "where po.person_id = ?";
        return jdbcTemplate.query(query, this::mapRowToProcedure, personId);
    }

    private Procedure mapRowToProcedure(ResultSet resultSet, int i) throws SQLException {
        var codeId = resultSet.getString("procedure_code_id");
        var codeLabel = resultSet.getString("procedure_code_label");
        OntologyClass code = OntologyClassBuilder.ontologyClass(codeId, codeLabel);

        var bodySiteId = resultSet.getString("body_site_id");
        var bodySiteLabel = resultSet.getString("body_site_label");
        OntologyClass bodySite = null;
        if (bodySiteId != null && bodySiteLabel != null) {
            bodySite = OntologyClassBuilder.ontologyClass(bodySiteId, bodySiteLabel);
        }

        TimeElement timePerformed = TimeElements.timestamp(resultSet.getTimestamp("performed_datetime").toInstant());

        Procedure.Builder procedureBuilder = Procedure.newBuilder()
                .setCode(code)
                .setPerformed(timePerformed);

        if (bodySite != null) {
            procedureBuilder.setBodySite(bodySite);
        }

        return procedureBuilder.build();
    }
}
