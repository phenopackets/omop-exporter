package org.phenopackets.omopexporter;

import org.phenopackets.omopexporter.dao.DiseaseDao;
import org.phenopackets.omopexporter.dao.IndividualDao;
import org.phenopackets.omopexporter.dao.MeasurementDao;
import org.phenopackets.omopexporter.dao.MedicalActionDao;
import org.phenopackets.phenotools.builder.builders.MetaDataBuilder;
import org.phenopackets.schema.v2.Phenopacket;
import org.phenopackets.schema.v2.core.Disease;
import org.phenopackets.schema.v2.core.Individual;
import org.phenopackets.schema.v2.core.Measurement;
import org.phenopackets.schema.v2.core.MedicalAction;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

@RestController
public class OmopExporterController {

    private final ExecutorService executorService = Executors.newWorkStealingPool();

    private final IndividualDao individualDao;
    private final MeasurementDao measurementDao;
    private final MedicalActionDao medicalActionDao;
    private final DiseaseDao diseaseDao;

    public OmopExporterController(IndividualDao individualDao, MeasurementDao measurementDao, MedicalActionDao medicalActionDao, DiseaseDao diseaseDao) {
        this.individualDao = individualDao;
        this.measurementDao = measurementDao;
        this.medicalActionDao = medicalActionDao;
        this.diseaseDao = diseaseDao;
    }

    @CrossOrigin
    @GetMapping(value = "phenopacket/{personId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Phenopacket getPhenopacketForPerson(@PathVariable int personId) {
        Optional<Individual> result = individualDao.getIndividual(personId);
        if (result.isEmpty()) {
            return null;
        }
        Phenopacket.Builder builder = Phenopacket.newBuilder();
        Individual individual = result.get();
        // TODO: provide a project CURIE prefix for this to be more meaningful - using N3C as a placeholder / example
        builder.setId("N3C:" + individual.getId());
        builder.setSubject(individual);

        var metaData = MetaDataBuilder.create(Instant.now().toString(), "OMOPackager").build();
        builder.setMetaData(metaData);

//        Future<List<Measurement>> futureMeasurements = executorService.submit(() -> measurementDao.getMeasurements(personId));

        List<Measurement> measurements = measurementDao.getMeasurements(personId);
        if (!measurements.isEmpty()) {
            builder.addAllMeasurements(measurements);
        }

        List<MedicalAction> medicalActions = medicalActionDao.getMedicalActions(personId);
        if (!medicalActions.isEmpty()) {
            builder.addAllMedicalActions(medicalActions);
        }

        List<Disease> diseases = diseaseDao.getDiseases(personId);
        if (!diseases.isEmpty()) {
            builder.addAllDiseases(diseases);
        }

        return builder.build();
    }
}
