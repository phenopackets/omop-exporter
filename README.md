# OMOP Phenopacket Exporter

This is a Spring Boot app which runs a webserver on top of an OMOP database and will export data in Phenopacket format for an individual. 

This tool was developed as part of the [Biohackathon Europe 2021](https://biohackathon-europe.org/index.html) by members of by [Project 36](https://github.com/elixir-europe/bioHackathon-projects-2021/tree/main/projects/36).

## People
The following people are acknowledged for their input:

  - Núria Queralt Rosinach
  - Anastasios Siapos
  - Danielle Welter
  - Jules Jacobsen

## Build

```shell
./mvnw clean package
```
## Configuration

Set the correct connection settings for your Synthia/OMOP database in the `application.properties` file.


## Launching
Ensure your new `aplication.properties` file is in the `target/` directory. Then do: 
```shell
java -jar target/omop-exporter-0.0.1-SNAPSHOT.jar
```

## Querying
The server will be running on `localhost:8080` data can be retrieved by OMOP person_id e.g. `http://localhost:8080/phenopacket/{patient_id}` (substituting `{patient_id}` with the patient_id integer value)
