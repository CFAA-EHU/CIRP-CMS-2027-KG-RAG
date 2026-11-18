# CIRP-CMS-2027-KG-RAG

This repository contains a question-answering system that integrates a Knowledge Graph representing an industrial broaching machine with time-series data collected from its sensors.

The application allows users to query information about machine components, properties, and relationships, as well as retrieve and analyse recorded measurements through a conversational interface.

The application uses:
- [GraphDB](https://graphdb.ontotext.com) as a triple store for the ontology and Knowledge Graph.
- [InfluxDB](https://www.influxdata.com) for storing and querying time-series data.
- [LangChain](https://www.langchain.com) for question processing and integration of the different data sources.
- [Ollama](https://ollama.com) for local large language model inference.
- [Chainlit](https://docs.chainlit.io/get-started/overview) for the conversational interface.

## Repository structure

The repository contains the main application, its Python dependencies, an environment configuration template, and the ontology and Knowledge Graph files.

```text
.
├── code/
│   ├── ontology_influx_chainlit.py  # Main Chainlit application
│   ├── requirements.txt            # Python dependencies
│   └── .env.example                # Configuration template
├── RAMON-Retrieval-Advanced-Manufacturing-Ontology/ # Submodule containing the ontology and knowledge graph
├── evaluation/
│   └── evaluation_questions.csv # CSV file containing the list of questions used to evaluate the system
├── LICENSE
└── README.md
```

The ontology and Knowledge Graph are provided in the RAMON submodule and must be imported into GraphDB before running the application.

## Hardware requirements

The application currently uses the model `qwen2.5:32b`.

The model occupies approximately 20 GB, and additional RAM/VRAM is required for inference.

A system with sufficient GPU VRAM and/or system RAM is therefore required.

## Deployment

1. Clone the repository, including its submodules:

   ```bash
   git clone --recurse-submodules <repository-url>
   cd CIRP-CMS-2027-KG-RAG
   ```

2. Create and activate a Python virtual environment:

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. Install the required dependencies:

   ```bash
   pip install -r code/requirements.txt
   ```

4. Set up GraphDB and configure it:
   - Start a GraphDB instance (default port `7200`).
   - Create a repository named `broaching`.
   - Import the ontology and Knowledge Graph files provided in this repository.
   - Ensure that the imported graph contains the machine structure, properties, and mappings to InfluxDB measurements.

5. Set up InfluxDB:
   - Start an InfluxDB 2.x instance with Flux query support (default port `8086`).
   - Configure an organisation and create a bucket containing the machine measurements.
   - Import the corresponding time-series data into the bucket.
   - Generate an API token with read access to the bucket.

   The bucket, measurement, and field names must match the time-series mappings defined in the Knowledge Graph.

6. Set up Ollama and download the required model. The current configuration uses:

   ```bash
   ollama pull qwen2.5:32b
   ```

   Ensure that the Ollama server is running (default port `11434`).

7. Create a local configuration file from the provided template:

   ```bash
   cp code/.env.example code/.env
   ```

8. Edit `code/.env` according to your local configuration. The main settings are:

   | Variable | Description |
   |---|---|
   | `GRAPHDB_ENDPOINT` | SPARQL endpoint of the GraphDB repository |
   | `INFLUX_URL` | InfluxDB server URL |
   | `INFLUX_TOKEN` | InfluxDB API token |
   | `INFLUX_ORG` | InfluxDB organisation |
   | `OLLAMA_HOST` | Ollama server URL |
   | `OLLAMA_MODEL` | Language model used for inference |

   The ontology and Knowledge Graph namespaces must also match those used in the imported RDF files. Additional parameters can be configured through `.env.example`.

9. Run the application from the code directory:

   ```bash
   cd code
   chainlit run ontology_influx_chainlit.py --host 0.0.0.0 --port 8000
   ```

The Chainlit interface will then be available at:

http://localhost:8000

## Running the application

Once the initial configuration is complete, subsequent executions only require:

1. Ensuring that GraphDB, InfluxDB, and Ollama are running and accessible.
2. Activating the Python virtual environment.
3. Starting the Chainlit application:

   ```bash
   cd code
   source ../.venv/bin/activate
   chainlit run ontology_influx_chainlit.py --host 0.0.0.0 --port 8000
   ```

The application automatically connects to the configured services when processing user questions.

The system uses the Knowledge Graph to retrieve structural information and identify the corresponding time-series measurements in InfluxDB. Depending on the question, it queries GraphDB, InfluxDB, or both data sources to generate an answer.