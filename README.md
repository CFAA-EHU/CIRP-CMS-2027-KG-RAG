# CIRP-CMS-2027-KG-RAG

This repository contains a question-answering system connected to a Knowledge Graph representing a broaching machine.

The application uses:
- [GraphDB](https://graphdb.ontotext.com) as a triple store.
- [LangChain](https://www.langchain.com) for graph-based question answering.
- [Ollama](https://ollama.com) for local large language model inference.
- [Chainlit](https://docs.chainlit.io/get-started/overview) for the conversational interface.

## Repository structure

```
.
├── code/
│   ├── broaching_app.py # Main Chainlit application
│   ├── requirements.txt # Python dependencies
│   └── .env.example # Configuration template
├── graph/
│   └── broaching_aas_knowledge_graph.ttl    # RDF knowledge graph
├── LICENSE
└── README.md
```

## Deployment

1. Clone the repository.
2. Create and activate a Python virtual environment.
```
python3 -m venv .venv
source .venv/bin/activate

```
3. Install the required dependencies:
```
pip install -r code/requirements.txt
```
4. Set up GraphDB and configure it: 
    - Create a new repository called `aas-rdf`
    - Import the RDF graph file `broaching_aas_knowledge_graph.ttl` to the Named Graph `urn:cfaa:brochadora:001:graph`.
5. Set up Ollama and download the required model. The current configuration uses:
```
ollama pull qwen2.5:32b
```
6. Create a local configuration file from the provided template:
```
cp code/.env.example code/.env
```
7. Edit code/.env according to your local GraphDB and Ollama configuration.
8. Run the application from the code directory:
```
cd code
chainlit run broaching_app.py --host 0.0.0.0 --port 8000
```

The Chainlit interface will then be available at:
```http://localhost:8000```
