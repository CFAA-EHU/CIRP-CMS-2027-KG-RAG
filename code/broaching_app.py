"""
Chainlit interface for the QA system about the broaching machine.

Execute:
       chainlit run broaching_app.py --host 0.0.0.0 --port 8000
"""
import os
from dotenv import load_dotenv

import chainlit as cl

from langchain_ollama import OllamaLLM
from langchain_community.graphs import OntotextGraphDBGraph
from langchain_community.chains.graph_qa.ontotext_graphdb import OntotextGraphDBQAChain

# Load environment variables from .env file
load_dotenv()

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
GRAPHDB_SPARQL_ENDPOINT = os.getenv("GRAPHDB_SPARQL_ENDPOINT")
GRAPHDB_NAMED_GRAPH = os.getenv("GRAPHDB_NAMED_GRAPH")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL")

ONTOLOGY_QUERY = f"""
PREFIX aas: <https://admin-shell.io/aas/3/1/>
PREFIX aas-ref: <https://admin-shell.io/aas/3/1/Referable/>
PREFIX aas-prop: <https://admin-shell.io/aas/3/1/Property/>
PREFIX aas-smc: <https://admin-shell.io/aas/3/1/SubmodelElementCollection/>
PREFIX aas-sm: <https://admin-shell.io/aas/3/1/Submodel/>
CONSTRUCT {{
    ?s a ?type .
    ?s aas-ref:idShort ?idShort .
    ?s aas-prop:value ?value .
    ?s aas-sm:submodelElements ?elem .
    ?s aas-smc:value ?child .
}}
WHERE {{
    GRAPH <{GRAPHDB_NAMED_GRAPH}> {{
        ?s a ?type .
        OPTIONAL {{ ?s aas-ref:idShort ?idShort }}
        OPTIONAL {{ ?s aas-prop:value ?value }}
        OPTIONAL {{ ?s aas-sm:submodelElements ?elem }}
        OPTIONAL {{ ?s aas-smc:value ?child }}
    }}
}}
"""

# ─────────────────────────────────────────
# INITIALIZATION: runs once when the server starts
# ─────────────────────────────────────────
print("Conectando con GraphDB...")
_graph = OntotextGraphDBGraph(
    query_endpoint=GRAPHDB_SPARQL_ENDPOINT,
    query_ontology=ONTOLOGY_QUERY,
)
print("Cargando modelo Ollama...")
_llm = OllamaLLM(model=OLLAMA_MODEL, temperature=0)
_chain = OntotextGraphDBQAChain.from_llm(
    llm=_llm,
    graph=_graph,
    verbose=True,
    max_fix_retries=3,
    allow_dangerous_requests=True,
)
print("Sistema listo.")

# ─────────────────────────────────────────
# CHAT START: save the chain in the user session without sending messages
# This way, Chainlit shows the welcome screen with the logo
# ─────────────────────────────────────────
@cl.on_chat_start
async def on_chat_start():
    cl.user_session.set("chain", _chain)


# ─────────────────────────────────────────
# MESSAGES HANDLING: receive question and respond
# ─────────────────────────────────────────
@cl.on_message
async def on_message(message: cl.Message):
    chain = cl.user_session.get("chain")

    if chain is None:
        await cl.Message(
            content="⚠️ The chain is not initialized. Please reload the page to retry."
        ).send()
        return

    thinking_msg = cl.Message(content="🔍 Generating SPARQL query and looking for the answer...")
    await thinking_msg.send()

    try:
        # Invoke chain with the user's question
        result = await cl.make_async(chain.invoke)({"query": message.content})

        sparql_query = result.get("sparql_query", "")
        answer = result.get("result", "No answer found.")

        # Build the answer with the SPARQL query as a collapsible detail
        response_parts = [answer]
        if sparql_query:
            response_parts.append(f"\n\n<details>\n<summary>🔎 Generated SPARQL query</summary>\n\n```sparql\n{sparql_query}\n```\n</details>")

        thinking_msg.content = "".join(response_parts)
        await thinking_msg.update()

    except Exception as e:
        thinking_msg.content = f"❌ Error while processing the question: {e}"
        await thinking_msg.update()
