import base64
import logging
import os
from datetime import datetime
from io import StringIO
from typing import Any, Dict, List

import google.cloud.logging
from firebase_admin import db, firestore, initialize_app
from google.cloud import storage
from igraph import Graph
from sap import CachedCollection, Sap, giant

storage_client = storage.Client()
logging_client = google.cloud.logging.Client()
logging_client.get_default_handler()
logging_client.setup_logging()

BUCKET_URL = os.getenv("STORAGEBUCKET")
DATABASE_URL = os.getenv("DATABASEURL")
BUCKET = storage_client.get_bucket(BUCKET_URL)

initialize_app(
    options={
        "databaseURL": DATABASE_URL,
    }
)


def tree_from_strings(strings: List[str]) -> Graph:
    """Creates a ToS tree from a list of strings."""
    sap = Sap()
    graph = giant(CachedCollection(*[StringIO(text) for text in strings]))
    return sap.tree(graph)


def convert_tos_to_json(tree: Graph) -> Dict[str, List[Dict]]:
    """Converts a ToS graph in the default format to be processed by the
    frontend. The default format is like:
    {
        "root": [{"label": "...", "title": "...", "authors": "...", ...}, ...],
        "trunk": [{"label": "...", "title": "...", "authors": "...", ...}, ...],
        "leaf": [{"label": "...", "title": "...", "authors": "...", ...}, ...],
    }
    """
    output = {}
    sections = ["root", "trunk", "leaf"]
    for section in sections:
        vertices = tree.vs.select(**{f"{section}_gt": 0})
        data = sorted(
            [vertex.attributes() for vertex in vertices],
            key=lambda article: article.get(section, 0),
            reverse=True,
        )
        output[section] = data

    return output


def get_contents(delta: Dict[str, Any]) -> List[str]:
    """Get the contents for the files in order to create the graph."""
    names = [f"isi-files/{name}" for name in delta["files"].values()]
    logging.info(f"Reading source files {names}")
    blobs = [BUCKET.get_blob(name) for name in names]
    return [blob.download_as_text() for blob in blobs if blob is not None]


def store_tree_result(tree_id: str, result: Dict[str, List[Dict]]) -> str:
    """Stores a json in the storage service with the result for the ToS built."""
    result_name = f"results/{base64.b64encode(tree_id.encode()).decode()}"
    client = firestore.client()
    client.document(result_name).set(result)
    return result_name


def create_tree(event, context):
    delta = event.get("delta", {"files": {}})
    tree_id = "/".join(context.resource.split("/")[-2:])
    logging.info(f"Creating tree for {tree_id}")
    delta.update({"startedDate": int(datetime.utcnow().timestamp())})
    db.reference(tree_id).set(delta)

    try:
        contents = get_contents(delta)
        tos = tree_from_strings(contents)
        result = convert_tos_to_json(tos)
        result_name = store_tree_result(tree_id, result)
        logging.info(f"Successfuly stored tree at {result_name}")
        delta.update({"result": result_name, "error": None})
    except Exception as error:
        logging.exception(f"There was an error processing {tree_id}")
        delta.update(
            {
                "result": None,
                "error": str(error),
            }
        )

    delta.update(
        {
            "version": "1",
            "finishedDate": int(datetime.utcnow().timestamp()),
        }
    )
    db.reference(tree_id).set(delta)
