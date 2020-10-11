import base64
import json
import os
import logging
from datetime import datetime
from io import StringIO
from typing import Dict, List

from firebase_admin import db, initialize_app
from google.cloud import storage
from igraph import Graph
from sap import CachedCollection, Sap, giant

storage_client = storage.Client()

BUCKET_URL = os.getenv("STORAGEBUCKET")
DATABASE_URL = os.getenv("DATABASEURL")

initialize_app(options={"databaseURL": DATABASE_URL})


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

    labels = ["root", "trunk", "leaf"]
    for label in labels:
        vertices = tree.vs.select(**{f"{label}_gt": 0})
        data = [vertex.attributes() for vertex in vertices]
        output[label] = data

    return output


def create_tree(event, context):
    delta = event.get("delta", {"files": {}})
    tree_id = "/".join(context.resource.split("/")[-2:])
    logging.info(f"Creating tree for {tree_id}")
    delta.update({"startedDate": int(datetime.utcnow().timestamp())})
    db.reference(tree_id).set(delta)
    bucket = storage_client.get_bucket(BUCKET_URL)
    names = [f"isi-files/{name}" for name in delta["files"].values()]
    logging.info(f"Reading source files {names}")
    blobs = [bucket.get_blob(name) for name in names]
    contents = [blob.download_as_text() for blob in blobs if blob is not None]
    tos = tree_from_strings(contents)
    result = convert_tos_to_json(tos)
    logging.info(f"Successfuly created tree for {tree_id}")
    result_name = f"results/{base64.b64encode(tree_id.encode()).decode()}.json"
    bucket.blob(result_name).upload_from_string(json.dumps(result, indent=2))
    logging.info(f"Successfuly stored tree at {result_name}")
    delta.update(
        {
            "version": "1",
            "result": result_name,
            "finishedDate": int(datetime.utcnow().timestamp()),
        }
    )
    db.reference(tree_id).set(delta)
    logging.info(f"Successfuly stored tree for {tree_id}")
