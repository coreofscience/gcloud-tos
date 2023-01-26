import logging
import os
from datetime import datetime
from io import StringIO
from typing import Any, Dict, List

import google.cloud.logging
from firebase_admin import firestore, initialize_app
from google.cloud import storage
from igraph import Graph
from sap import Sap, giant
from wostools import Collection


storage_client = storage.Client()
logging_client = google.cloud.logging.Client()
logging_client.get_default_handler()
logging_client.setup_logging()

BUCKET_URL = os.getenv("STORAGEBUCKET")
DATABASE_URL = os.getenv("DATABASEURL")
BUCKET = storage_client.get_bucket(BUCKET_URL)

MAX_SIZE = 10  # MB

initialize_app(
    options={
        "databaseURL": DATABASE_URL,
    }
)


def tree_from_strings(strings: List[str]) -> Graph:
    """Creates a ToS tree from a list of strings."""
    sap = Sap()
    graph = giant(Collection(*[StringIO(text) for text in strings]))
    return sap.tree(graph)


def convert_tos_to_json(tree: Graph) -> Dict[str, List[Dict]]:
    """
    Converts a ToS graph in the default format to be processed by the frontend.
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


def get_contents(document_data: Dict[str, Any]) -> Dict[str, str]:
    """Get the contents for the files in order to create the graph."""
    names = [
        f'isi-files/{name["stringValue"]}'
        for name in document_data["fileReferences"]["arrayValue"]["values"]
    ]
    logging.info(f"Reading source files {names}")
    blobs = [BUCKET.get_blob(name) for name in names]

    size = 0
    output = {}
    for blob in blobs:
        if blob is not None:
            size += blob.size
            if (size / 1e6) > MAX_SIZE:
                break
            output[blob.name] = blob.download_as_text()
    return output


def get_int_utcnow() -> int:
    return int(datetime.utcnow().timestamp())


def create_tree_v2(event, context):
    client = firestore.client()

    tree_id = context.resource.split("/").pop()
    logging.info("Handling new created tree ({tree_id})")

    document_reference = client.collection("trees").document(tree_id)

    document_reference.update({"startedDate": get_int_utcnow()})

    try:
        logging.info("Tree process started ({tree_id})")

        contents = get_contents(event["value"]["fields"])
        tos = tree_from_strings(list(contents.values()))
        result = convert_tos_to_json(tos)
        document_reference.update(
            {
                "version": "2",
                "result": result,
                "errorMessage": None,
                "fileNames": list(contents.keys()),
                "finishedDate": get_int_utcnow(),
            }
        )
        logging.info("Tree process finished ({tree_id})")
    except Exception as error:
        logging.exception("Tree process failed ({tree_id})")
        document_reference.update(
            {
                "version": "2",
                "result": None,
                "errorMessage": str(error),
                "fileNames": None,
                "finishedDate": get_int_utcnow(),
            }
        )
