import base64
import logging
import os
from datetime import datetime
from io import StringIO
from typing import Any, Dict, List
from dataclasses import asdict

import google.cloud.logging
from google.cloud import storage, firestore
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
    names = [f"isi-files/{name}" for name in document_data["fileReferences"].values()]
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
    logging.basicConfig("%(asctime)s %(tree_id)s %(message)s")
    logger = logging.getLogger("tree-collection-create-logger")

    client = firestore.Client()

    tree_id = context.resource.split("/").pop()
    logger_extra = {"treeId": tree_id}
    logger.info("Handling new created tree", extra=logger_extra)

    document_reference = client.collection("trees").document(tree_id)

    document_data = event["value"]["fields"]

    document_data.update({"startedDate": get_int_utcnow()})
    document_reference.set(document_data)

    try:
        logger.info("Tree process started", extra=logger_extra)

        contents = get_contents(document_data)
        tos = tree_from_strings(list(contents.values()))
        result = convert_tos_to_json(tos)
        document_data.update(
            {"result": result, "errorMessage": None, "fileNames": list(contents.keys())}
        )

        logger.info("Tree process finished", extra=logger_extra)
    except Exception as error:
        logger.exception("Tree process failed", extra=logger_extra)
        document_data.update(
            {"result": None, "errorMessage": str(error), "fileNames": None}
        )

    document_data.update(
        {
            "version": "2",
            "finishedDate": get_int_utcnow(),
        }
    )
    document_reference.set(document_data)
