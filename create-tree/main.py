import os
from google.cloud import storage

storage_client = storage.Client()


def create_tree(event, context):
    print(event, context)
    delta = event.get("delta", {"files": {}})
    print(delta)
    bucket_name = os.getenv("STORAGEBUCKET")
    bucket = storage_client.get_bucket(bucket_name)
    names = [f"isi-files/{name}" for name in delta["files"].values()]
    print(names)
    blobs = [bucket.get_blob(name) for name in names]
    print(blobs)
    contents = [blob.download_as_text() for blob in blobs if blob is not None]
    print(contents)

    for content in contents:
        print(content[:1000])
