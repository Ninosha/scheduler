bucket = storage_client.get_bucket("crudtask")
blobs = bucket.list_blobs()
for b in blobs:
    metadata = {"Blob_Name": b.name,
                "Size": b.size,
                "Updated": b.updated, b.md_5hash}
    print(metadata)