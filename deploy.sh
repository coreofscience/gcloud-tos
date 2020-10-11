gcloud functions deploy create_tree \
  --project coreofscience-dev \
  --trigger-event providers/google.firebase.database/eventTypes/ref.create \
  --trigger-resource projects/_/instances/coreofscience-dev/refs/trees/{treeId} \
  --source ./create-tree \
  --runtime python38
