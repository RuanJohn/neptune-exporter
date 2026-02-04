import neptune

project = neptune.init_project(
    "ruan-marl-masters/centralised-marl-msc", mode="read-only"
)
# Try fetching just 10 runs
runs = project.fetch_runs_table(
    query='`sys/creation_time`:datetime >= "2025-12-26T00:00:00Z" AND `sys/creation_time`:datetime < "2025-12-27T00:00:00Z"',
    columns=["sys/id"],
    limit=10,
)
print(runs.to_pandas())
