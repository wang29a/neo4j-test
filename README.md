
```sh
docker run --rm --name neo4j-dev \
    --publish=7474:7474 --publish=7688:7687 \
    --env NEO4J_AUTH=neo4j/password \
    -v /mnt/neo4j/data:/data \
    neo4j:2025.06.2
```