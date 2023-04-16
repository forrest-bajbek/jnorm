# jnorm
Jnorm is a Python library that normalizes deeply nested JSON files into a bunch of flat files in the [JSON Lines](https://jsonlines.org/) format, generating primary keys based on the position of items. These normalized files can be easily loaded into a relational database and queried with SQL. While the queries are complex, they are at least possible.

## Installation
- via pip: `pip install jnorm`
- via Docker: `docker pull forrest-bajbek/jnorm`

## How it works
Consider the following JSON:
```json
[
    {
        "name": "Bob",
        "gender": "male",
        "favorite_foods": [
            "tacos",
            "burritos"
        ],
        "address": {
            "address_1": "1234 Main Road",
            "address_2": "Apt A",
            "city": "Fort Lauderdale",
            "state": "FL",
            "zipcode": 12345
        }
    },
    {
        "name": "Alice",
        "gender": "female",
        "favorite_foods": [
            "sushi",
            "takoyami"
        ],
        "address": {
            "address_1": "5678 Secondary Road",
            "address_2": "Apt B",
            "city": "San Francisco",
            "state": "CA",
            "zipcode": 67890
        }
    }
]
```

Running `jnorm example/people.json` yields the following `jsonl` files:
- `example/output/people.jsonl`:
    ```json
    {"people_id": 1, "name": "Bob", "gender": "male"}
    {"people_id": 2, "name": "Alice", "gender": "female"}
    ```
- `example/output/people_address.jsonl`:
    ```json
    {"people_address_id": 1, "people_id": 1, "address_1": "1234 Main Road", "address_2": "Apt A", "city": "Fort Lauderdale", "state": "FL", "zipcode": 12345}
    {"people_address_id": 2, "people_id": 2, "address_1": "5678 Secondary Road", "address_2": "Apt B", "city": "San Francisco", "state": "CA", "zipcode": 67890}
    ```
- `example/output/people_favorite_foods.jsonl`:
    ```json
    {"people_favorite_foods_id": 1, "people_id": 1, "value": "tacos"}
    {"people_favorite_foods_id": 2, "people_id": 1, "value": "burritos"}
    {"people_favorite_foods_id": 3, "people_id": 2, "value": "sushi"}
    {"people_favorite_foods_id": 4, "people_id": 2, "value": "takoyami"}
    ```

Once loaded into a database, it can be queried with SQL:
```sql
-- Show all favorite foods
SELECT
    p.id AS people_id
    , p.name
    , p.gender
    , pff.value as favorite_food
FROM people_favorite_foods pff
JOIN people p
    ON p.people_id = pff.people_id
;

-- Show all addresses
SELECT
    p.id as people_id
    , p.name
    , p.gender
    , pa.address_1
    , pa.address_2
    , pa.city
    , pa.state
    , pa.zipcode
FROM people p
JOIN people_address pa
    ON p.people_id = pa.people_id
;
```

## Running from Docker
Example:
```bash
docker run -it --rm -v /path/to/folder:/data forrest-bajbek/jnorm:1.0.0 myfile.json
```