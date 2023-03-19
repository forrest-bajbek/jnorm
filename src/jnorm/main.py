import argparse
import decimal
import io
import json
import logging
import pathlib
import sys
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, List

import ijson
from rich.console import Console
from rich.table import Table

logger = logging.getLogger(__name__)


def setup_logging(loglevel):
    """Setup basic logging

    Args:
      loglevel (int): minimum loglevel for emitting messages
    """
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
    logging.basicConfig(
        level=loglevel,
        stream=sys.stdout,
        format=logformat,
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args(args):
    parser = argparse.ArgumentParser(
        description="Normalize JSON into relational tables."
    )

    def file_type(path: str) -> pathlib.Path:
        p = pathlib.Path(path)
        if not p.exists():
            raise ValueError(f"Source path '{path}' does not exist!")
        elif not p.is_file():
            raise ValueError(f"Source path '{path}' must be a file!")
        return p

    def folder_type(path: str) -> pathlib.Path:
        p = pathlib.Path(path)
        if not p.exists():
            p.mkdir(parents=True)
        elif not p.is_dir():
            raise ValueError(f"Target path '{path}' must be a directory!")
        return p

    parser.add_argument(
        "--source",
        type=file_type,
        default="example/people.json",
        help="Path to file containing source JSON",
    )
    parser.add_argument(
        "--target",
        type=folder_type,
        default="example/output",
        help="Path to folder in which to save output files",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const=logging.INFO,
    )
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG,
    )

    return parser.parse_args(args)


@dataclass
class Entity:
    hierarchy: List[str]
    target_folder: pathlib.Path

    @property
    def name(self) -> str:
        return "_".join(self.hierarchy)

    @property
    def id_col(self) -> str:
        return self.name + "_id"

    @property
    def target_file(self) -> pathlib.Path:
        return self.target_folder.joinpath(self.name + ".jsonl")

    @property
    def has_parent(self) -> bool:
        return len(self.hierarchy) > 1

    @property
    def parent_name(self) -> str:
        return "_".join(self.hierarchy[:-1])

    @property
    def parent_id_col(self) -> str:
        return self.parent_name + "_id"


class Writer:
    def __init__(self):
        self.router: Dict[str, io.TextIOWrapper] = dict()

    def initialize_writer(self, entity: Entity):
        entity.target_file.unlink(missing_ok=True)
        self.router[entity.name] = {
            "writer": open(entity.target_file, "a"),
            "last_id": 0,
        }

    def get_last_id(self, entity: Entity) -> int:
        if self.router.get(entity.name):
            return self.router[entity.name]["last_id"]
        return 0

    def write(self, entity: Entity, record: OrderedDict) -> int:
        if entity.name not in self.router:
            self.initialize_writer(entity)

        record_serialized = json.dumps(record, default=str)
        self.router[entity.name]["writer"].write(record_serialized)
        self.router[entity.name]["writer"].write("\n")
        self.router[entity.name]["last_id"] = record[entity.id_col]

    def summary(self):
        total = 0
        table = Table()
        table.add_column("target_file")
        table.add_column("records")
        for entity, route in self.router.items():
            record_count = route["last_id"]
            total += record_count
            table.add_row(
                entity,
                f"{record_count:,}",
            )
        table.add_row("total", f"{total:,}")
        console = Console()
        console.print(table)


def parse_array(
    parser,
    entity: Entity,
    writer: Writer,
    parent_id: int = None,
):
    for prefix, event, value in parser:
        if event in ["string", "number", "boolean"]:
            record = OrderedDict()
            record[entity.id_col] = writer.get_last_id(entity) + 1
            if parent_id is not None:
                record[entity.parent_id_col] = parent_id
            record["value"] = value
            writer.write(entity=entity, record=record)
        elif event == "start_array":
            parser = parse_array(
                parser=parser,
                entity=entity,
                writer=writer,
                parent_id=parent_id,
            )
        elif event == "start_map":
            parser = parse_map(
                parser=parser,
                entity=entity,
                writer=writer,
                parent_id=parent_id,
            )
        elif event == "end_array":
            return parser


def parse_map(
    parser,
    entity: Entity,
    writer: Writer,
    parent_id: int = None,
):
    map_key = None
    id = writer.get_last_id(entity) + 1
    record = OrderedDict()
    record[entity.id_col] = id
    if parent_id is not None:
        record[entity.parent_id_col] = parent_id
    for prefix, event, value in parser:
        if event == "map_key":
            map_key = value
        elif event == "start_map":
            entity.hierarchy.append(map_key)
            parser = parse_map(
                parser=parser,
                entity=entity,
                writer=writer,
                parent_id=id,
            )
            entity.hierarchy.pop()
        elif event in ["string", "number", "boolean"]:
            record[map_key] = (
                float(value) if isinstance(value, decimal.Decimal) else value
            )
        elif event == "start_array":
            entity.hierarchy.append(map_key)
            parser = parse_array(
                parser=parser,
                entity=entity,
                writer=writer,
                parent_id=id,
            )
            entity.hierarchy.pop()
        elif event == "end_map":
            writer.write(entity=entity, record=record)
            return parser


def main(args):
    args = parse_args(args)
    setup_logging(args.loglevel)
    logger.debug("Starting jnorm...")

    entity = Entity(hierarchy=[args.source.stem], target_folder=args.target)
    writer = Writer()
    parser = ijson.parse(open(args.source))
    for prefix, event, value in parser:
        if event == "start_map":
            parse_map(
                parser=parser,
                entity=entity,
                writer=writer,
            )
        elif event == "start_array":
            parse_array(
                parser=parser,
                entity=entity,
                writer=writer,
            )
    writer.summary()


def run():
    main(sys.argv[1:])


if __name__ == "__main__":
    run()
