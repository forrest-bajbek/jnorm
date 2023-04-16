import argparse
import decimal
import io
import json
import logging
import pathlib
import shutil
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
        "source",
        type=file_type,
        help="Path to file containing source JSON",
    )
    parser.add_argument(
        "--target",
        type=folder_type,
        help="Path to folder in which to save output files. default: {source}/output",
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
        logger.debug("Initializing Writer Class")
        self.router: Dict[str, io.TextIOWrapper] = dict()

    def initialize_writer(self, entity: Entity):
        logger.info(f"Starting new writer for entity: {str(entity)}")
        entity.target_file.unlink(missing_ok=True)
        self.router[entity.name] = {
            "writer": open(entity.target_file, "a"),
            "last_id": 0,
        }

    def get_last_id(self, entity: Entity) -> int:
        logger.debug(f"Retrieving Last ID for entity: {str(entity)}")
        if self.router.get(entity.name):
            last_id = self.router[entity.name]["last_id"]
        else:
            last_id = 0
        logger.debug(f"Last ID: {last_id}")
        return last_id

    def write(self, entity: Entity, record: OrderedDict) -> int:
        logger.debug(f"Writing record to entity: {str(entity)}")
        if entity.name not in self.router:
            self.initialize_writer(entity)

        record_serialized = json.dumps(record, default=str)
        logger.debug(f"Record: {record_serialized}")
        self.router[entity.name]["writer"].write(record_serialized)
        self.router[entity.name]["writer"].write("\n")
        self.router[entity.name]["last_id"] = record[entity.id_col]
        logger.debug(f"Last ID: {record[entity.id_col]}")

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
    logger.debug(f"Parsing array for entity {str(entity)}, parent_id {parent_id}")
    for prefix, event, value in parser:
        logger.debug(f"prefix: {prefix}, event: {event}, value: {value}")
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
    logger.debug(f"Parsing map for entity {str(entity)}, parent_id {parent_id}")
    map_key = None
    id = writer.get_last_id(entity) + 1
    record = OrderedDict()
    record[entity.id_col] = id
    if parent_id is not None:
        record[entity.parent_id_col] = parent_id
    for prefix, event, value in parser:
        logger.debug(f"prefix: {prefix}, event: {event}, value: {value}")
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


def ensure_target_folder(source: pathlib.Path, target: pathlib.Path) -> pathlib.Path:
    if target:
        target_folder = target
    else:
        target_folder = source.parent.joinpath("output")

    if target_folder.exists():
        shutil.rmtree(target_folder)

    target_folder.mkdir(parents=True, exist_ok=False)

    return target_folder


def main(args):
    args = parse_args(args)
    setup_logging(args.loglevel)
    logger.debug("Starting jnorm...")

    target_folder = ensure_target_folder(source=args.source, target=args.target)
    entity = Entity(hierarchy=[args.source.stem], target_folder=target_folder)
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
