"""
dataset_build.py, by Yaniv Masler, 25-04-2024
Part of the mongodb-redissearch comparison project.
This program creates a JSON dataset
"""

from dataclasses import dataclass, field, asdict
from typing import List, Dict
import json
import random
import string

FIRST_NAME_LIST = [
    "James",
    "John",
    "Robert",
    "Michael",
    "William",
    "David",
    "Richard",
    "Charles",
    "Joseph",
    "Thomas",
    "Olivia",
    "Emma",
    "Charlotte",
    "Amelia",
    "Sophia",
    "Isabella",
    "Ava",
    "Mia",
    "Evelyn",
    "Luna",
    "Santiago",
    "Mateo",
    "Sebastián",
    "Leonardo",
    "Diego",
]
LAST_NAME_LIST = [
    "Smith",
    "Brown",
    "Tremblay",
    "Martin",
    "Roy",
    "Gagnon",
    "Lee",
    "Wilson",
    "Johnson",
    "MacDonald",
]
COLLECTION_SIZE = 1000000
COLLECTION_FILE_NAME = "testData.json"


def snake_to_camel(input: str) -> str:
    camel_cased = "".join(x.capitalize() for x in input.lower().split("_"))
    if camel_cased:
        return camel_cased[0].lower() + camel_cased[1:]
    else:
        return camel_cased


@dataclass
class Child:
    first_name: string = field(default_factory=str)
    last_name: string = field(default_factory=str)
    age: int = field(default_factory=int)

    def __post_init__(self):
        self.first_name = random.choice(FIRST_NAME_LIST)
        self.last_name = random.choice(LAST_NAME_LIST)
        self.age = random.randint(0, 18)


@dataclass
class Person:
    first_name: str = field(default_factory=str)
    last_name: str = field(default_factory=str)
    age: int = field(default_factory=int)
    mobile_number: str = field(default_factory=str)
    description: str = field(default_factory=str)
    children: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.first_name = random.choice(FIRST_NAME_LIST)
        self.last_name = random.choice(LAST_NAME_LIST)
        self.age = random.randint(18, 99)
        self.mobile_number = "".join(random.choices(string.digits, k=10))
        self.description = "".join(random.choices(string.ascii_letters))
        number_of_childrean = random.randint(0, 3)
        for x in range(number_of_childrean):
            self.children.append(Child())

    def to_jason(self, include_null=False) -> dict:
        return asdict(
            self,
            dict_factory=lambda fields: {
                snake_to_camel(key): value
                for (key, value) in fields
                if value is not None or include_null
            },
        )

    def print_person(self):
        print(self.first_name)


@dataclass
class People:
    person: Person


def main():
    print("Hello World")
    p = Person()
    print(p.to_jason())
    p.print_person()
    print("-" * 50)

    json_dataset = []
    for i in range(0, COLLECTION_SIZE):
        p = Person()
        json_dataset.append(p.to_jason())
    with open(COLLECTION_FILE_NAME, "w") as f:
        json.dump(json_dataset, f)


if __name__ == "__main__":
    main()
