from pathlib import Path

import pytest

from contract_registry import load_contracts


def test_load_contract_from_yaml(
    tmp_path: Path,
) -> None:
    contract_file = tmp_path / "test_event.v1.yaml"

    contract_file.write_text(
        """
name: test_event
version: 1
topic: test.event.v1

schema:
  fields:
    - name: event_id
      type: string
      nullable: false
""".strip(),
        encoding="utf-8",
    )

    contracts = load_contracts(tmp_path)

    assert len(contracts) == 1

    contract = contracts[0]

    assert contract["name"] == "test_event"
    assert contract["version"] == 1
    assert contract["topic"] == "test.event.v1"

    assert contract["schema"]["fields"] == [
        {
            "name": "event_id",
            "type": "string",
            "nullable": False,
        }
    ]


def test_load_multiple_contracts(
    tmp_path: Path,
) -> None:
    first_contract = tmp_path / "first.v1.yaml"
    second_contract = tmp_path / "second.v1.yaml"

    first_contract.write_text(
        """
name: first
version: 1
topic: test.first.v1

schema:
  fields:
    - name: id
      type: string
      nullable: false
""".strip(),
        encoding="utf-8",
    )

    second_contract.write_text(
        """
name: second
version: 1
topic: test.second.v1

schema:
  fields:
    - name: id
      type: string
      nullable: false
""".strip(),
        encoding="utf-8",
    )

    contracts = load_contracts(tmp_path)

    assert len(contracts) == 2

    names = {
        contract["name"]
        for contract in contracts
    }

    assert names == {
        "first",
        "second",
    }


def test_invalid_yaml_raises_error(
    tmp_path: Path,
) -> None:
    contract_file = tmp_path / "invalid.v1.yaml"

    contract_file.write_text(
        """
name: invalid
version: [
topic: test.invalid.v1
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(Exception):
        load_contracts(tmp_path)


def test_duplicate_topics_raise_error(
    tmp_path: Path,
) -> None:
    first_contract = tmp_path / "first.v1.yaml"
    second_contract = tmp_path / "second.v1.yaml"

    first_contract.write_text(
        """
name: first
version: 1
topic: test.same-topic.v1

schema:
  fields:
    - name: id
      type: string
      nullable: false
""".strip(),
        encoding="utf-8",
    )

    second_contract.write_text(
        """
name: second
version: 1
topic: test.same-topic.v1

schema:
  fields:
    - name: id
      type: string
      nullable: false
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match="topic|Topic|duplicate|Duplicate",
    ):
        load_contracts(tmp_path)