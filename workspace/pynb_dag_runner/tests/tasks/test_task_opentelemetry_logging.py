from pynb_dag_runner.tasks.task_opentelemetry_logging import SerializedData

# --
import pytest

# ---- test SerializedData encoding and decoding ----


@pytest.mark.parametrize(
    "test_case",
    [
        ("foo", SerializedData("utf-8", "utf-8", "foo")),
        (123, SerializedData("int", "json", "123")),
        (bytes([0, 1, 2, 3, 4, 5]), SerializedData("bytes", "base64", "AAECAwQF")),
    ],
)
def test__encode_decode_to_wire__explicit_examples(test_case):
    data, serialized_data = test_case

    assert SerializedData.encode(data) == serialized_data
    assert serialized_data.decode() == data


def test__encode_decode_to_wire__is_identity():
    for msg in [
        "test-text-message",
        bytes([0, 1, 2, 3]),
        bytes(1000 * list(range(256))),
        True,
        1.23,
        1000000,
    ]:
        assert msg == SerializedData.encode(msg).decode()


def test__encode_decode_to_wire__exceptions_for_invalid_data():
    class Foo:
        pass

    # encoding/decoding should fail for these inputs
    examples_of_invalid_data = [None, Foo()]

    for invalid_data in examples_of_invalid_data:
        with pytest.raises(Exception):
            SerializedData.encode(invalid_data)

    with pytest.raises(ValueError):
        SerializedData("string", "utf8", "should be 'utf-8'").decode()
