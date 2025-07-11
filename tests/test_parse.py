import pytest
from src.fields_parser import parse_line
from src.config import ParserSettings, FieldSpec


@pytest.fixture
def parser_config():
    return (
        ParserSettings(parse_to='json', delimiter='|'),
        [
            FieldSpec(name='field1', type='str'),
            FieldSpec(name='field2', type='int'),
            FieldSpec(name='field3', type='float'),
            FieldSpec(name='field4', type='bool')
        ]
    )


def test_parse_line_valid(parser_config):
    raw_line = b"test_string|42|3.14|True"
    expected_result = {
        'field1': 'test_string',
        'field2': 42,
        'field3': 3.14,
        'field4': True
    }

    result = parse_line(raw_line, parser_config[0], parser_config[1])
    assert result == expected_result


@pytest.mark.parametrize("raw_line, got", [
    (b"test_string|42|3.14", 3),            # Missing one field
    (b"test_string|42|3.14|True|extra", 5), # Extra field
])
def test_parse_line_invalid_count(parser_config, raw_line, got):
    delimiter = parser_config[0].delimiter
    field_count = len(parser_config[1])
    expected_pattern = fr"Expected {field_count} fields, got {got}"
    with pytest.raises(ValueError, match=expected_pattern):
        parse_line(raw_line, parser_config[0], parser_config[1])


def test_parse_line_invalid_type(parser_config):
    raw_line = b"test_string|not_an_int|3.14|True"  # Second field is not an int
    with pytest.raises(ValueError, match="Invalid type for field 'field2'"):
        parse_line(raw_line, parser_config[0], parser_config[1])


def test_parse_line_invalid_bool(parser_config):
    raw_line = b"test_string|42|3.14|not_a_bool"  # Last field is not a bool
    with pytest.raises(ValueError, match="Invalid type for field 'field4'"):
        parse_line(raw_line, parser_config[0], parser_config[1])


def test_parse_line_empty_fields_and_whitespace(parser_config):
    for raw_line in [b"|||", b"", b"   ", None]:
        # None or all-empty/whitespace input
        with pytest.raises(ValueError, match="Invalid line format"):
            parse_line(raw_line, parser_config[0], parser_config[1])


def test_parse_line_special_and_unicode(parser_config):
    # Valid lines with special/unicode characters
    examples = [
        (b"test_string|42|3.14|True",
         {'field1': 'test_string', 'field2': 42, 'field3': 3.14, 'field4': True}),
        (b"\xe2\x9c\x93|42|3.14|True",  # ✓
         {'field1': '\u2713',         'field2': 42, 'field3': 3.14, 'field4': True}),
        (b"\xe2\x82\xac|42|3.14|True",  # €
         {'field1': '\u20ac',         'field2': 42, 'field3': 3.14, 'field4': True}),
    ]

    for raw_line, expected in examples:
        result = parse_line(raw_line, parser_config[0], parser_config[1])
        assert result == expected
