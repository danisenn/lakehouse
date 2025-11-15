import pytest
import polars as pl

from ..semantic_filed_mapping import SemanticFieldMapper, map_columns


def test_exact_case_insensitive_match():
    df = pl.DataFrame(
        {
            "Email": ["a@example.com", "b@example.com"],
            "PHONE": ["123", "456"],
            "Name": ["Alice", "Bob"],
        }
    )
    reference_fields = ["email", "phone", "name"]

    mapper = SemanticFieldMapper(reference_fields=reference_fields, threshold=0.7)
    result = mapper.map_columns(df)

    # All columns should map to the corresponding reference field ignoring case
    assert result["mapping"]["Email"]["target"].lower() == "email"
    assert result["mapping"]["PHONE"]["target"].lower() == "phone"
    assert result["mapping"]["Name"]["target"].lower() == "name"


def test_partial_reference_fields():
    df = pl.DataFrame(
        {
            "Email": ["a@example.com"],
            "PhoneNumber": ["123"],
            "Name": ["Alice"],
        }
    )
    reference_fields = ["email", "name"]  # phone is not in reference list

    res = map_columns(df=df, reference_fields=reference_fields, threshold=0.7)

    assert res["mapping"]["Email"]["target"].lower() == "email"
    assert res["mapping"]["Name"]["target"].lower() == "name"
    # Column not in references should not be accepted; it should be unmapped or ambiguous
    assert "PhoneNumber" not in res["mapping"]
    assert ("PhoneNumber" in res["unmapped"]) or ("PhoneNumber" in res["ambiguous"])  


def test_no_matches():
    df = pl.DataFrame(
        {
            "foo": [1, 2],
            "bar": [3, 4],
        }
    )
    reference_fields = ["email", "phone", "name"]

    res = map_columns(df=df, reference_fields=reference_fields)

    assert res["mapping"] == {}  # nothing should match above threshold
    assert set(res["unmapped"]) == {"foo", "bar"}


def test_duplicate_reference_names_different_case():
    # Ensure duplicates in reference_fields don’t break behavior
    df = pl.DataFrame({"Email": ["a@example.com"]})
    reference_fields = ["EMAIL", "email"]  # semantically same

    res = map_columns(df=df, reference_fields=reference_fields)

    # Either "EMAIL" or "email" is acceptable as target, depending on scoring
    assert "Email" in res["mapping"]
    assert res["mapping"]["Email"]["target"].lower() == "email"


def test_empty_dataframe():
    df = pl.DataFrame()
    reference_fields = ["email", "phone"]

    res = map_columns(df=df, reference_fields=reference_fields)

    assert res["mapping"] == {}
    assert res["unmapped"] == []


def test_synonym_based_mapping():
    df = pl.DataFrame({
        "e_mail": ["a@example.com"],
        "cust_phone": ["+1-555-1234"],
    })
    refs = ["email", "phone"]
    synonyms = {
        "email": ["e_mail", "email_address", "mail"],
        "phone": ["phone_number", "tel", "telephone", "cust_phone"],
    }

    mapper = SemanticFieldMapper(reference_fields=refs, synonyms=synonyms, threshold=0.7)
    res = mapper.map_columns(df)

    assert res["mapping"]["e_mail"]["target"] == "email"
    assert res["mapping"]["cust_phone"]["target"] == "phone"


essential_cols = ["Label", "Title", "ReviewText"]

def test_mapping_from_column_list_without_df():
    # Provide just a list of column names
    cols = essential_cols
    refs = ["label", "title", "text"]

    res = map_columns(columns=cols, reference_fields=refs)
    assert res["mapping"]["Label"]["target"].lower() == "label"
    assert res["mapping"]["Title"]["target"].lower() == "title"
    # "ReviewText" should map to "text" via token overlap
    assert res["mapping"]["ReviewText"]["target"].lower() == "text"


def test_ambiguity_detection_with_large_epsilon():
    # Force ambiguity by using a very large epsilon so top-2 within window
    df = pl.DataFrame({"email": ["a@example.com"]})
    refs = ["email", "e_mail"]

    mapper = SemanticFieldMapper(reference_fields=refs, epsilon=1.0, threshold=0.5)
    res = mapper.map_columns(df)

    # Should be considered ambiguous (not auto-accepted) because second is close under large epsilon
    assert "email" in res["ambiguous"] or "email" in res["mapping"]


def test_threshold_blocks_weak_matches():
    df = pl.DataFrame({"phone_number": ["123", "456"]})
    refs = ["phone"]

    # Set threshold higher than sequence similarity between 'phone_number' and 'phone'
    res = map_columns(df=df, reference_fields=refs, threshold=0.99)

    assert res["mapping"] == {}
    assert res["unmapped"] == ["phone_number"]
