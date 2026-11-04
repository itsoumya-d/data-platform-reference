from dataplat.ai_service import InsightService, Note, TfidfRetriever


def notes():
    return [
        Note("N1", "C1", "A refund delay caused a payment risk review."),
        Note("N2", "C1", "The customer asked about invoice timing."),
        Note("N3", "C2", "Refund completed without incident."),
    ]


def test_retriever_ranks_the_matching_note():
    hits = TfidfRetriever(notes()).search("refund delay", k=2)
    assert hits[0].note_id == "N1"


def test_insight_is_extractive_and_cited():
    result = InsightService(notes()).insight("C1", query="refund delay")
    assert result["citations"] == ["N1"]
    assert "refund delay" in result["summary"].lower()
    assert result["provider"] == "extractive-v1"


def test_missing_customer_has_no_citations():
    result = InsightService(notes()).insight("C9")
    assert result["citations"] == []
