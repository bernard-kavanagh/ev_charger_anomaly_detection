"""
Integration Tests for Hybrid Search against a live TiDB cluster
================================================================
Exercises the real FTS_MATCH_WORD hybrid query against outage_catalog.

These tests hit a live TiDB Cloud cluster and generate real embeddings,
so they are skipped by default. To run:

    RUN_INTEGRATION=1 python -m pytest tests/test_integration_hybrid.py -x -q -s

Requires TIDB_HOST (+ the other TIDB_* creds, typically via .env) and
RUN_INTEGRATION=1. The suite inserts rows prefixed ITEST- into
outage_catalog and deletes them on teardown.
"""

import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load .env so TIDB_* creds are available when run outside a shell that
# already exported them. No-op if python-dotenv isn't installed.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
    ))
except Exception:
    pass


# ---------------------------------------------------------------------------
# Skip gate: only run with an explicit opt-in AND a configured cluster.
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.skipif(
    not (os.environ.get("TIDB_HOST") and os.environ.get("RUN_INTEGRATION") == "1"),
    reason="integration test: set TIDB_HOST and RUN_INTEGRATION=1 to run",
)


_ROW1 = {
    "pattern_id": "ITEST-001",
    "pattern_name": "ITEST Ground Fault",
    "category": "electrical",
    "root_cause": (
        "Persistent GroundFailure on the earth-leakage monitor after "
        "rain ingress corroded the coastal enclosure gland."
    ),
    "resolution": "Reseal the enclosure gland and replace the RCM module.",
    "severity": "safety",
}
_ROW2 = {
    "pattern_id": "ITEST-002",
    "pattern_name": "ITEST Thermal Derate",
    "category": "thermal",
    "root_cause": (
        "Thermal runaway in the power module during sustained fast "
        "charging when ambient cooling airflow is obstructed."
    ),
    "resolution": "Clear the intake filter and recalibrate the fan curve.",
    "severity": "degraded",
}


@pytest.fixture(scope="module")
def seeded_catalog():
    """Insert two ITEST- outage rows with real embeddings; delete on teardown."""
    from tool_handlers import get_db, embed

    def _insert(cur, row):
        vec = embed(row["root_cause"])
        cur.execute(
            """
            INSERT INTO outage_catalog
                (pattern_id, pattern_name, category, root_cause, symptoms,
                 resolution, severity, signature_vec)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row["pattern_id"], row["pattern_name"], row["category"],
                row["root_cause"], "[]", row["resolution"], row["severity"],
                str(vec),
            ),
        )

    with get_db() as db, db.cursor() as cur:
        # Idempotent: clear any leftovers from a prior aborted run.
        cur.execute("DELETE FROM outage_catalog WHERE pattern_id LIKE 'ITEST-%%'")
        _insert(cur, _ROW1)
        _insert(cur, _ROW2)

    yield

    with get_db() as db, db.cursor() as cur:
        cur.execute("DELETE FROM outage_catalog WHERE pattern_id LIKE 'ITEST-%%'")


def _search(query_text):
    """Run the real hybrid search against outage_catalog, scoped to ITEST rows."""
    from tool_handlers import get_db, embed, _hybrid_search

    query_vec = str(embed(query_text))
    with get_db() as db, db.cursor() as cur:
        return _hybrid_search(
            cur, "outage_catalog", "signature_vec",
            "root_cause",
            query_vec, query_text,
            ["pattern_id LIKE %s"], ["ITEST-%"], limit=5,
        )


class TestHybridSearchIntegration:
    def test_keyword_query_ranks_matching_row_first(self, seeded_catalog, caplog):
        """Assert 1 (+ Assert 3): an exact keyword from row 1's root_cause
        returns row 1 first, ft_used is True, and NO fallback warning fired.

        This is the assertion the original MATCH..AGAINST bug would fail:
        the full-text query would have thrown pymysql.Error and silently
        degraded to vector-only (ft_used False)."""
        caplog.set_level(logging.WARNING, logger="tool_handlers")

        rows, ft_used = _search(
            "Investigating a GroundFailure on a coastal charger enclosure"
        )

        assert ft_used is True, "hybrid full-text path must run (regression guard)"
        assert rows, "expected at least one result"
        assert rows[0]["pattern_id"] == "ITEST-001", (
            f"expected ITEST-001 ranked first, got {rows[0]['pattern_id']}"
        )
        # Assert 3: no FULLTEXT_FALLBACK sentinel during a working query.
        assert not any(
            "FULLTEXT_FALLBACK" in r.getMessage() for r in caplog.records
        ), "no fallback warning should fire on a successful hybrid query"

    def test_semantic_only_query_uses_vector_path(self, seeded_catalog):
        """Assert 2: a query with no extractable keywords still returns
        results via the vector path, with no exception."""
        rows, ft_used = _search(
            "the unit stopped delivering energy after heavy continuous use"
        )
        assert ft_used is False, "generic text yields no keywords → vector-only"
        assert rows, "vector path must still return results"

    def test_keyword_absent_from_all_rows_returns_empty_not_vector(
        self, seeded_catalog
    ):
        """Assert 4 (fallback discipline): a keyword present in NO seeded
        row's root_cause must return (rows=[], ft_used=True).

        This proves the hybrid FTS path executed and correctly returned an
        empty 'no keyword matches' answer — it did NOT silently degrade to
        vector-only (which would return the closest rows and hide that the
        keyword filter matched nothing). 'InternalError' is extracted as a
        keyword but appears in neither ITEST row's root_cause."""
        rows, ft_used = _search(
            "charger reported an InternalError during boot sequence"
        )
        assert ft_used is True, (
            "valid FTS query that matches nothing must report ft_used=True, "
            "not degrade to the vector path"
        )
        # fetchall() may return a tuple or a list depending on the cursor;
        # assert emptiness type-agnostically rather than == [].
        assert len(rows) == 0, (
            f"expected zero keyword matches, got {[r.get('pattern_id') for r in rows]}"
        )
