## R4 — Hybrid Retrieval and Custom Reranking

**Retrieval method:** Hybrid Search

The policy retrieval implementation uses Databricks Vector Search with `query_type="HYBRID"` to combine keyword-based and vector-based retrieval.

The retrieval process works as follows:

1. **Hybrid retrieval:** Retrieves the top 20 candidate policy chunks using keyword and semantic vector search.
2. **Custom reranking:** Uses the `BAAI/bge-reranker-base` Cross-Encoder model to calculate relevance scores for each question-document pair.
3. **Ranking:** Sorts the candidate chunks by descending reranking score.
4. **Top-5 selection:** Returns the five most relevant policy sections.

**Vector Search index:** `claims.genai.policy_chunks_idx`

**Source table:** `claims.genai.policy_chunks`

**Reranking model:** `BAAI/bge-reranker-base`

**Final retrieval output:** Top 5 policy chunks, ranked by relevance score.

The retrieval is tested against G04, G05, G07, and G08. The expected first-ranked chunks are from the individual-plan document for G04 and G05, and the group-plan document for G07 and G08.