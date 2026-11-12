# R2 – Column Lineage Evidence

## Target Column

- **Catalog:** `claims`
- **Schema:** `raw`
- **Table:** `members`
- **Column:** `phone`
- **Fully Qualified Name:** `claims.raw.members.phone`

## Upstream Table Lineage

The following tables contribute to the target column:

| S. No. | Fully Qualified Table Name | Contribution |
|---|---|---|
| 1 | `claims.raw.members` | Target table containing the phone column |

## Lineage Explanation

The `phone` column is stored in `claims.raw.members`. Its upstream source and transformation lineage should be verified using Unity Catalog lineage information.

## Evidence

Attach the Unity Catalog lineage screenshot or query output showing the upstream dependencies of `claims.raw.members.phone`.