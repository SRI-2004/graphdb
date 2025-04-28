# Plan for Modularizing the Neo4j Data Pipeline (Technical Refinement)

## Goal

Refactor the monolithic `pipeline.py` script into a modular structure within a new `pipeline/` directory. This enhances maintainability, readability, testability, and scalability. The original `pipeline.py` will remain unaltered.

## Target Architecture

```
.
├── pipeline.py           # Original monolithic script (untouched)
├── pipeline_plan.md      # This file
└── pipeline/             # New directory for modular code
    ├── __init__.py       # Marks 'pipeline' as a Python package
    ├── graph_base.py     # Contains GraphTransformer class (core Neo4j interactions)
    ├── main_pipeline.py  # Orchestrates data loading and transformation steps
    └── transformers/       # Sub-package for entity-specific transformation logic
        ├── __init__.py   # Marks 'transformers' as a Python package
        ├── account_transformer.py     # Handles AdAccount, Account Metrics, ConversionAction
        ├── campaign_transformer.py    # Handles Campaign, Budget, Criterion, Campaign Metrics
        ├── ad_transformer.py          # Handles AdGroup, Ad, KeywordGroup, Ad Metrics
        ├── audience_transformer.py    # Handles Audience and its components
        ├── product_transformer.py     # Handles Product (from shopping view)
        └── misc_transformer.py        # Handles Label, Asset
```

## Refactoring Sprint Plan

**Sprint Goal:** Complete the modularization of the pipeline code into the new structure, ensuring functional equivalence with the original script.

**User Story:** As a developer, I want to refactor the pipeline script into logical modules so that it is easier to understand, maintain, and extend.

---

**Task 1: Setup Project Structure**

*   **Action:** Create the directory structure: `pipeline/` and `pipeline/transformers/`.
*   **Action:** Create empty files:
    *   `pipeline/__init__.py`
    *   `pipeline/graph_base.py`
    *   `pipeline/main_pipeline.py`
    *   `pipeline/transformers/__init__.py`
    *   `pipeline/transformers/account_transformer.py`
    *   `pipeline/transformers/campaign_transformer.py`
    *   `pipeline/transformers/ad_transformer.py`
    *   `pipeline/transformers/audience_transformer.py`
    *   `pipeline/transformers/product_transformer.py`
    *   `pipeline/transformers/misc_transformer.py`
*   **Check:** Verify all directories and empty files exist as per the target architecture.

**Task 2: Implement Base Neo4j Logic (`pipeline/graph_base.py`)**

*   **Action:** Copy essential imports (`json`, `pandas`, `neo4j`, `logging`, `os`, `typing`) from `pipeline.py` to `graph_base.py`.
*   **Action:** Copy the `logging.basicConfig` setup. Define a base logger: `logger = logging.getLogger(__name__)`.
*   **Action:** Move the `GraphTransformer` class definition to `graph_base.py`.
*   **Action:** Retain only the following methods within the `GraphTransformer` class:
    *   `__init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str)`: Keep driver init, `BATCH_SIZE`, `metric_categories`. Log initialization.
    *   `close(self)`
    *   `create_constraints(self)`: Keep the exact Cypher queries.
    *   `create_indexes(self)`: Keep the exact Cypher queries.
    *   `create_entity_nodes_batch(self, tx, entity_type: str, nodes: List[Dict[str, Any]])`: Keep the logic with `id_property` mapping and UNWIND query.
    *   `create_relationships_batch(self, tx, start_type: str, end_type: str, rel_type: str, relationships: List[Dict[str, Any]])`: Keep the generic UNWIND query.
*   **Action:** Explicitly delete *all* `transform_...` methods and `run_pipeline` from the `GraphTransformer` class in `graph_base.py`.
*   **Check:** `graph_base.py` contains only the specified imports and methods. No `transform_...` methods remain in the class. The `id_property` map in `create_entity_nodes_batch` should cover all entity types from the original script, including keys like: `'AdAccount'`, `'Campaign'`, `'AdGroup'`, `'Ad'`, `'KeywordGroup'`, `'Audience'`, `'Asset'`, `'ConversionAction'`, `'Metric'`, `'AdMetricsSnapshot'`, `'CampaignMetricsSnapshot'`, `'AdGroupMetricsSnapshot'`, `'Label'`, `'CampaignCriterion'`, `'CampaignBudget'`, `'Product'`, `'GeoLocation'`, `'DailyMetric'`, `'AdDailyMetric'`, `'AccountDailyMetric'`, `'AccountMonthlyMetric'`, `'AdGroupWeeklyMetric'`, `'AdGroupBiddingSettings'`, `'AgeRange'`, `'Gender'`, `'UserInterest'`, `'CustomAudience'`, `'AdOverallMetric'`, `'AdMonthlyMetric'`, `'CampaignOverallMetric'`, `'CampaignMonthlyMetric'`, `'AccountOverallMetric'`, `'WeeklyMetric'`.

**Task 3: Implement Transformer Modules (`pipeline/transformers/*.py`)**

*   **Action:** For *each* transformer file (e.g., `account_transformer.py`):
    1.  **Add Imports:** Include `import pandas as pd`, `import json`, `import logging`, `from typing import Dict, List, Any`, `from ..graph_base import GraphTransformer`.
    2.  **Add Logger:** `logger = logging.getLogger(__name__)`.
    3.  **Move Functions:** Cut the relevant `transform_...` methods from the original `pipeline.py` and paste them as top-level functions in the target file.
    4.  **Update Signatures:** Modify each function signature to remove `self` and accept `transformer: GraphTransformer` as the first argument, followed by the required `pd.DataFrame` arguments. (e.g., `def transform_adaccount(transformer: GraphTransformer, account_df: pd.DataFrame):`). Type hint appropriately.
    5.  **Update Internal Calls:** Replace `self.driver...` with `transformer.driver...`. Replace `self.create_entity_nodes_batch(...)` with `transformer.create_entity_nodes_batch(...)`. Replace `self.BATCH_SIZE` with `transformer.BATCH_SIZE`.
    6.  **Verify Logic:** Ensure all logic, including DataFrame manipulations, node property creation, and relationship creation specific to the transformed entities, is moved correctly.

*   **Function & Relationship Mapping:**
    *   `account_transformer.py`:
        *   `transform_adaccount` (Node: `AdAccount`)
        *   `transform_account_monthly_metrics` (Node: `AccountMonthlyMetric`, Rel: `AdAccount -[:HAS_MONTHLY_METRICS]-> AccountMonthlyMetric`)
        *   `transform_account_overall_metrics` (Node: `AccountOverallMetric`, Rel: `AdAccount -[:HAS_OVERALL_METRICS]-> AccountOverallMetric`)
        *   `transform_conversion_action` (Node: `ConversionAction`, Rel: `AdAccount -[:HAS_CONVERSION_ACTIONS]-> ConversionAction`)
    *   `campaign_transformer.py`:
        *   `transform_campaign` (Node: `Campaign`, Rel: `AdAccount -[:HAS_CAMPAIGN]-> Campaign`)
        *   `transform_campaign_budget` (Node: `CampaignBudget`, Rel: `Campaign -[:USES_BUDGET]-> CampaignBudget`)
        *   `transform_campaign_criterion` (Nodes: `GeoLocation`, others implicit. Rels: `Campaign -[:TARGETS_LOCATION]-> GeoLocation`, `Campaign -[:EXCLUDES_LOCATION]-> GeoLocation`)
        *   `transform_campaign_weekly_metrics` (Node: `WeeklyMetric` {entity_type: 'Campaign'}, Rel: `Campaign -[:HAS_WEEKLY_METRICS]-> WeeklyMetric`)
        *   `transform_campaign_overall_metrics` (Node: `CampaignOverallMetric`, Rel: `Campaign -[:HAS_OVERALL_METRICS]-> CampaignOverallMetric`)
        *   `transform_campaign_monthly_metrics` (Node: `CampaignMonthlyMetric`, Rel: `Campaign -[:HAS_MONTHLY_METRICS]-> CampaignMonthlyMetric`)
    *   `ad_transformer.py`:
        *   `transform_adgroup` (Nodes: `AdGroup`, `AdGroupBiddingSettings`. Rels: `Campaign -[:HAS_ADGROUP]-> AdGroup`, `AdGroup -[:HAS_BIDDING_SETTINGS]-> AdGroupBiddingSettings`)
        *   `transform_ad` (Node: `Ad`. Rel: `AdGroup -[:CONTAINS]-> Ad`)
        *   `transform_keyword` (Node: `KeywordGroup`. Rel: `AdGroup -[:HAS_KEYWORDS]-> KeywordGroup`)
        *   `transform_ad_daily_metrics` (Node: `AdDailyMetric`. Rel: `Ad -[:HAS_DAILY_METRICS]-> AdDailyMetric`)
        *   `transform_ad_overall_metrics` (Node: `AdOverallMetric`. Rel: `Ad -[:HAS_OVERALL_METRICS]-> AdOverallMetric`)
        *   `transform_ad_monthly_metrics` (Node: `AdMonthlyMetric`. Rel: `Ad -[:HAS_MONTHLY_METRICS]-> AdMonthlyMetric`)
    *   `audience_transformer.py`:
        *   `transform_audience` (Nodes: `Audience`, `AgeRange`, `Gender`, `UserInterest`, `CustomAudience`. Rels: `AdAccount -[:DEFINED_AUDIENCE]-> Audience`, `Audience -[:HAS_AGE_RANGE]-> AgeRange`, `Audience -[:HAS_GENDER]-> Gender`, `Audience -[:INCLUDES_SEGMENT]-> UserInterest`, `Audience -[:INCLUDES_SEGMENT]-> CustomAudience`)
    *   `product_transformer.py`:
        *   `transform_product` (Node: `Product`. Rel: `Campaign -[:ADVERTISES_PRODUCT]-> Product`)
    *   `misc_transformer.py`:
        *   `transform_label` (Node: `Label`. Rel: `AdAccount -[:HAS_LABEL]-> Label` - *requires customer_label_df*)
        *   `transform_asset` (Node: `Asset`)

*   **Check:** Each transformer file contains only the specified functions. Function signatures are updated correctly. All `self` references are replaced with `transformer`. Imports are correct (using relative `..graph_base`). Logging is initialized. Node and relationship logic matches the original intent for the moved functions.

**Task 4: Implement Orchestrator (`pipeline/main_pipeline.py`)**

*   **Action:** Add imports: `os`, `logging`, `psycopg2`, `pandas`, `sys` (potentially for path manipulation if needed, though relative imports should work), `from .graph_base import GraphTransformer`, and import all transformer modules: `from .transformers import account_transformer, campaign_transformer, ...`.
*   **Action:** Define `run_full_pipeline()` function.
*   **Action:** Copy PostgreSQL connection details and logic (using `os.getenv`).
*   **Action:** Copy `table_exists(pg_conn, table_name)` function (pass connection).
*   **Action:** Copy the data loading loop (using `table_exists`).
*   **Action:** Copy the logic for deriving `sql_data['asset']` and `sql_data['conversion_action']`.
*   **Action:** Instantiate `transformer = GraphTransformer(...)`.
*   **Action:** Implement the main execution block within a `try...except...finally`:
    *   `try:` block:
        *   Call `transformer.create_constraints()`.
        *   Call `transformer.create_indexes()`.
        *   Determine `customer_id`.
        *   Call *each* transformer function in the correct dependency order, passing `transformer` and the required `sql_data` slice(s). Log before/after each call. (Follow the order specified in the original `run_pipeline` method). Example:
          ```python
          logger.info("Pipeline Step: AdAccount transformation")
          if customer_id and 'customer' in sql_data:
              account_transformer.transform_adaccount(transformer, sql_data['customer'])
          else:
              logger.warning("Skipping AdAccount...")

          logger.info("Pipeline Step: Audience transformation")
          if customer_id and 'audience' in sql_data:
              audience_transformer.transform_audience(transformer, sql_data['audience'])
          else:
              logger.warning("Skipping Audience...")
          # ... continue for all steps in correct order ...
          ```
    *   `except Exception as e:` block: Log error and traceback.
    *   `finally:` block: Close `pg_conn` and `transformer.close()`.
*   **Action:** Add `if __name__ == "__main__":` block to call `run_full_pipeline()`.
*   **Check:** All necessary imports are present. `run_full_pipeline` contains DB connection, data loading, transformer instantiation, constraint/index creation, transformation calls, and connection closing. The order of transformation calls strictly follows the dependencies identified in the original `run_pipeline`. Error handling and connection closing are present.

**Task 5: Review, Test, and Verify**

*   **Action:** Perform a code review of all new files in the `pipeline/` directory. Pay close attention to imports (especially relative imports like `from ..graph_base import ...`).
*   **Action:** Run a linter (like `flake8` or `pylint`) over the `pipeline/` directory to catch syntax errors and style issues.
*   **Action:** Set necessary environment variables (`NEO4J_USER`, `NEO4J_PASSWORD`, DB credentials).
*   **Action:** Execute the pipeline from the *workspace root* directory: `python -m pipeline.main_pipeline`.
*   **Action:** Monitor logs for errors during execution. Debug any exceptions.
*   **Action:** If the pipeline runs successfully, perform basic checks in the Neo4j database (e.g., `MATCH (n) RETURN labels(n), count(*)`) to verify nodes are created. Check a few relationships.
*   **Action:** Explicitly verify that the original `pipeline.py` file has *not* been modified.
*   **Check:** Code passes linting. Pipeline executes without Python errors when run via `python -m pipeline.main_pipeline`. Logs indicate successful completion or expected warnings for missing data. Basic node counts in Neo4j seem reasonable. `pipeline.py` is identical to its state before the refactoring.

---

This detailed plan provides specific actions and verification steps for each stage of the refactoring process. 