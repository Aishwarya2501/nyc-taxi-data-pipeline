# AI Usage Disclosure

**Tools Used:** Databricks Genie Code (data validation) and Gemini (code refactoring and documentation).

**Architectural Ownership & Design:**
The system design and foundational approach were my own decisions. I opted to structure the pipeline using a **Medallion architecture (Bronze → Silver → Gold)** and implemented a **Quarantine pattern** (retaining failed rows with explicit rejection tags rather than silently dropping data). I mapped out the ingest, transform, and validation stages upfront, using the AI as an execution tool to help build out that structural blueprint.

**Code Breakdown:**
Roughly 80% of the submitted code is mine, the AI's share is limited to refactoring passes and boilerplate (the orchestrator's dynamic parts, the formatting of checks in `quality_checks.py`). For documentation, roughly 60% is my effort. `DATA_DICTIONARY.md` has my effort in lining up the column descriptions, and I supplied the outline, facts, and approach behind the README and query exports; the AI drafted the prose.


### What the AI Touched

* **Written by me:** `src/ingest.py`, `DATA_DICTIONARY.md`, and the SQL queries. The AI was only used to verify that my manual PySpark checks produced the expected row counts.
* **`src/clean_transform.py`:** I mapped out the schema alignment and zone joins. I prompted the AI to refactor and optimize my PySpark syntax (e.g., implementing broadcast hints).
* **`src/quality_checks.py`:** The check logic and every rule design are mine, all versions. The final formatting pass of the file was done by the AI at my direction, and I verified it produces row-for-row identical output to my own version.
* **`src/orchestrator.py`:** I designed the stage execution and modular flow. The AI drafted the boilerplate for the dynamic path resolution and the Unity Catalog `saveAsTable` syntax based on my instructions.
* **`README.md`:** I provided the outline, architectural decisions, and template. The AI drafted the documentation.
* query results (`QUERY_RESULTS.md`):AI-generated file. Every number comes straight from SQL queries i've written without AI help.

### What the AI Got Wrong (My Corrections)
As expected, the AI lacked broader architectural context, requiring me to override it on several fronts:

1. **Hardcoded Common Columns:** For combining the Yellow and Green datasets, the AI's approach hardcoded the list of common columns and selected them explicitly. I instead reverse-engineered the problem from the *unmatched* columns, rename only the datetime columns that differ (`tpep_`/`lpep_` → `pickup_datetime`/`dropoff_datetime`), tag each source with `cab_colour`, and let `unionByName(..., allowMissingColumns=True)` auto-fill the rest with NULL. The code only ever handles the differences, so it tolerates schema drift instead of breaking on it.
2. **Storage Anti-Patterns:** The AI suggested hardcoding `/Volumes/` absolute paths for the outputs. I rejected this and rewrote it to use Unity Catalog Managed Tables (`workspace.default...`) to properly satisfy the "no hardcoded paths" constraint.
3. **Over-engineering:** The AI initially generalized the entry point into a `main.py` with `argparse` parameterization. I rejected both: my design keeps a single `orchestrator.py` as the one end-to-end run for the core package - no arguments, no options.


