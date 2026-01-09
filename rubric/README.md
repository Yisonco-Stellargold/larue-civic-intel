## Rubric overview

This directory defines the scoring axes and weights used for civic-intelligence evaluations.
The rubric is designed to stay neutral, focusing on constitutional rights, fiscal stewardship,
transparency, and procedural integrity rather than ideological framing.

## Weights and normalization

- `weights.yaml` assigns axis weights that must be non-negative.
- Scoring code should normalize weights so they sum to 1.0 before applying them to per-axis
  scores.
- If additional weight files are introduced, follow the same normalization rule.

## Scoring focus

Rubric language should remain factual and scoped to:
- Constitutional and statutory protections.
- Fiscal impacts and budgetary discipline.
- Transparency, due process, and public access to records.

Avoid partisan labels or ideology-based scoring criteria.

## TODO (integration prep)

- TODO: Define the exact normalization formula in code once the scorer is integrated.
- TODO: Document the expected input/output schema for rubric scoring in this README.
