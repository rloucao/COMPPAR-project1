# Report

`report.tex` is the report (4 pages + 1 extra page, per the brief), and
`references.bib` holds the bibliography.

```bash
cd report && latexmk -pdf report.tex      # builds report.pdf
```

- Red `[TODO: ...]` markers show what still has to be written; remove each one when done.
- Numbers already filled in come from `results/*-baseline-r*.json` (median of 3 runs)
  and `output/m1_cleaning_stats.csv`.
- `figures/scale_fraction.pdf` and `figures/speedup_workers.pdf` are written by
  `notebooks/analysis.ipynb`. Until they exist, the report shows placeholders.
- If the course releases an official template, move the sections across; the
  structure follows the brief's required sections.
