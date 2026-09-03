# Meld Protocol Specification

This directory contains the working LaTeX specification for Meld.

- [meld-protocol.tex](meld-protocol.tex): versioned protocol specification source.
  It separates front matter from five navigable parts, provides task-oriented
  reading paths and a visual index, and uses a consistent, grayscale-safe diagram
  language. Native TikZ figures cover layering, recovery timing, symbol layout, a
  worked rank-recovery example, moving-versus-fixed coding geometry, decoder
  isolation, receiver admission, and automatic policy selection. Side-by-side
  coding and signal-to-action tables explain both the algorithms and the
  observations that control them; the continuous allocator subsection records
  evidence attack/release, reactive and slack scaling, and safe block-boundary
  updates. Normative RFC requirement words and field tables remain authoritative;
  figures and worked examples are explanatory maps.

Build the PDF with:

```sh
make -C docs/spec
```

The build uses `latexmk` when available, otherwise `tectonic`, otherwise
`pdflatex`. Generated PDF and auxiliary files are intentionally ignored by git;
commit the `.tex` source and curated notes, not build output.
