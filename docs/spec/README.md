# Meld Protocol Specification

This directory contains the working LaTeX specification for Meld.

- [meld-protocol.tex](meld-protocol.tex): versioned protocol specification source.

Build the PDF with:

```sh
make -C docs/spec
```

The build uses `latexmk` when available, otherwise `tectonic`, otherwise
`pdflatex`. Generated PDF and auxiliary files are intentionally ignored by git;
commit the `.tex` source and curated notes, not build output.
