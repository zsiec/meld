LATEXMK ?= latexmk
TECTONIC ?= tectonic
PDFLATEX ?= pdflatex

.PHONY: all clean

all: meld-protocol.pdf

meld-protocol.pdf: meld-protocol.tex Makefile
	@if command -v $(LATEXMK) >/dev/null 2>&1; then \
		$(LATEXMK) -pdf -interaction=nonstopmode -halt-on-error $<; \
	elif command -v $(TECTONIC) >/dev/null 2>&1; then \
		$(TECTONIC) --keep-logs --keep-intermediates $<; \
	else \
		$(PDFLATEX) -interaction=nonstopmode -halt-on-error $<; \
		$(PDFLATEX) -interaction=nonstopmode -halt-on-error $<; \
	fi

clean:
	@if command -v $(LATEXMK) >/dev/null 2>&1; then \
		$(LATEXMK) -C meld-protocol.tex; \
	else \
		rm -f meld-protocol.aux meld-protocol.bbl meld-protocol.bcf \
			meld-protocol.blg meld-protocol.fdb_latexmk meld-protocol.fls \
			meld-protocol.log meld-protocol.out meld-protocol.run.xml \
			meld-protocol.synctex.gz meld-protocol.toc meld-protocol.pdf; \
	fi
