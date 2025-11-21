%.mmd.pdf: %.mmd
	mmdc --pdfFit --input $^ --outputFormat pdf --output $@

DIAGRAMS = $(wildcard *.mmd)
COMPILED_DIAGRAMS = $(addsuffix .pdf, $(DIAGRAMS))

report.pdf: $(COMPILED_DIAGRAMS) report.tex
	pdflatex -interaction nonstopmode report.tex


.PHONY: clean

clean:
	-rm report.aux report.log report.out report.pdf $(COMPILED_DIAGRAMS)
