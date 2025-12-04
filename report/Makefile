%.mmd.pdf: %.mmd
	mmdc --pdfFit --input $^ --outputFormat pdf --output $@

DIAGRAMS = $(wildcard *.mmd)
COMPILED_DIAGRAMS = $(addsuffix .pdf, $(DIAGRAMS))

report.pdf: $(COMPILED_DIAGRAMS) report.tex
	latexmk -pdf -pdflatex="pdflatex -interaction nonstopmode" report.tex

report-draft.pdf: $(COMPILED_DIAGRAMS) report.tex
	latexmk -pdf -pdflatex="pdflatex -interaction nonstopmode" -jobname=report-draft -usepretex="\def\mmCfgTodos{}" report.tex

report-nofigures.pdf: $(COMPILED_DIAGRAMS) report.tex
	latexmk -pdf -pdflatex="pdflatex -interaction nonstopmode" -jobname=report-nofigures -usepretex="\def\mmCfgNofigures{}" report.tex

.PHONY: clean clean-diagrams clean-latex

clean-report:
	-rm report.aux report.log report.out report.pdf

clean-diagrams:
	-rm $(COMPILED_DIAGRAMS)

clean: clean-report clean-diagrams
