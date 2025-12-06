%.mmd.pdf: %.mmd
	mmdc --pdfFit --input $^ --outputFormat pdf --output $@

DIAGRAMS = $(wildcard *.mmd)
COMPILED_DIAGRAMS = $(addsuffix .pdf, $(DIAGRAMS))
REPORT_TEX_SUBFILES = $(wildcard sections/*.tex)

report.pdf: $(COMPILED_DIAGRAMS) report.tex $(REPORT_TEX_SUBFILES) sources.bib
	latexmk -pdf -pdflatex="pdflatex -interaction nonstopmode" report.tex

report-draft.pdf: $(COMPILED_DIAGRAMS) report.tex $(REPORT_TEX_SUBFILES) sources.bib
	latexmk -pdf -pdflatex="pdflatex -interaction nonstopmode" -jobname=report-draft -usepretex="\def\mmCfgTodos{}" report.tex

report-nofigures.pdf: $(COMPILED_DIAGRAMS) report.tex $(REPORT_TEX_SUBFILES) sources.bib
	latexmk -pdf -pdflatex="pdflatex -interaction nonstopmode" -jobname=report-nofigures -usepretex="\def\mmCfgNofigures{}" report.tex

.PHONY: clean clean-diagrams clean-latex

clean-report:
	latexmk -C report.tex
	latexmk -C report.tex -jobname=report-draft
	latexmk -C report.tex -jobname=report-nofigures
	-find ./ \( -name '*.bbl' -or -name '*.tdo' \) -delete

clean-diagrams:
	-rm $(COMPILED_DIAGRAMS)

clean: clean-report clean-diagrams
