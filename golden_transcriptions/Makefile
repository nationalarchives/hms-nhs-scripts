COLS = admission_number date_of_entry name quality age place_of_birth port_sailed_out_of years_at_sea last_services \
       under_what_circumstances_admitted_LBor_nature_of_complaintRB date_of_discharge how_disposed_of number_of_days_victualled
golden_targets = $(addprefix GOLDEN_, $(COLS))
output_targets = $(addprefix OUTPUT_, $(COLS))
comma = ,
lb = (
rb = )
outdir = output
separator = ^

.PHONY: all
all: $(COLS)
	@:

clean:
	rm -f GOLDEN OUTPUT $(patsubst %,'%',$(golden_targets) $(output_targets))

$(COLS): $(golden_targets) $(output_targets)

#Split golden into per-column files
$(golden_targets): GOLDEN_%: GOLDEN
	csvsql --no-inference --tables foo --query 'select "volume","page","admission number",ifnull("$(subst LB,$(lb),$(subst RB,$(rb),$(subst _, ,$*)))","00") as "$(subst LB,$(lb),$(subst RB,$(rb),$(subst _, ,$*)))" from foo' $< | csvformat -U 2 > $@.tmp
	csvsql --no-inference --tables foo --query 'select "volume","page","admission number",replace("$(subst LB,$(lb),$(subst RB,$(rb),$(subst _, ,$*)))",",","") as "$(subst LB,$(lb),$(subst RB,$(rb),$(subst _, ,$*)))" from foo' $@.tmp | csvformat -U 2 > $@
	rm $@.tmp

#Split output into per-column files
$(output_targets): OUTPUT_%: OUTPUT
	csvsql --no-inference --tables foo --query 'select "volume","page","admission number",ifnull("$(subst LB,$(lb),$(subst RB,$(rb),$(subst _, ,$*)))","00") as "$(subst LB,$(lb),$(subst RB,$(rb),$(subst _, ,$*)))" from foo' $< | csvformat -U 2 > $@

#See README for explanation of what is going on here
GOLDEN: modified/DSH_1-4_Golden_Transcriptions.csv \
        modified/DSH_7_Golden_Transcriptions.csv \
        modified/DSH_12_Golden_Transcription.csv \
        modified/DSH_18_Golden_Transcription.csv
	csvtool cols 3,4,7-19 $^ | \
	csvformat -U 2 | \
	sed '1s/.*/&KEEPME/' | \
	grep -v '^"volume","page","admission number","date of entry","name","quality","age","place of birth","port sailed out of","years at sea","last services","under what circumstances admitted (or nature of complaint)","date of discharge","how disposed of","number of days victualled"$$' | \
	sed '1s/KEEPME$$//' | \
	tr '[:upper:]' '[:lower:]' | \
	sed 's/  \+/ /g' > $@

#See README for explanation of what is going on here
OUTPUT: ../$(outdir)/joined.csv
	$(if $(shell grep -F '$(separator)' $<), $(error 'Found a $(separator), need a different line ending char'))

	csvsql --no-inference --tables foo --query 'select * from foo where (volume=1 and (page=1 or page=2 or page = 3)) or (volume = 2 and page=1) or (volume=3 and page = 1) or (volume=4 and page=2) or (volume=7 and page=1) or (volume=12 and page=1) or (volume=18 and page=1)' $< | \
	csvtool cols 2-16 - | \
	csvformat -M $(separator) -U 2 | \
	tr '\n$(separator)' '$(separator)\n' | \
	tr '[:upper:]' '[:lower:]' > $@
