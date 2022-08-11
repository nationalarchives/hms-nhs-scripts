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
	csvsql --no-inference --tables foo --query 'select subject,"admission number",ifnull("$(subst LB,$(lb),$(subst RB,$(rb),$(subst _, ,$*)))","00") as "$(subst LB,$(lb),$(subst RB,$(rb),$(subst _, ,$*)))" from foo' $< | csvformat -U 2 > $@.tmp
	csvsql --no-inference --tables foo --query 'select subject,"admission number",replace("$(subst LB,$(lb),$(subst RB,$(rb),$(subst _, ,$*)))",",","") as "$(subst LB,$(lb),$(subst RB,$(rb),$(subst _, ,$*)))" from foo' $@.tmp | csvformat -U 2 > $@
	rm $@.tmp

#Split output into per-column files
$(output_targets): OUTPUT_%: OUTPUT
	csvsql --no-inference --tables foo --query 'select subject,"admission number",ifnull("$(subst LB,$(lb),$(subst RB,$(rb),$(subst _, ,$*)))","00") as "$(subst LB,$(lb),$(subst RB,$(rb),$(subst _, ,$*)))" from foo' $< | csvformat -U 2 > $@

#See README for explanation of what is going on here
GOLDEN: modified/DSH_1-4_Golden_Transcriptions.csv \
        modified/DSH_7_Golden_Transcriptions.csv \
        modified/DSH_12_Golden_Transcription.csv \
        modified/DSH_18_Golden_Transcription.csv
	csvtool cols 1,3,4,7-19 $^ | \
	sed '1s/^subject_id,/subject,/' | \
	csvformat -U 2 | \
	grep -v '^"subject_id","volume","page","admission number","date of entry","name","quality","age","place of birth","port sailed out of","years at sea","last services","under what circumstances admitted (or nature of complaint)","date of discharge","how disposed of","number of days victualled"$$' | \
	tr '[:upper:]' '[:lower:]' | \
	sed 's/  \+/ /g' > $@

#See README for explanation of what is going on here
OUTPUT: ../$(outdir)/joined.csv
	$(if $(shell grep -F '$(separator)' $<), $(error 'Found a $(separator), need a different line ending char'))

	csvsql --no-inference --tables foo --query 'select * from foo where raw_subject=44121904 or raw_subject=44121906 or raw_subject=44121907 or raw_subject=44290143 or raw_subject=44293717 or raw_subject=44377680 or raw_subject=44398522 or raw_subject=44431354 or raw_subject=44444317' $< | \
	csvtool cols 18,2,3,5-17 - | \
	sed '1s/^raw_subject,/subject,/' | \
	csvformat -M $(separator) -U 2 | \
	tr '\n$(separator)' '$(separator)\n' | \
	tr '[:upper:]' '[:lower:]' > $@
