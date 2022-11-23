/* Run like this:
   csvsql --blanks --tables foo --query golden_transcriptions/queries/phase1_blanks_only.sql output/joined.csv | paste -sd ':\n'
*/

/* Blanks on Problem rows that are not also an Unresolved */
select count(Problems) as "Blanks on rows that have no other problems" from foo where Problems='Blank(s)';
