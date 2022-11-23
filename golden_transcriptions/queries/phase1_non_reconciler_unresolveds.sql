/* Run like this:
   csvsql --blanks --tables foo --query golden_transcriptions/queries/phase1_non_reconciler_unresolveds.sql output/joined.csv | paste -sd ':\n'
*/

/* Unresolveds on rows that have no reconciler failures */
select count(Problems) as "Unresolveds on rows with no reconciler failures (should be transcriptionisms only)" from foo where (Problems like '%nresolved%') and not (
  ("admission number"                                           regexp "^[^\n]*\n-{10}\n") or
  ("date of entry"                                              regexp "^[^\n]*\n-{10}\n") or
  ("name"                                                       regexp "^[^\n]*\n-{10}\n") or
  ("quality"                                                    regexp "^[^\n]*\n-{10}\n") or
  ("age"                                                        regexp "^[^\n]*\n-{10}\n") or
  ("place of birth"                                             regexp "^[^\n]*\n-{10}\n") or
  ("port sailed out of"                                         regexp "^[^\n]*\n-{10}\n") or
  ("years at sea"                                               regexp "^[^\n]*\n-{10}\n") or
  ("last services"                                              regexp "^[^\n]*\n-{10}\n") or
  ("under what circumstances admitted (or nature of complaint)" regexp "^[^\n]*\n-{10}\n") or
  ("date of discharge"                                          regexp "^[^\n]*\n-{10}\n") or
  ("how disposed of"                                            regexp "^[^\n]*\n-{10}\n") or
  ("number of days victualled"                                  regexp "^[^\n]*\n-{10}\n")
);
