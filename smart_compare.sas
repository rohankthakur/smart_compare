/* builds on PROC COMPARE to reduce columns, rows etc and summarise changes per key item */
%macro SmartCompare(libref1=,dsn1=,libref2=,dsn2=,keyname=,excl_cols=);
       /*%SmartCompare(
              libref1=work, 
              dsn1=temp_test_1_old, 
              libref2=work, 
              dsn2=temp_test_1_lat, 
              keyname = COMP_TIMESTAMP, 
              excl_cols=('_OBS_' '_TYPE_' 'COMP_TIMESTAMP' 'OLD_LATEST_NEW_TERMIN')
       );
       */
       *;%macro a;%mend a;
       %let rc1=%sysfunc(open(&libref1..&dsn1,i));
       %let nobs1=%sysfunc(attrn(&rc1,NOBS));
       %let close1=%sysfunc(CLOSE(&rc1));
       %let is_there1=%sysfunc(Exist(&libref1..&dsn1));
 
       %let rc2=%sysfunc(open(&libref2..&dsn2,i));
       %let nobs2=%sysfunc(attrn(&rc2,NOBS));
       %let close2=%sysfunc(CLOSE(&rc2));
       %let is_there2=%sysfunc(Exist(&libref2..&dsn2));
 
       %if (&nobs1 ne 0 and %sysfunc(Exist(&libref1..&dsn1))) and  (&nobs2 ne 0 and %sysfunc(Exist(&libref2..&dsn2))) %then %do;
       %let syscc = 0;
       /* print when exists with non-zero row count */
              proc sort data=&libref1..&dsn1; by &keyname; run;
              proc sort data=&libref2..&dsn2; by &keyname; run;
              proc compare base=&libref1..&dsn1 compare=&libref2..&dsn2 OUT=work.diff_b OUTNOEQUAL /*OUTBASE OUTCOMP*/ OUTDIF NOPRINT; id &keyname; run;
              /* select CHAR columns */
                     proc sql;
                     create table work.contents as
                     select monotonic() as start, NAME from dictionary.columns where libname=%upcase("work") and memname=%upcase("DIFF_B") and (format like '$%' or type = 'char');
                     quit;
              /* make macro var with a list of columns */
                     proc sql noprint;
                     select name into :varlist separated by ' ' from work.contents where name not in &excl_cols order by start;
                     quit;
              /* make macro var with a list of corresponding counter columns */
                     proc sql noprint;
                     select trim(name) || '_2' into :varlist2 separated by ' ' from work.contents where name not in &excl_cols order by start;
                     quit;
              /* create a format for column names */
                     proc sql;
                     create table work.contents_format as
                     select monotonic() as start, 'get_col_name' as fmtname, NAME as label, 'n' as type from work.contents where name not in &excl_cols order by start;
                     quit;
                     proc format library=work.formats cntlin=work.contents_format; run;
                     proc format library=work.formats fmtlib; select get_col_name; run;
                     /* Get number of changes per keyname and all changed columns per keyname */
                           data work.count_changes_pre ;
                             set work.diff_b;
                             length changes $ 8000;
                             array vi{*} &varlist ;
                             changes = '';
                             num_changes = 0;
                                    do i = 1 to dim(vi) ;
                                      if index(vi{i}, 'X') ge 1 then 
                                                do;
                                                       if changes = '' then do; changes = put(i,$get_col_name.); num_changes = num_changes + 1; end;
                                                       else do; changes = catx(', ',changes,put(i,$get_col_name.)); num_changes = num_changes + 1; end;
                                                end;
                                    end ;
                             output work.count_changes_pre;
                             drop i;
                           run;
                           /**/
                           %let counts = %eval(%sysfunc(count(%cmpres(&keyname),%str( )))+1);
                           %global new_keyname;
                           data _null_;
                                  length full_piece $ 200;
                                  full_piece = '';     
                                  do i = 1 to &counts;
                                         full_piece = catx(', ',full_piece, scan("&keyname",i,' ')); 
                                  end;
                                  drop i;
                                  call symput('new_keyname',full_piece);
                           run;
                           /**/
                           proc sql;
                           create table work.sc_count_changes as
                           select &new_keyname, trim(put(num_changes,4.) || ' changes, ' || changes) as Description
                           from work.count_changes_pre;
                           quit;
                     /* Check columns that have changes and keep only those columns in result view */
                           data work.limited_output_pre_a;
                             set work.diff_b end=eof ;
                             array vi{*} &varlist ; /* incoming values */
                             array vc{*} &varlist2 ; /* counters */
                             retain vc . ;
                                    do i = 1 to dim(vi) ;
                                      if index(vi{i}, 'X') ge 1 then vc{i} + 1 ;
                                    end ;
                             if eof then output ;
                             keep &varlist2 ;
                           run;
                            proc transpose data=work.limited_output_pre_a out=work.limited_output_pre_b (Rename= (_NAME_ = NAME COL1=COUNT)); run;
                           proc sql noprint;
                           select substr(name,1,length(name)-2)into :shortlist separated by ' '
                           from work.limited_output_pre_b where count > 0;
                           quit;
                           data work.sc_limited_output (keep= &keyname &shortlist);
                           informat &keyname &shortlist;
                           set work.diff_b ;
                           run;
                           /* check for errors and delete temp datasets etc */
                           %if &syscc < 10 %then %do;
                                  proc datasets library=work memtype=data;
                                         delete diff_b;
                                         delete contents;
                                         delete contents_format;
                                         delete count_changes_pre;
                                         delete limited_output_pre_a;
                                         delete limited_output_pre_b;
                                  run;
                                  proc catalog catalog=work.formats;
                                  delete get_col_name.format;
                                  quit;
                           %end;
                           %else %if  &syscc > 10 %then %do;
                                  %put Warning! Temp files not deleted as there were errors detected.;
                           %end;
       %end;
       %else %if %sysfunc(Exist(&libref1..&dsn1)) = 0 %then %do;
              /* print when dataset does not exist */
       %end;
       %else %if &nobs1 = 0 and %sysfunc(Exist(&libref1..&dsn1)) %then %do;
              /* print when dataset exists with zero row count */
       %end;
%mend SmartCompare;