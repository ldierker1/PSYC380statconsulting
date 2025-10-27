libname in1 "P:\QAC\qac380\Data and Codebooks\Connection\Data";
libname in2 "G:\My Drive\Files\Teaching\DATA ANALYSIS\##PSYC380 Statistical Consulting";

proc format; 

VALUE gender 
1='1. male'
2='2. female';

VALUE racegroup
1='1. Hispanic'
2='2. African American or Black'
3='3. Caucasian or White'
4='4. Other';

VALUE marital
1='1. married or civil union'
2='2. divorced, seperated widowed'
3='3. single/never married';

VALUE address
1='1. Community'
2='2. Program';

/*********************************************************************************************/
/****************************************  CLIENT ********************************************/
/*********************************************************************************************/

/*Start by merging the client and episode data sets by StudyClientID. 
Then merge other data sets to this new merged data set as needed by StudyClientID AND StudyEpisodeID.*/

proc import file="P:\QAC\qac380\Data and Codebooks\Connection\Data\ClientData.csv"
 out=work.clientdata
 dbms=csv
 replace;
run;

data clientd; set work.clientdata; /*N=3280*/
birthyear_num = input(birthyear, 4.);

if BiologicalGender='M' then gender=1;
else if BiologicalGender='F' then gender=2;

if ethnicity='Yes, Cuban' or ethnicity='Yes, Mexican, Mexican American, Chicano.' 
or ethnicity='Yes, Puerto Rican' or ethnicity='Yes, South or Central American' or 
ethnicity='Yes, another Hispanic, Latino, or Spanish origin' 
or ethnicity='Yes, of Hispanic/Latino Origin' then Hispanic=1;
else Hispanic=0;

/*too few Hispanic to break into two groups HB HW*/
if Hispanic=1 then RaceGroup=1;
else if Race='African American or Black' then RaceGroup=2;
else if Race='Caucasian or White' then RaceGroup=3;
else if Race='American Indian or Alaskan' or Race='Asian' or Race='Multi-Racial' or Race='Native Hawaiian/Other Pacif'
or Race='Other Pacific Islander' or Race='Some other race' then RaceGroup=4;
else if Race='Undisclosed' or Race='Not on file' then RaceGroup=.;

If MaritalStatus='Civil Union' or MaritalStatus='Married' then Marital=1;
else if MaritalStatus='Divorced/Annulled' or MaritalStatus='Legally separated' 
or MaritalStatus='Widow/widower' then Marital=2;
else if MaritalStatus='Single/Never Married' then Marital=3;
else if MaritalStatus='Not Specified' or MaritalStatus='Other' then Marital=.;

format gender gender. racegroup racegroup. marital marital.;

drop birthyear GenderIdentity IdentifiesAsLGBTQ IdentifiesAsTransGender Religion PrimaryLanguage UsVeteran maritalstatus race
hispanic BiologicalGender;
proc sort; by studyclientid;

/*proc freq; tables birthyear_num gender RaceGroup Marital; run;*/

proc import file="P:\QAC\qac380\Data and Codebooks\Connection\Data\ClientAddressData.csv"
 out=work.clientaddress
 dbms=csv
 replace;
run;

data address; set work.clientaddress; /*N=3775*/
length zip $7.;
proc sort; by studyclientid;

data addressfirstid; set work.address; by studyclientid; if first.studyclientid; /*get rid of duplicates*/ /*N=3775*/
if AddressType='Community' then admitaddress=1;
else if AddressType='Program' then admitaddress=2;
format admitaddress address.;
drop city AddressType; 
proc sort; by studyclientid;

data client; merge clientd (in=a) addressfirstid; by studyclientid; if a; /*N=3280 - simple duplicates in address file*/
proc sort; by studyclientid;

/*********************************************************************************************/
/***************************************  EPISODE ********************************************/
/*********************************************************************************************/

proc import file="P:\QAC\qac380\Data and Codebooks\Connection\Data\EpisodeData.csv"
 out=work.episode
 dbms=csv
 replace;
run;

data episode; set work.episode; /*1204 episodes of REACH program - 1084 unique clients*/
AgeAtAdmissionN = input(AgeAtAdmission, best12.);
AdmissionYearN = input(AdmissionYear, best12.);
DischargeYearN = input(DischargeYear, best12.);
LengthofStayN = input(LengthofStay, best12.);
*HighestGradeCompletedAtAdmissiN = input(HighestGradeCompletedAtAdmissio, best12.);

if DischargeStatus='Successful' then success=1; 
else success=0;

drop AgeAtAdmission AdmissionYear DischargeYear LengthofStay HighestGradeCompletedAtAdmissio BehavioralHealthNeed
DxObtainedFrom DxDateReceivedFromSource DxSource HighestGradeCompletedAtAdmissio IncomeSourceAtAdmission
IncomeSourceAtDischarge HomeTownCity HomeTownZip;
if ProgramCode='A-CB00-119';
proc sort; by studyclientid studyepisodeid;

proc freq; run; 

data clientepisode; merge client /*does not have episodeID*/ episode (in=a); by studyclientid; if a;
rename zip=zipcodemerge;
proc sort; by studyclientid studyepisodeid;

/*proc freq; tables AgeAtAdmissionN AdmissionYearN DischargeYearN LengthofStayN 
HighestGradeCompletedAtAdmissiN DischargeStatus; run; */

/*********************************************************************************************/
/***************************************  EXTERNAL *******************************************/
/*********************************************************************************************/

proc import file="G:\My Drive\Files\Teaching\DATA ANALYSIS\##PSYC380 Statistical Consulting\population_density.csv"
 out=work.ctdensity
 dbms=csv
 replace;
run;

data density; set work.ctdensity;
length zipcodemerge $7.;
informat zipcodemerge $7.;
format zipcodemerge $7.;
    zipcodemerge = cats('0', put(zip, best.));
	drop zip lat lng county_fips;
proc sort; by zipcodemerge;

proc import file="G:\My Drive\Files\Teaching\DATA ANALYSIS\##PSYC380 Statistical Consulting\connecticut_rent.csv"
 out=work.ctrent
 dbms=csv
 replace;
run;

data rent; set work.ctrent;
length zipcodemerge $7.;
informat zipcodemerge $7.;
format zipcodemerge $7.;
zip5 = tranwrd(zipcode1, 'zip/', '');
zipcodemerge = substr(compress(zip5, , 'kd'), 1, 5);
drop zipcode1 zipcode2 zip5;
if rentsourceyear=2022; /*could also subset to 2021*/
proc sort; by zipcodemerge;

proc freq; run; 

data densityrent; merge density rent; by zipcodemerge; 
proc sort; by zipcodemerge;

data new1; set clientepisode; 
length zipcodemerge $7.;
informat zipcodemerge $7.;
format zipcodemerge $7.;
proc sort; by zipcodemerge;

data new2; merge new1 (in=a) densityrent; by zipcodemerge; if a;  /*N=1155 since 49 episodes had no density*/
proc sort; by studyclientid studyepisodeid; 					  /*N=1090 since 114 episodes had no rent*/

/*proc freq; tables density Monthly_Median_GrossRent_Housing; run; 

proc print; var studyclientid studyepisodeid zipcodemerge density Monthly_Median_GrossRent_Housing; run;
proc contents; run; */

/**************************************************************************/
/******************************  ASUS  ***********************************/
/**************************************************************************/

proc import file="P:\QAC\qac380\Data and Codebooks\Connection\Data\ASUS_dimension_scores.csv"
 out=work.ASUS
 dbms=csv
 replace;
run;

data ASUS1; set work.ASUS;
*KEEP studyclientid studyepisodeid AOD_INVOLVEMENT1 AOD_DISRUPTION1 SOCIAL_NON_CONFORMING MOOD_ADJUSTMENT
studyclientidC studyepisodeidC;
length studyclientidC $4. studyepisodeidC $6.;
format studyclientidC $4. studyepisodeidC $6.;
informat studyclientidC $4. studyepisodeidC $6.;
studyclientidC = strip(put(studyclientid, best12.));
studyepisodeidC = strip(put(studyepisodeid, best12.)); 
drop studyclientid studyepisodeid q011-q096 q001a--q064b;
proc sort; by studyclientidC studyepisodeidC; 

data ASUS2; set ASUS1; 
rename studyclientidC=studyclientid;
rename studyepisodeidC=studyepisodeid;
proc sort; by studyclientid studyepisodeid; 

data ASUSepisode; merge new2(in=a) ASUS2; by studyclientid studyepisodeid; if a; /*N=961 with ASUS scores*/
proc sort; by studyclientid studyepisodeid; 

proc freq; /*tables studyclientid studyepisodeid 
AOD_INVOLVEMENT1 AOD_DISRUPTION1 SOCIAL_NON_CONFORMING MOOD_ADJUSTMENT;*/
run;

proc freq; tables studyclientid studyepisodeid AOD_INVOLVEMENT1 AOD_DISRUPTION1 SOCIAL_NON_CONFORMING MOOD_ADJUSTMENT; run; 

proc print; var studyclientid studyepisodeid AOD_INVOLVEMENT1 AOD_DISRUPTION1 SOCIAL_NON_CONFORMING MOOD_ADJUSTMENT
density Monthly_Median_GrossRent_Housing; run; 

/*data clientepiASUS; merge new2 (in=a) ASUS2 (in=b); by studyclientid studyepisodeid; 
 in_new2 = a;
 in_ASUS2 = b;
 if a and b then flag = 'Both         ';
    else if a and not b then flag = 'Only_in_1';
    else if b and not a then flag = 'Only_in_2';
proc sort; by studyclientid studyepisodeid;*/

/*proc import file="P:\QAC\qac380\Data and Codebooks\Connection\Data\ClientServiceData.csv"
 out=work.service
 dbms=csv
 replace;
run;

data service; set work.service; 
proc sort; by studyepisodeid;

data clientepiservice; merge clientepisode service; by studyepisodeid;
proc sort; by studyclientid;

proc freq; run;*/

run;
libname in1 "P:\QAC\qac380\Data and Codebooks\Connection\Data";
libname in2 "G:\My Drive\Files\Teaching\DATA ANALYSIS\##PSYC380 Statistical Consulting";

proc format; 

VALUE gender 
1='1. male'
2='2. female';

VALUE racegroup
1='1. Hispanic'
2='2. African American or Black'
3='3. Caucasian or White'
4='4. Other';

VALUE marital
1='1. married or civil union'
2='2. divorced, seperated widowed'
3='3. single/never married';

VALUE address
1='1. Community'
2='2. Program';

/*********************************************************************************************/
/****************************************  CLIENT ********************************************/
/*********************************************************************************************/

/*Start by merging the client and episode data sets by StudyClientID. 
Then merge other data sets to this new merged data set as needed by StudyClientID AND StudyEpisodeID.*/

proc import file="P:\QAC\qac380\Data and Codebooks\Connection\Data\ClientData.csv"
 out=work.clientdata
 dbms=csv
 replace;
run;

data clientd; set work.clientdata; /*N=3280*/
birthyear_num = input(birthyear, 4.);

if BiologicalGender='M' then gender=1;
else if BiologicalGender='F' then gender=2;

if ethnicity='Yes, Cuban' or ethnicity='Yes, Mexican, Mexican American, Chicano.' 
or ethnicity='Yes, Puerto Rican' or ethnicity='Yes, South or Central American' or 
ethnicity='Yes, another Hispanic, Latino, or Spanish origin' 
or ethnicity='Yes, of Hispanic/Latino Origin' then Hispanic=1;
else Hispanic=0;

/*too few Hispanic to break into two groups HB HW*/
if Hispanic=1 then RaceGroup=1;
else if Race='African American or Black' then RaceGroup=2;
else if Race='Caucasian or White' then RaceGroup=3;
else if Race='American Indian or Alaskan' or Race='Asian' or Race='Multi-Racial' or Race='Native Hawaiian/Other Pacif'
or Race='Other Pacific Islander' or Race='Some other race' then RaceGroup=4;
else if Race='Undisclosed' or Race='Not on file' then RaceGroup=.;

If MaritalStatus='Civil Union' or MaritalStatus='Married' then Marital=1;
else if MaritalStatus='Divorced/Annulled' or MaritalStatus='Legally separated' 
or MaritalStatus='Widow/widower' then Marital=2;
else if MaritalStatus='Single/Never Married' then Marital=3;
else if MaritalStatus='Not Specified' or MaritalStatus='Other' then Marital=.;

format gender gender. racegroup racegroup. marital marital.;

drop birthyear GenderIdentity IdentifiesAsLGBTQ IdentifiesAsTransGender Religion PrimaryLanguage UsVeteran maritalstatus race
hispanic BiologicalGender;
proc sort; by studyclientid;

/*proc freq; tables birthyear_num gender RaceGroup Marital; run;*/

proc import file="P:\QAC\qac380\Data and Codebooks\Connection\Data\ClientAddressData.csv"
 out=work.clientaddress
 dbms=csv
 replace;
run;

data address; set work.clientaddress; /*N=3775*/
length zip $7.;
proc sort; by studyclientid;

data addressfirstid; set work.address; by studyclientid; if first.studyclientid; /*get rid of duplicates*/ /*N=3775*/
if AddressType='Community' then admitaddress=1;
else if AddressType='Program' then admitaddress=2;
format admitaddress address.;
drop city AddressType; 
proc sort; by studyclientid;

data client; merge clientd (in=a) addressfirstid; by studyclientid; if a; /*N=3280 - simple duplicates in address file*/
proc sort; by studyclientid;

/*********************************************************************************************/
/***************************************  EPISODE ********************************************/
/*********************************************************************************************/

proc import file="P:\QAC\qac380\Data and Codebooks\Connection\Data\EpisodeData.csv"
 out=work.episode
 dbms=csv
 replace;
run;

data episode; set work.episode; /*1204 episodes of REACH program - 1084 unique clients*/
AgeAtAdmissionN = input(AgeAtAdmission, best12.);
AdmissionYearN = input(AdmissionYear, best12.);
DischargeYearN = input(DischargeYear, best12.);
LengthofStayN = input(LengthofStay, best12.);
*HighestGradeCompletedAtAdmissiN = input(HighestGradeCompletedAtAdmissio, best12.);

if DischargeStatus='Successful' then success=1; 
else success=0;

drop AgeAtAdmission AdmissionYear DischargeYear LengthofStay HighestGradeCompletedAtAdmissio BehavioralHealthNeed
DxObtainedFrom DxDateReceivedFromSource DxSource HighestGradeCompletedAtAdmissio IncomeSourceAtAdmission
IncomeSourceAtDischarge HomeTownCity HomeTownZip;
if ProgramCode='A-CB00-119';
proc sort; by studyclientid studyepisodeid;

proc freq; run; 

data clientepisode; merge client /*does not have episodeID*/ episode (in=a); by studyclientid; if a;
rename zip=zipcodemerge;
proc sort; by studyclientid studyepisodeid;

/*proc freq; tables AgeAtAdmissionN AdmissionYearN DischargeYearN LengthofStayN 
HighestGradeCompletedAtAdmissiN DischargeStatus; run; */

/*********************************************************************************************/
/***************************************  EXTERNAL *******************************************/
/*********************************************************************************************/

proc import file="G:\My Drive\Files\Teaching\DATA ANALYSIS\##PSYC380 Statistical Consulting\population_density.csv"
 out=work.ctdensity
 dbms=csv
 replace;
run;

data density; set work.ctdensity;
length zipcodemerge $7.;
informat zipcodemerge $7.;
format zipcodemerge $7.;
    zipcodemerge = cats('0', put(zip, best.));
	drop zip lat lng county_fips;
proc sort; by zipcodemerge;

proc import file="G:\My Drive\Files\Teaching\DATA ANALYSIS\##PSYC380 Statistical Consulting\connecticut_rent.csv"
 out=work.ctrent
 dbms=csv
 replace;
run;

data rent; set work.ctrent;
length zipcodemerge $7.;
informat zipcodemerge $7.;
format zipcodemerge $7.;
zip5 = tranwrd(zipcode1, 'zip/', '');
zipcodemerge = substr(compress(zip5, , 'kd'), 1, 5);
drop zipcode1 zipcode2 zip5;
if rentsourceyear=2022; /*could also subset to 2021*/
proc sort; by zipcodemerge;

proc freq; run; 

data densityrent; merge density rent; by zipcodemerge; 
proc sort; by zipcodemerge;

data new1; set clientepisode; 
length zipcodemerge $7.;
informat zipcodemerge $7.;
format zipcodemerge $7.;
proc sort; by zipcodemerge;

data new2; merge new1 (in=a) densityrent; by zipcodemerge; if a;  /*N=1155 since 49 episodes had no density*/
proc sort; by studyclientid studyepisodeid; 					  /*N=1090 since 114 episodes had no rent*/

/*proc freq; tables density Monthly_Median_GrossRent_Housing; run; 

proc print; var studyclientid studyepisodeid zipcodemerge density Monthly_Median_GrossRent_Housing; run;
proc contents; run; */

/**************************************************************************/
/******************************  ASUS  ***********************************/
/**************************************************************************/

proc import file="P:\QAC\qac380\Data and Codebooks\Connection\Data\ASUS_dimension_scores.csv"
 out=work.ASUS
 dbms=csv
 replace;
run;

data ASUS1; set work.ASUS;
*KEEP studyclientid studyepisodeid AOD_INVOLVEMENT1 AOD_DISRUPTION1 SOCIAL_NON_CONFORMING MOOD_ADJUSTMENT
studyclientidC studyepisodeidC;
length studyclientidC $4. studyepisodeidC $6.;
format studyclientidC $4. studyepisodeidC $6.;
informat studyclientidC $4. studyepisodeidC $6.;
studyclientidC = strip(put(studyclientid, best12.));
studyepisodeidC = strip(put(studyepisodeid, best12.)); 
drop studyclientid studyepisodeid q011-q096 q001a--q064b;
proc sort; by studyclientidC studyepisodeidC; 

data ASUS2; set ASUS1; 
rename studyclientidC=studyclientid;
rename studyepisodeidC=studyepisodeid;
proc sort; by studyclientid studyepisodeid; 

data ASUSepisode; merge new2(in=a) ASUS2; by studyclientid studyepisodeid; if a; /*N=961 with ASUS scores*/
proc sort; by studyclientid studyepisodeid; 

proc freq; /*tables studyclientid studyepisodeid 
AOD_INVOLVEMENT1 AOD_DISRUPTION1 SOCIAL_NON_CONFORMING MOOD_ADJUSTMENT;*/
run;

proc freq; tables studyclientid studyepisodeid AOD_INVOLVEMENT1 AOD_DISRUPTION1 SOCIAL_NON_CONFORMING MOOD_ADJUSTMENT; run; 

proc print; var studyclientid studyepisodeid AOD_INVOLVEMENT1 AOD_DISRUPTION1 SOCIAL_NON_CONFORMING MOOD_ADJUSTMENT
density Monthly_Median_GrossRent_Housing; run; 

/*data clientepiASUS; merge new2 (in=a) ASUS2 (in=b); by studyclientid studyepisodeid; 
 in_new2 = a;
 in_ASUS2 = b;
 if a and b then flag = 'Both         ';
    else if a and not b then flag = 'Only_in_1';
    else if b and not a then flag = 'Only_in_2';
proc sort; by studyclientid studyepisodeid;*/

/*proc import file="P:\QAC\qac380\Data and Codebooks\Connection\Data\ClientServiceData.csv"
 out=work.service
 dbms=csv
 replace;
run;

data service; set work.service; 
proc sort; by studyepisodeid;

data clientepiservice; merge clientepisode service; by studyepisodeid;
proc sort; by studyclientid;

proc freq; run;*/

run;
