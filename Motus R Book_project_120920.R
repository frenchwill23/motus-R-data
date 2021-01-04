# William Blake - Motus
# 10/28/2020 --> Moved all R data into "Prior Data" subfolder and re-uploading datasets to HOPE we are able to integrate CTT data!!
# Post-processing data from Motus with ALLTAGS_FAST
# Use this code in combination with "Motus R Book Filter213_100620.R"
# You can scroll to latest df.selecttagsFILTER RDS

###################################################################################################
## Deletion Technique update
#update.packages()
#remove.packages("motus")
#install.packages("remotes")              ## if you haven't already done this
#library(remotes)
#install_github("MotusWTS/motus")
#library(motus)
#remove.packages("RSQLite")
#install.packages("RSQLite")
#library(RSQLite)
#remove.packages("DBI")
#install.packages("DBI")
#library(DBI)

# install motusData package which contains sample datasets, e.g., vanishBearing
# used in Chapter 7
#install_github("MotusWTS/motusData")

# force a re-installation of motus package in case of required updates
#install_github("MotusWTS/motus", force = TRUE)

#packageVersion("motus")
###################################################################################################
## Installing Packages
#function to install and load required packages
ipak <- function(pkg){
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg)) 
    install.packages(new.pkg, dependencies = TRUE)
  sapply(pkg, require, character.only = TRUE)
}

#load or install these packages:
packages <- c("motus","lubridate","maps","tidyverse","rworldmap","ggmap","DBI", "RSQLite")
ipak(packages)

###################################################################################################
#Set New Directory
mac<-"~/Egnyte/Private/wblake/MPG Research/Motus/4_Analyses/R data"
setwd(mac)
###################################################################################################

Sys.setenv(TZ = "UTC")
#Download data for the first time

###################################################################################################
## IF NO INTERNET
# df.goodtags <- readRDS("./df.receiver_goodtags_110419.RDS")
#IF INTERNET OR MEMORY ISSUE YOU CAN TRY motusLogout().
###################################################################################################

##09/28/20: Still issues with memory exhausted limits. Tried to fix it but didn't work?
#Check for memory size limits :

##########
##########
#gc()
##########
##########

#Sys.setenv('R_MAX_VSIZE'=32000000000)
#REMOVE ALL VARIALBLES THAT ARE TEMPORARY OR TOO LARGE IF MEMORY USAGE TOO BIG:
#rm(large_df, large_list, large_vector, temp_variables)
#CHECK terminal?

## IF INTERNET ACCESS

#By Project:
proj.num <- 213 # IWC birds
#src1 <- tagme(proj.num, new=T, update=T, forceMeta = T) # On 10/22: force meta to amend big changes in projects 213, 226, and 352.
src1 <- tagme(proj.num, update=T)
#newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
#DBI::dbSendQuery(src1$con, newview)
#tbl.alltags1 <- tbl(src1, "alltags_fast") # virtual table
tbl.alltags1 <-tbl(src1,"alltags")
df.alltags1 <- tbl.alltags1 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num <- 226 # IWC bats
#src2 <- tagme(proj.num, new=T, update=T, forceMeta = T) # On 10/22: force meta to amend big changes in projects 213, 226, and 352.
src2 <- tagme(proj.num, update=T)
#newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
#DBI::dbSendQuery(src2$con, newview)
#tbl.alltags2 <- tbl(src2, "alltags_fast") # virtual table
tbl.alltags2 <-tbl(src2,"alltags")
df.alltags2 <- tbl.alltags2 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num <- 352 # IWC UM research
#src3 <- tagme(proj.num, new=T, update=T, forceMeta = T) # On 10/22: force meta to amend big changes in projects 213, 226, and 352.
src3 <- tagme(proj.num, update=T)
#newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
#DBI::dbSendQuery(src3$con, newview)
#tbl.alltags3 <- tbl(src3, "alltags_fast") # virtual table
tbl.alltags3 <-tbl(src3,"alltags")
df.alltags3 <- tbl.alltags3 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

############################
############################

# If just one project: 
df.alltags <- df.alltags1
# Or else: Bind all prokect rows:
df.alltags <- bind_rows(df.alltags1,df.alltags2, df.alltags3)
#View(df.alltags)

###################################################################################################
###################################################################################################
### SKIP EVERYTHING UNTIL NEXT DOUBLE HASHTAG LINES if not interested in going one-by-one for each receiver.

### BY RECEIVERS:

# Inactive receivers
# Active receivers
# MPG Ranch
# Bitterroot
# Other Montana sites
# Idaho sites
# Oregon sites
# California sites
# Wisconsin sites
# Mexico sites

proj.num1 <- "SG-6F3BRPI3EA63" # Indian Ridge  (twice)
src1 <- tagme(proj.num1, new=F, update=T, forceMeta =T)
tbl.alltags1 <- tbl(src1, "alltags") # virtual table
df.alltags1 <- tbl.alltags1 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num14 <- "CTT-5031194D3168" # IndianRidge3-MPG-MT
sql.motus14 <- tagme(proj.num14, new = F,  update=TRUE, dir = "~/Egnyte/Private/wblake/MPG Research/Motus/4_Analyses/R data")
tbl.alltags14 <- tbl(sql.motus14, "alltags")
df.alltags14 <- tbl.alltags14 %>% 
  collect() %>% 
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num2 = "SG-04EBRPI3F784" # BreunerLab # Batmobile Cliffs # Miller Ridge
src2 <- tagme(proj.num2, new=T, update=T, forceMeta =T)
#newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
#DBI::dbSendQuery(src2$con, newview)
tbl.alltags2 <- tbl(src2, "alltags") # virtual table
df.alltags2 <- tbl.alltags2 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num3 = "SG-8550RPI3768F" # Miller Ridge # Miller Creek # Pump Slough
src3 <- tagme(proj.num3, new=T, update=T, forceMeta =T)
#newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
#DBI::dbSendQuery(src3$con, newview)
tbl.alltags3 <- tbl(src3, "alltags") # virtual table
df.alltags3 <- tbl.alltags3 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num4 = "SG-AE14RPI346CE" # Teller
src4 <- tagme(proj.num4, new=T, update=T, forceMeta =T)
#newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
#DBI::dbSendQuery(src4$con, newview)
tbl.alltags4 <- tbl(src4, "alltags") # virtual table
df.alltags4 <- tbl.alltags4 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num5 = "SG-2091RPI3BB4A" # South Baldy Ridge
src5 <- tagme(proj.num5, new=T,forceMeta = T, update = TRUE)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src5$con, newview)
tbl.alltags5 <- tbl(src5, "alltags_fast") # virtual table
df.alltags5 <- tbl.alltags5 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num6 = "SG-E5B7RPI30AB4" # Lee Metcalf
src6 <- tagme(proj.num6, new=T,forceMeta = T, update = TRUE)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src6$con, newview)
tbl.alltags6 <- tbl(src6, "alltags_fast") # virtual table
df.alltags6 <- tbl.alltags6 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num7 = "SG-5F6ERPI30C91" # UMBELmobile
src7 <- tagme(proj.num7, new=T,forceMeta = T, update = TRUE)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src7$con, newview)
tbl.alltags7 <- tbl(src7, "alltags_fast") # virtual table
df.alltags7 <- tbl.alltags7 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num8 = "SG-DD7DRPI3F777" # Lake Petite
src8 <- tagme(proj.num8, new=T,forceMeta = T, update = TRUE)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src8$con, newview)
tbl.alltags8 <- tbl(src8, "alltags_fast") # virtual table
df.alltags8 <- tbl.alltags8 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num9 = "SG-393CRPI3946E" # Lost Trail 
src9 <- tagme(proj.num9, new=T,forceMeta = T, update = TRUE)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src9$con, newview)
tbl.alltags9 <- tbl(src9, "alltags_fast") # virtual table
df.alltags9 <- tbl.alltags9 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num10 = "SG-F43CRPI363B4" # Willow Mtn
src10 <- tagme(proj.num10, new=T,forceMeta = T, update = TRUE)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src10$con, newview)
tbl.alltags10 <- tbl(src10, "alltags_fast") # virtual table
df.alltags10 <- tbl.alltags10 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num11 = "SG-FEBBRPI3AFBA" # Barb # Ginger_KBK # Sula
src11 <- tagme(proj.num11, new=T,forceMeta = T, update = TRUE)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src11$con, newview)
tbl.alltags11 <- tbl(src11, "alltags_fast") # virtual table
df.alltags11 <- tbl.alltags11 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num12 = "SG-3180RPI3AE9D" # Batmobile Floodplains
src12 <- tagme(proj.num12, new=T,forceMeta = T, update = TRUE)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src12$con, newview)
tbl.alltags12 <- tbl(src12, "alltags_fast") # virtual table
df.alltags12 <- tbl.alltags12 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num13 = "SG-C08ERPI38376" # INACTIVE Birds Of Prey Center
src13 <- tagme(proj.num13, new=T,forceMeta = T, update = TRUE)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src13$con, newview)
tbl.alltags13 <- tbl(src13, "alltags_fast") # virtual table
df.alltags13 <- tbl.alltags13 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num15 <- "CTT-39B99EDBFFFC" #Lucky Peak
src15 <- tagme(proj.num15, new=T, rename = T , update=T) #, forceMeta =T
tbl.alltags15 <- tbl(src15, "alltags") # virtual table
df.alltags15 <- tbl.alltags15 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num16 <- "CTT-F2B981B73FE3" # Birds of Prey Center 1
src16 <- tagme(proj.num16, new=T, update=T, forceMeta =T)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src16$con, newview)
tbl.alltags16 <- tbl(src16, "alltags_fast") # virtual table
df.alltags16 <- tbl.alltags16 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num17 <- "CTT-2EE41E27E49C" # The Roost
  src1 <- tagme(proj.num17, new=T, update=T, forceMeta =T)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src17$con, newview)
tbl.alltags17 <- tbl(src17, "alltags_fast") # virtual table
df.alltags17 <- tbl.alltags17 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num15 <- 
  src1 <- tagme(proj.num1, new=T, update=T, forceMeta =T)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src15$con, newview)
tbl.alltags15 <- tbl(src5, "alltags_fast") # virtual table
df.alltags15 <- tbl.alltags15 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num15 <- 
  src1 <- tagme(proj.num1, new=T, update=T, forceMeta =T)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src15$con, newview)
tbl.alltags15 <- tbl(src5, "alltags_fast") # virtual table
df.alltags15 <- tbl.alltags15 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num15 <- 
  src1 <- tagme(proj.num1, new=T, update=T, forceMeta =T)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src15$con, newview)
tbl.alltags15 <- tbl(src5, "alltags_fast") # virtual table
df.alltags15 <- tbl.alltags15 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num15 <- 
  src1 <- tagme(proj.num1, new=T, update=T, forceMeta =T)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src15$con, newview)
tbl.alltags15 <- tbl(src5, "alltags_fast") # virtual table
df.alltags15 <- tbl.alltags15 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num15 <- 
  src1 <- tagme(proj.num1, new=T, update=T, forceMeta =T)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src15$con, newview)
tbl.alltags15 <- tbl(src5, "alltags_fast") # virtual table
df.alltags15 <- tbl.alltags15 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num15 <- 
  src1 <- tagme(proj.num1, new=T, update=T, forceMeta =T)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src15$con, newview)
tbl.alltags15 <- tbl(src5, "alltags_fast") # virtual table
df.alltags15 <- tbl.alltags15 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num15 <- 
  src1 <- tagme(proj.num1, new=T, update=T, forceMeta =T)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src15$con, newview)
tbl.alltags15 <- tbl(src5, "alltags_fast") # virtual table
df.alltags15 <- tbl.alltags15 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

proj.num15 <- 
  src1 <- tagme(proj.num1, new=T, update=T, forceMeta =T)
newview <- "CREATE VIEW alltags_fast as SELECT t1.hitID as hitID, t1.runID as runID, t1.batchID as batchID, t1.ts as ts, CASE WHEN t6.utcOffset is null then t1.ts else t1.ts - t6.utcOffset * 60 * 60 end as tsCorrected, t1.sig as sig, t1.sigSD as sigsd, t1.noise as noise, t1.freq as freq, t1.freqSD as freqsd, t1.slop as slop, t1.burstSlop as burstSlop, t2.done as done, CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end as motusTagID, t12.ambigID as ambigID, t2.ant as port, t2.len as runLen, t3.monoBN as bootnum, t4.projectID as tagProjID, t4.mfgID as mfgID, t4.type as tagType, t4.codeSet as codeSet, t4.manufacturer as mfg, t4.model as tagModel, t4.lifeSpan as tagLifespan, t4.nomFreq as nomFreq, t4.bi as tagBI, t4.pulseLen as pulseLen, t5.deployID as tagDeployID, t5.speciesID as speciesID, t5.markerNumber as markerNumber, t5.markerType as markerType, t5.tsStart as tagDeployStart, t5.tsEnd as tagDeployEnd, t5.latitude as tagDepLat, t5.longitude as tagDepLon, t5.elevation as tagDepAlt, t5.comments as tagDepComments, ifnull(t5.fullID, printf('?proj?-%d#%s:%.1f', t5.projectID, t4.mfgID, t4.bi)) as fullID, t3.motusDeviceID as deviceID, t6.deployID as recvDeployID, t6.latitude as recvDeployLat, t6.longitude as recvDeployLon, t6.elevation as recvDeployAlt, t6a.serno as recv, t6.name as recvDeployName, t6.siteName as recvSiteName, t6.isMobile as isRecvMobile, t6.projectID as recvProjID, t6.utcOffset as recvUtcOffset, t7.antennaType as antType, t7.bearing as antBearing, t7.heightMeters as antHeight, t8.english as speciesEN, t8.french as speciesFR, t8.scientific as speciesSci, t8.`group` as speciesGroup, t9.label as tagProjName, t10.label as recvProjName, NULL as gpsLat, NULL as gpsLon, NULL as gpsAlt FROM hits AS t1 LEFT JOIN runs AS t2 ON t1.runID = t2.runID left join allambigs t12 on t2.motusTagID = t12.ambigID LEFT JOIN batches AS t3 ON t3.batchID = t1.batchID LEFT JOIN tags AS t4 ON t4.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end LEFT JOIN tagDeps AS t5 ON t5.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5.tsStart = (SELECT max(t5b.tsStart) FROM tagDeps AS t5b WHERE t5b.tagID = CASE WHEN t12.motusTagID is null then t2.motusTagID else t12.motusTagID end 	AND t5b.tsStart <= t1.ts AND (t5b.tsEnd IS NULL OR t5b.tsEnd >= t1.ts) ) LEFT JOIN recvs as t6a on t6a.deviceID = t3.motusDeviceID LEFT JOIN recvDeps AS t6 ON t6.deviceID = t3.motusDeviceID AND t6.tsStart = (SELECT max(t6b.tsStart) FROM recvDeps AS t6b WHERE t6b.deviceID = t3.motusDeviceID 	AND t6b.tsStart <= t1.ts AND (t6b.tsEnd IS NULL OR t6b.tsEnd >= t1.ts) ) LEFT JOIN antDeps AS t7 ON t7.deployID = t6.deployID AND t7.port = t2.ant LEFT JOIN species AS t8 ON t8.id = t5.speciesID LEFT JOIN projs AS t9 ON t9.ID = t5.projectID LEFT JOIN projs AS t10 ON t10.ID = t6.projectID"
DBI::dbSendQuery(src15$con, newview)
tbl.alltags15 <- tbl(src5, "alltags_fast") # virtual table
df.alltags15 <- tbl.alltags15 %>%
  collect() %>%
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "UTC", origin = "1970-01-01"))

############################

# Bind all receiver rows:
df.alltags <- bind_rows(df.alltags1,df.alltags2,df.alltags3,df.alltags4,df.alltags5,df.alltags6,df.alltags7,df.alltags8,df.alltags9,df.alltags10,df.alltags11,df.alltags12, df.alltags13, df.alltags14,)
#View(df.alltags)

#Optional: Remove where there is no species info in species name:
#df.alltags <- df.alltags%>%
#  filter(!is.na(speciesEN))
#Optional: Upload only one receiver in the alltags
#df.alltags <- df.alltags14

####
#View(df.alltags)
#saveRDS(df.alltags, "./df.alltags_allprojects_120920.RDS")
#saveRDS(df.alltags, "./df.alltags_allreceivers_120920.RDS")

############################

# Filter to include only tags since 9/18/18 deployment at 20:27 UTC
df.goodtags<-df.alltags%>%
  filter(runLen > 1) %>%         ## On 10/28/20 to read CTT tags do not include filter=freqsd < 0.1 & 
  ## May want to run analyses with runlength of 2 instead
  filter(ts >= ymd_hms("2018-09-18 20:27:00"))
#saveRDS(df.goodtags, "./goodtags_102820.RDS")
#View(df.goodtags)
###################################################################################################
###################################################################################################

#Select specific columns and reorder column display:
df.selecttags<-df.goodtags%>%
  select(ts, freqsd, runLen, recvProjName,motusTagID,mfgID,speciesEN,
         markerNumber,tagDeployID, tagProjID, tagProjName,tagDeployStart, tagDepLat,tagDepLon,sig,
         runLen,recvProjID,recvDeployName,recvSiteName,recvDeployLat,recvDeployLon,
         port, antBearing,
         tagType, codeSet, tagModel,pulseLen, tagLifespan, tagDeployID)
df.selecttags<-df.selecttags %>%
  mutate(tagDeployStart = as_datetime(tagDeployStart, tz = "UTC", origin = "1970-01-01"))
#View(df.selecttags)

## Make Excel sheet:
#write.csv(df.selecttags, "./selecttags_102820.csv") ## This will take a while or not work if going for the millions of lines of tag data from 213. Better to split by species, etc.
#saveRDS(df.selecttags, "./selecttags_102820.RDS")
###################################################################################################
###################################################################################################
## NOW you can use df.selecttagsFILTER code based on the R.file: "Motus R Book Filter213_MMDDYY.R"

## CSV
#write.csv(df.selecttagsFILTER, "./filteredTags213_100720.csv") ## This will take a while or not work if going for the millions of lines of tag data from 213. Better to split by species, etc.

## RDS
saveRDS(df.selecttagsFILTER, "./selecttags_filtered_AllProjects_120920.RDS")
df.selecttagsFILTER <-readRDS("./selecttags_filtered_AllProjects_102820.RDS")
View(df.selecttagsFILTER)
########################################################
########################################################
#SELECT TAGS FILTER
#To find the number of different detections for each tag:
df.detectSum <- df.selecttagsFILTER %>% ## See "Motus R Book Filter213"
  count(mfgID, tagDeployID, tagProjID, tagProjName, tagDeployStart,motusTagID, speciesEN, recvDeployName,recvProjName, recvDeployLat,recvDeployLon) %>%
  collect() %>%
  as.data.frame() 
#View(df.detectSum)
#str(df.detectSum)
#df.detectSum <- filter(df.detectSum,tagProjID == 213)  # If interested in just looking at project specific tags

########################################################


#Filter by Species
Species.detect<-df.selecttagsFILTER %>%
  filter(runLen >1) %>% # you can play here with the filtered data to reduce false positives. Try "runlen>2" and "runlen3".
  filter(speciesEN == "Lewis's Woodpecker") # It could be "Common Poorwill", "Common Nighthawk", "Turkey Vulture", etc.
#  filter(!recvDeployName=="SCBI Motus") %>%
#  filter(!recvDeployName=="Zoo New England") %>%
#  filter(recvDeployLat< 49.0) %>%
#  filter(recvDeployLon< -100)
#View(Species.detect)

Species.detect<-Species.detect %>%
  filter(recvProjName =="IWC birds")

#write.csv(Species.detect, "./Motus_LEWO_total2020.csv")

Species.detect <-Species.detect %>% # 2019 tags detected in 2020
  filter(tagDeployStart >= ymd_hms("2020-01-01 00:00:00")) %>%
  filter(ts >= ymd_hms("2020-01-01 00:00:00"))
  
## Last detections
df.detectlast<-Species.detect %>% # You can also just look up directly with Species.detect instead of df.selecttagsFILTER
  group_by(motusTagID,recvDeployName) %>%
  slice(which.max(as.Date(ts, '%Y/%m/%d %h:%m:%s')))
df.detectlast <- df.detectlast %>%
  select(ts, mfgID,recvDeployName,recvProjName,speciesEN,tagProjName,tagDepLat,tagDepLon,
         recvSiteName,recvDeployLat,recvDeployLon,recvProjName,
         motusTagID,markerNumber,sig,
         runLen,recvProjID,
         port, antBearing)
View(df.detectlast)

########################
## Investigate further by tag:
Tag.detect<-Species.detect %>%
  filter(mfgID == "62") #
#%>%
#  filter(ts >= "2020-06-29 00:00:00") %>%
#  filter(ts <= "2020-07-01 04:00:00")
View(Tag.detect) #67,669 entries #runlen>2= 62,150 entries #runlen>3= 60,669 entries

#First and last detections for this tag search
df.tagUnique<-Species.detect %>% #You can also try with Tag.detect instead of df.selecttagsFILTER
  distinct(mfgID, recvDeployName, recvProjName, recvDeployLat,recvDeployLon)
#View(df.tagUnique)
df.detectfirst<-Species.detect %>% 
  group_by(mfgID, recvDeployName) %>%
  slice(which.min(ts)) %>%
  distinct(mfgID, ts, recvDeployName, recvProjName, recvDeployLat,recvDeployLon) 
df.detectfirst<-rename(df.detectfirst, start_ts = ts)
#View(df.detectfirst)
#####
## Last detections
df.detectlast<-Species.detect %>% 
  group_by(mfgID, recvDeployName) %>%
  slice(which.max(ts)) %>%
  distinct(mfgID, ts, recvDeployName, recvProjName, recvDeployLat,recvDeployLon) 
df.detectlast<-rename(df.detectlast, end_ts = ts)
#View(df.detectlast)
## Assemble first/last dates seen for each receiver, with tag and receiver info.
df.TagStations <- left_join(df.detectfirst,df.detectlast, by = c("mfgID", "recvDeployName", "recvProjName", "recvDeployLat","recvDeployLon")) %>%
  select(mfgID, recvDeployName, recvProjName, start_ts, end_ts, recvDeployLat,recvDeployLon)
View(df.TagStations)
#write.csv(df.TagStations, "./EachMotus_firstlast_TUVU_2020.csv") #132 entries #runlen>2= 13 entries #runlen>3= 9 entries

df.TagsBothYears <- df.TagStations %>%
  filter(start_ts <= "2019-12-31 23:59:59") %>%
  filter(end_ts <= "2020-01-01 00:00:00")
View(df.TagsBothYears)

#Or by location/project:
Proj.detect<-Species.detect %>%
  filter(recvProjName == "ASCmotus")
View(Proj.detect)
########################################################
### 2020 detections Only:

# Filter to include only tags since 9/18/18 deployment at 20:27 UTC
#df.2020tags<-df.selecttags%>%
#  filter(freqsd < 0.1 & runLen > 2) %>%
#  filter(ts >= ymd_hms("2020-01-01 00:00:00"))
#df.2020tags <- df.2020tags %>%
#  select(ts,mfgID,recvDeployName,speciesEN,tagProjName,tagDeployStart,tagDepLat,tagDepLon,
#         recvSiteName,recvDeployLat,recvDeployLon,recvProjName,
#         motusTagID,markerNumber,sig,
#         runLen,recvProjID,
#         port, antBearing,
#        tagModel, tagLifespan, tagDeployID)
#View(df.2020tags)

############################

# Make spreadsheet of only first/last tags detected in 2020 that were banded in 2019.
df.firstlast2020TagstationsFrom2019 <- df.firstlast2020Tagstations %>% 
  filter(tagDeployStart <= ymd_hms("2019-12-31 00:00:00"))
View(df.firstlast2020TagstationsFrom2019)
## Make Excel sheet:
df.firstlast2020Tagstations <- with_tz(df.firstlast2020Tagstations, "MST")
write.csv(df.firstlast2020Tagstations, "./df.2020_tags_stations061220.csv")

############################
## Examples of other searches:

# Tag.detect<-Species.detect %>%
tag.search <- df.selecttagsFILTER %>%
  filter(mfgID == "47") #Look for a tag in the whole 213 database
View(tag.search)

#Isolate First and Last 2020 Tag detection table by species: ex: TUVU
TUVU.firstlastTagstations <- df.firstlastTagstations %>%
  filter(speciesEN == "Turkey Vulture")
View(TUVU.firstlastTagstations)

# Isolate Gray catbirds:
GRCA.detect<-df.selecttags %>%
  filter(speciesEN == "Gray Catbird")
View(GRCA.detect)
GRCA212<-GRCA.detect %>% 
  filter(mfgID == 212.1) %>% 
  select(ts, mfgID,recvDeployName,recvProjName,speciesEN,tagProjName,tagDepLat,tagDepLon,
         recvSiteName,recvDeployLat,recvDeployLon,recvProjName,
         motusTagID,markerNumber,sig,
         runLen,recvProjID,
         port, antBearing)
View(GRCA212)
## Last detections
df.detectlastGC<-GRCA.detect %>% 
  group_by(motusTagID,recvDeployName) %>%
  slice(which.max(as.Date(ts, '%Y/%m/%d %h:%m:%s')))

df.detectlastGC <- df.detectlastGC %>%
  select(ts, mfgID,recvDeployName,recvProjName,speciesEN,tagProjName,tagDepLat,tagDepLon,
         recvSiteName,recvDeployLat,recvDeployLon,recvProjName,
         motusTagID,markerNumber,sig,
         runLen,recvProjID,
         port, antBearing)
View(df.detectlastGC)

# Individual SWTH investigations: #59, 205.1
SWTH.detect215<-SWTH.detect %>% 
  filter(mfgID == 215.1) %>% 
  select(ts, mfgID,recvDeployName,recvProjName,speciesEN,tagProjName,tagDepLat,tagDepLon,
         recvSiteName,recvDeployLat,recvDeployLon,recvProjName,
         motusTagID,markerNumber,sig,
         runLen,recvProjID,
         port, antBearing)
View(SWTH.detect215)
##############
# Individual LEWO investigations: #65, 79 for dispatch#3 2020.
LW.detect65_79<-LEWO.detect %>% 
  filter(mfgID %in% c(65,79)) %>% 
  select(ts, mfgID,recvDeployName,recvProjName,speciesEN,tagProjName,tagDepLat,tagDepLon,
         recvSiteName,recvDeployLat,recvDeployLon,recvProjName,
         motusTagID,markerNumber,sig,
         runLen,recvProjID,
         port, antBearing)
View(LW.detect65_79)
##############
#Isolate thrushes project detections:
thrushes.detect<-df.selecttags %>%
  filter(tagProjName == "thrushes")
View(thrushes.detect)
#write.csv(thrushes.detect, file = "Thrushes_101619.csv")
thrushes.detect$tagDeployStart <- as_datetime(thrushes.detect$tagDeployStart)
thrushes.detect$tagDeployEnd <- as_datetime(thrushes.detect$tagDeployEnd)

thrushes.df.detect<-df.goodtags %>%
  filter(tagProjName == "thrushes")
View(thrushes.df.detect)
thrushes.df.detect$tagDeployStart <- as_datetime(thrushes.df.detect$tagDeployStart)
thrushes.df.detect$tagDeployEnd <- as_datetime(thrushes.df.detect$tagDeployEnd)


thrush.detectlast<-thrushes.detect %>% 
  group_by(motusTagID,recvDeployName) %>%
  slice(which.max(as.Date(ts, '%Y/%m/%d %h:%m:%s')))
thrush.detectlast <- thrush.detectlast %>%
  select(ts,mfgID,recvDeployName,speciesEN,tagProjName,tagDepLat,tagDepLon,
         recvSiteName,recvDeployLat,recvDeployLon,recvProjName,
         motusTagID,markerNumber,sig,
         runLen,recvProjID,
         port, antBearing)
View(thrush.detectlast)

#Other individual birds
indiv.detect<-df.goodtags %>%
  filter(tagProjName == "Tonra")
View(indiv.detect)
#write.csv(indiv.detect, file = "PUMA_in MT_101619.csv")

#Teller station
df.goodtagsTeller<-df.goodtags%>%
  filter(recvDeployName=="Teller Refuge-MT")
View(df.goodtagsTeller)

# Individual Tags/mfgID etc...
df.goodtags11<-df.goodtags%>%
  filter(mfgID=="11")
View(df.goodtags11)

#46 BHCO
df.goodtags79<-df.goodtags%>%
  filter(mfgID=="79")
View(df.goodtags79)
#Sanderling
lewo62.tag <- df.alltags %>% 
  filter(mfgID =="70")
View(lewo62.tag)
#saveRDS(sanderling.tag, "./sanderling29006_table092018")
sander <- readRDS("./sanderling29006_table092018", refhook = NULL)
View(sander)

# Get the LORING detections
#df.LORINGtags<-df.alltags%>%
#  filter(freqsd < 0.1 & runLen > 2)
#View(df.LORINGtags)

#saveRDS(df.alltags, "./df.alltags.RDS")

############################
totstable<-bind_rows(df.goodtags,df.goodtags226)
View(totstable)

#TUVU
tuvu4.tag <- df.goodtags %>% 
  filter(motusTagID =="34330")
View(tuvu4.tag)
#####################__________________

# Sample trials to find more tag detections (first deployed 09/11/18).
allrun.tags<-df.alltags%>%
  filter(freqsd < 0.1 & runLen >= 1) %>%
  filter(ts >= ymd_hms("2018-08-21 20:27:00")) # Date of first IR Station set-up
View(allrun.tags)
testrun.tags<-allrun.tags%>%
  filter(motusTagID !="32190") %>% #remove Gray Catbird from floodplain deployed on 09/15/2018
  filter(motusTagID !="32208") #remove Test Nanotag #76
View(testrun.tags)

#View Specific tag:
#allrun.tags<-df.goodtags
tag356<-allrun.tags%>%
  filter(motusTagID =="32210") #look specifically for myotis californicus from 09/18/18 #356
View(tag356)
#View Specific tag:
#allrun.tags<-df.goodtags
tag42<-allrun.tags%>%
  filter(motusTagID =="32190") #look specifically for myotis californicus from 09/18/18 #356
View(tag42)
#allrun.tags<-df.goodtags
tag47<-allrun.tags%>%
  filter(motusTagID =="32195") #look specifically for myotis californicus from 09/18/18 #356
View(tag47)
#####################___________________
# Look for other researcher's tags on our Motus stations:
# EXAMPLE: run tagme for "SG-04EBRPI3F784", “SG-6F3BRPI3EA63”, "SG-8550RPI3768F"

#Miller Ridge:
SG1 = "SG-04EBRPI3F784"
#SG1.motus <- tagme(projRecv = SG1, new = TRUE, update = TRUE)
SG1.motus <- tagme(projRecv = SG1, new = FALSE, update = TRUE)
sg1 <- tbl(SG1.motus, "alltags")

#Get unique nanotags detected.
#sg1<-df.goodtags
tmp <- sg1 %>% distinct() %>% collect %>% as.data.frame()
tmp$ts <- as_datetime(tmp$ts, tz = "UTC")
unique(tmp$motusTagID) # Comes up with: 

sg1 <- sg1 %>% 
  collect() %>% 
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "MST", origin = "1970-01-01"))
sgtags<-sg1%>%
  filter(freqsd < 0.1 & runLen > 2) %>%
  filter(ts >= ymd_hms("2018-08-21 20:27:00")) %>% # Date of first IR Station set-up
  filter(motusTagID !="32190") %>% #remove Gray Catbird from floodplain deployed on 09/15/2018
  filter(motusTagID !="32208") #remove Test Nanotag #76
View(sgtags) # Lots of false positives!!!

#Indian Ridge:
SG2 = "SG-6F3BRPI3EA63"
#SG2.motus <- tagme(projRecv = SG2, new = TRUE, update = TRUE)
SG2.motus <- tagme(projRecv = SG2, new = FALSE, update = TRUE)
sg2 <- tbl(SG2.motus, "alltags")

#Get unique nanotags detected.
tmp <- sg2 %>% distinct() %>% collect %>% as.data.frame()
tmp$ts <- as_datetime(tmp$ts, tz = "UTC")
unique(tmp$motusTagID)
tmp<-tmp %>%
  filter(tagProjID=="213")%>%
  filter(motusTagID !="32190")%>%
  filter(motusTagID !="32208")
#View(tmp)
sg2 <- sg2 %>% 
  collect() %>% 
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "MST", origin = "1970-01-01"))
sg2tags<-sg2%>%
  filter(freqsd < 0.1 & runLen > 2) %>%
  filter(ts >= ymd_hms("2018-08-21 20:27:00")) %>% # Date of first IR Station set-up
  filter(motusTagID !="32190") %>% #remove Gray Catbird from floodplain deployed on 09/15/2018
  filter(motusTagID !="32208") #remove Test Nanotag #76
View(sg2tags) # Lots of false positives!!!

#Pump Slough or Miller Creek:
SG3 = "SG-8550RPI3768F"
#SG3.motus <- tagme(projRecv = SG3, new = TRUE, update = TRUE)
SG3.motus <- tagme(projRecv = SG3, new = FALSE, update = TRUE)
sg3 <- tbl(SG3.motus, "alltags")

#Get unique nanotags detected.
tmp <- sg3 %>% distinct() %>% collect %>% as.data.frame()
tmp$ts <- as_datetime(tmp$ts, tz = "UTC")
unique(tmp$motusTagID)
tmp<-tmp %>%
  filter(tagProjID=="213")%>%
  filter(motusTagID !="32190")%>%
  filter(motusTagID !="32208")
#View(tmp)
sg3 <- sg3 %>% 
  collect() %>% 
  as.data.frame() %>%     # for all fields in the df (data frame)
  mutate(ts = as_datetime(ts, tz = "MST", origin = "1970-01-01"))
sg3tags<-sg3%>%
  filter(freqsd < 0.1 & runLen >= 1) %>%
  filter(ts >= ymd_hms("2018-08-21 20:27:00")) %>% # Date of first IR Station set-up
  filter(motusTagID !="32190") %>% #remove Gray Catbird from floodplain deployed on 09/15/2018
  filter(motusTagID !="32208") #remove Test Nanotag #76
View(sg3tags) # N tags other than test (#32208)
