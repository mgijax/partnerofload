
#  parnterOfQC.py
###########################################################################
#
#  Purpose:
#
#	This script will generate a QC report for a par relationship
#	    input file
#
#  Usage:
#
#      partnerOfQC.py  filename
#
#      where:
#          filename = path to the input file
#
#  Inputs:
#      - input file as parameter - see USAGE
#
#  Outputs:
#
#      - QC report (${QC_RPT})
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#      2:  
#
#  Assumes:
#
#  Implementation:
#
#      This script will perform following steps:
#
#      1) Validate the arguments to the script.
#      2) Perform initialization steps.
#      3) Open the input/output files.
#      4) Generate the QC reports.
#      5) Close the input/output files.
#
#  History:
#
# 01/27/2023   sc   
#       FL2 project PAR relationships
#
###########################################################################

import sys
import os
import string
import db
import time
import Set

#
#  CONSTANTS
#
TAB = '\t'
CRT = '\n'

USAGE = 'Usage: parnterOfQC.py  inputFile'

#
#  GLOBALS
#

# from stdin
inputFile = None

# QC report file
qcRptFile = os.getenv('QC_RPT')

# lines seen in the input file
distinctLineList = []

# duplicated lines in the input
dupeLineList = []

# lines with < 4 columns
missingColumnList = []

# lines with missing data in columns
reqColumnList = []

# a PAR id is not found in the database
badParIdList = []

# org and part are same ID
orgPartSameList = []

# a PAR id does not match symbol in the database
idSymDiscrepList = []

# org or part genetic chr is not XY
invalidGeneticChrList = []

# one object must have genomic Chr=X and the other genomic Chr=Y
# report if not
invalidGenomicChrList = []

# 1 if any QC errors in the input file
hasFatalErrors = 0

# lookup of PAR MGI IDs {parID: parSymbol|genetic chromosome, ...}
parLookupDict = {}

# Purpose: Validate the arguments to the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: sets global variable, exits if incorrect # of args
# Throws: Nothing
#
def checkArgs ():
    global inputFile

    if len(sys.argv) != 2:
        print(USAGE)
        sys.exit(1)

    inputFile = sys.argv[1]
    return 0

# end checkArgs() -------------------------------

# Purpose: create lookups, open files
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables, exits if a file can't be opened,
#  creates files in the file system, creates connection to a database

def init ():

    # open input/output files
    openFiles()
    db.useOneConnection(1)

    #
    # create lookups
    #

    loadLookups()

    return 0

# end init() -------------------------------

# Purpose: load lookups for verification
# Returns: Nothing
# Assumes: 
# Effects: queries a database, modifies global variables
#

def loadLookups(): 
    global parLookupDict

    results = db.sql('''select a.accid, m.symbol, m.chromosome as geneticChr
        from acc_accession a, mrk_marker m
        where m.chromosome = 'XY'
        and m._marker_status_key = 1 -- official
        and m._marker_key = a._object_key
        and a._mgitype_key = 2
        and a._logicaldb_key = 1
        and a.preferred = 1
        and a.private = 0
        and a.prefixPart = 'MGI:' ''', 'auto')

    for r in results:
        parLookupDict[r['accid']] = '%s|%s' % (r['symbol'], r['geneticChr'])
    
    return 0

# end loadLookups() -------------------------------

def lookupGenomicChr(accID):

    results = db.sql('''select a.accid, c.genomicChromosome
        from mrk_location_cache c, acc_accession a
        where c._marker_key = a._object_key
        and a._mgitype_key = 2
        and a._logicaldb_key = 1
        and a.prefixpart = 'MGI:'
        and a.accid = '%s' ''' % accID, 'auto')
    if len(results):
        return results[0]['genomicChromosome']
    else:
        return 0
#
# Purpose: Open input and output files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def openFiles ():
    #global fpInput, fpLoadReady, fpQcRpt
    global fpInput, fpQcRpt

    #
    # Open the input file
    #
    # encoding='utf-8' no
    # encoding=u'utf-8' no

    try:
        fpInput = open(inputFile, 'r', encoding='utf-8', errors='replace')
    except:
        print('Cannot open input file: %s' % inputFile)
        sys.exit(1)

    #
    # Open QC report file
    #
    try:
        fpQcRpt = open(qcRptFile, 'w')
    except:
        print('Cannot open report file: %s' % qcRptFile)
        sys.exit(1)

    return 0

# end openFiles() -------------------------------

#
# Purpose: writes out errors to the qc report
# Returns: Nothing
# Assumes: Nothing
# Effects: writes report to the file system
# Throws: Nothing
#

def writeReport():
    #
    # Now write any errors to the report
    #
    if not hasFatalErrors:
         fpQcRpt.write('No QC Errors')
         return 0
    fpQcRpt.write('Fatal QC - if published the file will not be loaded')

    if len(dupeLineList):
        fpQcRpt.write(CRT + CRT + str.center('Lines Duplicated In Input',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(dupeLineList))
        fpQcRpt.write(CRT + 'Total: %s' % len(dupeLineList))

    if len(missingColumnList):
        fpQcRpt.write(CRT + CRT + str.center('Lines with < 4 Columns',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(CRT.join(missingColumnList))
        fpQcRpt.write(CRT + 'Total: %s' % len(missingColumnList))

    if len(reqColumnList):
        hasSkipErrors = 1
        fpQcRpt.write(CRT + CRT + str.center('Missing Data in Required Columns',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(reqColumnList))
        fpQcRpt.write(CRT + 'Total: %s' % len(reqColumnList))

    if len(orgPartSameList):
        fpQcRpt.write(CRT + CRT + str.center('Organizer and Participant have same ID',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(orgPartSameList))
        fpQcRpt.write(CRT + 'Total: %s' % len(orgPartSameList))

    if len(badParIdList):
        fpQcRpt.write(CRT + CRT + str.center('Invalid PAR Relationship',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(badParIdList))
        fpQcRpt.write(CRT + 'Total: %s' % len(badParIdList))

    if len(idSymDiscrepList):
        fpQcRpt.write(CRT + CRT + str.center('Organizer and/or Participant ID does not match Symbol',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(idSymDiscrepList))
        fpQcRpt.write(CRT + 'Total: %s' % len(idSymDiscrepList))

    if len(invalidGeneticChrList):
        fpQcRpt.write(CRT + CRT + str.center('Organizer and/or Participant Genetic Chromosome is not "XY"',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(invalidGeneticChrList))
        fpQcRpt.write(CRT + 'Total: %s' % len(invalidGeneticChrList))

    if len(invalidGenomicChrList):
        fpQcRpt.write(CRT + CRT + str.center('Organizer/Participant Genomic Chromosome not "X" or "Y"',60) + CRT)
        fpQcRpt.write('%-12s  %-20s%s' % ('Line#','Line', CRT))
        fpQcRpt.write(12*'-' + '  ' + 20*'-' + CRT)
        fpQcRpt.write(''.join(invalidGenomicChrList))
        fpQcRpt.write(CRT + 'Total: %s' % len(invalidGenomicChrList))

    return 0

# end writeReport() -------------------------------

#
# Purpose: Close the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Modifies global variables
# Throws: Nothing
#
def closeFiles ():
    #global fpInput, fpLoadReady, fpQcRpt
    global fpInput, fpQcRpt
    fpInput.close()
    #fpLoadReady.close()
    fpQcRpt.close()

    return 0

# end closeFiles) -------------------------------

    #
    # Purpose: run all QC checks
    # Returns: Nothing
    # Assumes: file descriptors have been initialized
    # Effects: writes reports and the load ready file to file system
    # Throws: Nothing
    #

def runQcChecks():
    global hasFatalErrors, distinctLineList, dupeLineList
    global missingColumnList, reqColumnList, orgPartSameList, badParIdList
    global idSymDiscrepList, invalidGeneticChrList


    lineNum = 0
    # no header line
    for line in fpInput.readlines():
        lineList = str.split(line, TAB)[:4]
        lineNum += 1
        #print('lineNum: %s line: %s' % (lineNum, line))
        if line not in distinctLineList:
            distinctLineList.append(line)
        else:
            dupeLineList.append('%s  %s' % (lineNum, line))
        # check that the file has at least 4 columns
        print(str.split(line, TAB))
        if len(str.split(line, TAB)) < 4:
            missingColumnList.append('%s  %s' % (lineNum, line))
            hasFatalErrors = 1
            line = fpInput.readline()
            continue
        # get columns 1-4
        (orgID, orgSym, partID, partSym) = list(map(str.strip, str.split(line, TAB)))[:4]
        print('%s %s %s %s' % (orgID, orgSym, partID, partSym))
        # all columns required
        if orgID == '' or orgSym == '' or partID == '' or partSym == '':
            reqColumnList.append('%s  %s' % (lineNum, line))
            #hasFatalErrors = 1
            line = fpInput.readline()
            continue
        # Now verify each column

        # are the organizer and participant different?
        print('orgID: %s partID: %s' % (orgID, partID))
        if orgID == partID:
            orgPartSameList.append('%s  %s' % (lineNum, line))
            #hasFatalErrors = 1
            line = fpInput.readline()
            continue
        # is orgID a par ID?
        if orgID not in parLookupDict:
            print('orgID not in parLookupDict')
            badParIdList.append('%s  %s' % (lineNum, line))
            #hasFatalErrors = 1
            line = fpInput.readline()
            continue
        else:
            symChr =  parLookupDict[orgID]
            symbol, geneticChr = str.split(symChr, '|')
            print('org geneticChr: "%s" symbol: %s' % (geneticChr, symbol))
            # is orgSym geneticChr 'XY'
            if geneticChr != 'XY':
                print('org geneticChr not "XY": %s symbol: %s' % (geneticChr, symbol))
                invalidGeneticChrList.append('%s  %s' % (lineNum, line)) 
                #hasFatalErrors = 1
                line = fpInput.readline()
                continue

            # does orgSym match orgID?
            if orgSym != symbol:
                print ('orgSym != symbol')
                idSymDiscrepList.append('%s  %s' % (lineNum, line))
                #hasFatalErrors = 1
                line = fpInput.readline()
                continue

        # is partID  a PAR ID?
        if partID not in parLookupDict:
            print('partID not in parLookupDict')
            badParIdList.append('%s  %s' % (lineNum, line))
            #hasFatalErrors = 1
            line = fpInput.readline()
            continue

        else:
            symChr =  parLookupDict[partID]
            symbol, geneticChr = str.split(symChr, '|')
            print('part geneticChr: "%s" symbol: %s' % (geneticChr, symbol))
            # is partSym geneticChr 'XY'?
            if geneticChr != 'XY':
                print('geneticChr not "XY": %s symbol: %s' % (geneticChr, symbol))
                invalidGeneticChrList.append('%s  %s' % (lineNum, line))
                #hasFatalErrors = 1
                line = fpInput.readline()
                continue

            # does partSym match partID?
            if partSym != symbol:
                print('partSym != symbol')
                idSymDiscrepList.append('%s  %s' % (lineNum, line))
                #hasFatalErrors = 1
                line = fpInput.readline()
                continue


        # check that one marker has genomic Chr=X and the other Chr=Y
        # 0 returned from the lookup indicates invalid ID
        genChrSet = set()
        genChrSet.add(lookupGenomicChr(orgID))
        genChrSet.add(lookupGenomicChr(partID))
        print('GenChrSet: %s' % genChrSet)
        if 'Y' in genChrSet and 'X' in genChrSet:
            # we are good - pass
            pass
        else:
            invalidGenomicChrList.append('%s  %s' % (lineNum, line))

        line = fpInput.readline()
    return 0

# end runQcChecks() -------------------------------

#
# Main
#
print('checkArgs(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
checkArgs()

print('init(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
init()

print('runQcChecks(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
runQcChecks()

print('writeReport(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
writeReport()

print('closeFiles(): %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
sys.stdout.flush()
closeFiles()

db.useOneConnection(0)
print('done: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))

if hasFatalErrors == 1 :
    sys.exit(2)
else:
    sys.exit(0)

