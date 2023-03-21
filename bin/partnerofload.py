#
# partnerofload.py
###############################################################################
#
#  Purpose:
#
#      Load PAR relationships
#
# Usage:
#       partnerofload.py
#
#  Inputs:
#
#       File of relationships
#
#       1. PAR1 (organizer) MGI ID
#       2. PAR1 (organizer) symbol
#       3. PAR2 (participant) MGI ID
#       4. PAR22 (participant) symbol
#       5+ For curator use only; ignored by the load
#
#  Outputs:
#
#	1 BCP file:
#	A pipe-delimited file:
#       	MGI_Relationship.bcp
#
#       Diagnostics file - for verification calls to loadlib 
#       Error file - for verification calls to loadlib 
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#      2:  bcp fails
#
#  Assumes:
#
#	
#  Implementation:
#
#      1) Validate the arguments to the script.
#      2) Perform initialization steps.
#      3) Parse input file
#      4) Resolve MGI IDs to PAR marker keys
#      5) Write out to relationship bcp
#      6) Delete existing relationships
#      7) BCP in new relationships:
#
# History:
#
# sc	06/22/2022
#	- WTS2-1080 
#

import sys
import os
import string
import db
import subprocess

import mgi_utils
import loadlib
#db.setTrace()

CRT = '\n'
TAB = '\t'

# from configuration file
inputFileName = os.getenv('INPUT_FILE_DEFAULT')
outputDir = os.environ['OUTPUTDIR']

# if 'true',bcp files will not be bcp-ed into the database.
# Default is 'false'
DEBUG = os.getenv('LOG_DEBUG')

bcpFile = 'MGI_Relationship.bcp'
relationshipFileName = '%s/%s' % (outputDir, bcpFile)

#
# File descriptors
#
fpDiagFile = ''         # diagnostic file
fpErrorFile = ''        # error file
fpRelationshipFile = ''
fpInputFile = ''

#
# log file paths
#
head, tail = os.path.split(inputFileName)

diagFileName = outputDir + '/' + tail + '.diagnostics'
errorFileName = outputDir + '/' + tail + '.error'

# The category key 'PARtner_of"
catKey = 1012

# the relationship term akey
relTermKey = 111279238

# J:332469
refsKey = 604212

# the qualifier key 'Not Specified'
qualKey = 11391898

# the evidence key 'Not Specified'
evidKey = 17396909

# load user key
userKey = 1636

# database primary keys, will be set to the next available from the db
nextRelationshipKey = 1000	# MGI_Relationship._Relationship_key

cdate = loadlib.loaddate

# Purpose: prints error 'message' if it is not None
#     writes to log files and exits with 'status'
# Returns: nothing
# Assumes: Nothing
# Effects: Exits with 'status'

def exit(
    status,          # numeric exit status (integer)
    message = None   # exit message (str.
    ):

    if message is not None:
        sys.stderr.write('\n' + str(message) + '\n')

    try:
        fpDiagFile.write('\n\nEnd Date/Time: %s\n' % (mgi_utils.date()))
        fpErrorFile.write('\n\nEnd Date/Time: %s\n' % (mgi_utils.date()))
        fpDiagFile.close()
        fpErrorFile.close()
        fpInputFile.close()
    except:
        pass

    db.useOneConnection(0)
    sys.exit(status)

# end exit() -------------------------------

#
# Purpose: open files, create db connection, gets max keys from the db
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables, exits if a file can't be opened,
#
def initialize():

    global nextRelationshipKey

    #
    # Open input and output files
    #
    openFiles()

    #
    # create database connection
    #
    db.useOneConnection(1)

    db.set_sqlLogFunction(db.sqlLogAll)

    fpDiagFile.write('Start Date/Time: %s\n' % (mgi_utils.date()))
    fpDiagFile.write('Server: %s\n' % (db.get_sqlServer()))
    fpDiagFile.write('Database: %s\n' % (db.get_sqlDatabase()))

    fpErrorFile.write('Start Date/Time: %s\n\n' % (mgi_utils.date()))

    #
    # get next MGI_Relationship key
    #
    results = db.sql('''select nextval('mgi_relationship_seq') as nextKey''', 'auto')
    if results[0]['nextKey'] is None:
        nextRelationshipKey = 1000
    else:
        nextRelationshipKey = results[0]['nextKey']

    return 0

# end initialize() -------------------------------

# Purpose: Open input/output files.
# Returns: exit 1 if cannot open input or output file
# Assumes: Nothing
# Effects: sets global variables, exits if a file can't be opened
#
def openFiles ():

    global fpRelationshipFile, fpInputFile, fpDiagFile, fpErrorFile

    try:
        fpInputFile = open(inputFileName, 'r')
    except:
        print(('Cannot open input file: %s' % inputFileName))
        sys.exit(1)

    try:
        fpRelationshipFile = open(relationshipFileName, 'w')
    except:
        print(('Cannot open relationships bcp file: %s' % relationshipFileName))
        sys.exit(1)

    try:
        fpDiagFile = open(diagFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % diagFileName)

    try:
        fpErrorFile = open(errorFileName, 'w')
    except:
        exit(1, 'Could not open file %s\n' % errorFileName)


    return 0

# end openFiles() -------------------------------

#
# Purpose: Close the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
#
def closeFiles ():

    global fpRelationshipFile, fpInputFile

    fpRelationshipFile.close()
    fpInputFile.close()
    
    return 0

# end closeFiles() -------------------------------

# Purpose: read input, resolve to keys, write to bcp file
# Returns: 1 if error,  else 0
# Assumes: file descriptors have been initialized
# Effects: 
#

def processRelationships():
    global nextRelationshipKey

    line = fpInputFile.readline()
    lineNum = 1

    while line:
        lineNum += 1

        # get columns 1-6, already qc'd we know there are at least 4 columns
        (orgID, orgSym, partID, partSym) = list(map(str.strip, str.split(line, TAB)))[:6]

        orgKey = loadlib.verifyMarker(orgID, lineNum, fpErrorFile)
        partKey =  loadlib.verifyMarker(partID, lineNum, fpErrorFile)
        fpRelationshipFile.write('%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' % \
            (nextRelationshipKey, catKey, orgKey, partKey, relTermKey, qualKey, evidKey, refsKey, userKey, userKey, cdate, cdate))

        nextRelationshipKey += 1

        # Now the reciprocal
        fpRelationshipFile.write('%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' % \
            (nextRelationshipKey, catKey, partKey, orgKey, relTermKey, qualKey, evidKey, refsKey, userKey, userKey, cdate, cdate))

        nextRelationshipKey += 1

        line = fpInputFile.readline()
        
    return 0

# end processRelationships ----------------------

# Purpose: deletes existing relationships
# Returns: None
# Assumes: None
# Effects: None
#
def doDeletes():

    if DEBUG  == 'true':
        return 0

    db.sql('''delete from MGI_Relationship where _CreatedBy_key = %s ''' % userKey, None)
    db.commit()
    db.useOneConnection(0)

    return 0

# end doDeletes() -------------------------------------

# Purpose: loads bcp file
# Returns: exist 2 if bcp fails
# Assumes: None
# Effects: None
#
def bcpFiles():

    if DEBUG  == 'true':
        return 0

    bcpCommand = os.environ['PG_DBUTILS'] + '/bin/bcpin.csh'

    bcpCmd = '%s %s %s %s %s %s "|" "\\n" mgd' % \
            (bcpCommand, db.get_sqlServer(), db.get_sqlDatabase(), 'MGI_Relationship', outputDir, bcpFile)
    fpDiagFile.write('%s\n' % bcpCmd)
    result = subprocess.run(bcpCmd, shell=True, capture_output=True, text=True)
    stdout = result.stdout
    stderr = result.stderr
    statusCode = result.returncode

    if statusCode != 0:
        msg = '%s statusCode: %s stderr: %s%s' % (bcpCmd, statusCode, stderr, CRT)
        fpDiagFile.write(msg)
        return statusCode

    # update mgi_relationship auto-sequence
    db.sql(''' select setval('mgi_relationship_seq', (select max(_Relationship_key) from MGI_Relationship)) ''', None)

    return 0

# end bcpFiles() -------------------------------------

#
# Main
#

print('%s' % mgi_utils.date())
print ('initialize()')
if initialize() != 0:
    exit(1, 'Error in  initialize \n' )

print('%s' % mgi_utils.date())
print('processRelationships()')
# process the file, write to bcp
if processRelationships() != 0:
    exit(1, 'Error in  processRelationships \n' )

print('%s' % mgi_utils.date())
print('doDeletes()')
# delete existing relationships
if doDeletes() != 0:
    exit(1, 'Error in  doDeletes \n' )

print('%s' % mgi_utils.date())
print('closeFiles()')
# close all output files
if closeFiles() != 0:
    exit(1, 'Error in  closeFiles \n' )

print('%s' % mgi_utils.date())
print('bcpFiles()')
# bcp the relationships
if bcpFiles() != 0:
    exit(1, 'Error in  bcpFiles \n' )

exit(0, 'partnerofload successful')
