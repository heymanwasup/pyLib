'''
Parse the CxAOD directories, make the table with this format:
DSID | Name of CxAOD sampes | Name in hist | Descriptions | p tag | full tag | Period | FS or AFII | Usage 
'''

import os,re
import sqlite3 as sql


class SampleData(object):
    def __init__(self,debug=True):
        self.debug = debug
        db_samples = './db/SamplesIndo.db'
        db_xsections = './db/XSections.db'
        if self.debug:
          os.system('rm ./db/XSections.db')
          os.system('rm ./db/SamplesIndo.db')
        self.DB_SAMPLE = sql.connect(db_samples)
        self.DB_XSECTION = sql.connect(db_xsections)
    def initialize_db_xsection(self):
        cursor = self.DB_XSECTION.cursor()
        KEYS = ['DSID','XSection','kFactor','fEff','Name','Discription']
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS XSECTIONS(
                {0:} INT PRIMARY KEY NOT NULL,
                {1:} REAL NOT NULL,
                {2:} REAL NOT NULL,
                {3:} REAL NOT NULL,
                {4:} TEXT NOT NULL,
                {5:} TEXT
            );'''.format(*KEYS))
        path_to_xsection = '/afs/cern.ch/work/c/chenc/CxAODFW/CxAODFramework_branch_master_21.2.37_1_TP/source/FrameworkSub/data/XSections_13TeV.txt'

        with open(path_to_xsection,'read') as f:
            data = f.readlines()

        keys = ','.join(KEYS)
        for line in data:            
            n = line.find('#')            
            line = line[:n]
            if len(line)==0:
                continue
            splitted = re.split('[ \t]+', line)
            if splitted[0] == '':
                splitted = splitted[1:]
            if len(splitted)<6:
              print 'skip', splitted
              continue
            dsid = int(splitted[0])
            xsec = float(splitted[1])
            kFac = float(splitted[2])
            fEff = float(splitted[3])
            name = splitted[4]
            desc = splitted[5]
            cursor.execute('''
                INSERT INTO XSECTIONS({0:})\
                VALUES({1:},{2:},{3:},{4},"{5}","{6}");\
                '''.format(keys,dsid,xsec,kFac,fEff,name,desc))
        self.DB_XSECTION.commit()

    def initialize_db_sample(self):
        path_to_16a = '/eos/atlas/atlascerngroupdisk/phys-higgs/HSG5/Run2/VH/CxAOD_r31-10/HIGG2D4_13TeV/CxAOD_31-10_a'
        path_to_16d = '/eos/atlas/atlascerngroupdisk/phys-higgs/HSG5/Run2/VH/CxAOD_r31-10/HIGG2D4_13TeV/CxAOD_31-10_d'
        
        self.fill_period(path_to_16a,'mc16a')
        self.fill_period(path_to_16d,'mc16d')
    
    def fill_period(self,path_to_samples,period):
        samples_list = '/afs/cern.ch/work/c/chenc/CxAODFW/CxAODFramework_branch_master_21.2.37_1_TP/source/FrameworkSub/data/XSections_13TeV.txt'



sd = SampleData()
sd.initialize_db_xsection()
    

