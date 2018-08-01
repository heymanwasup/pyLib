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
        KEYS = ['DSID','XSection','kFactor','fEff','Name','Description']
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
        cursor = self.DB_SAMPLE.cursor()
        KEYS = ['DSID','SampleName','HistName','Description','Period','Tag','pTag','AFII/FS']
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS SAMPLES(
            {0:} INT NOT NULL,
            {1:} TEXT,
            {2:} TEXT,
            {3:} TEXT,
            {4:} TEXT NOT NULL,
            {5:} TEXT,
            {6:} TEXT,
            "{7:}" TEXT\
            );'''.format(*KEYS))

        path_to_16a = '/eos/atlas/atlascerngroupdisk/phys-higgs/HSG5/Run2/VH/CxAOD_r31-10/HIGG2D4_13TeV/CxAOD_31-10_a'
        path_to_16d = '/eos/atlas/atlascerngroupdisk/phys-higgs/HSG5/Run2/VH/CxAOD_r31-10/HIGG2D4_13TeV/CxAOD_31-10_d'
        self.fill_period(path_to_16a,'mc16a')
        self.fill_period(path_to_16d,'mc16d')
        

    def fill_period(self,path_to_samples,period):
        KEYS = ['DSID','SampleName','HistName','Description','Period','Tag','pTag','"AFII/FS"']
        files = os.listdir(path_to_samples)
        for f in files:
            path_to_f = os.path.join(path_to_samples,f)
            if not os.path.isdir(path_to_f):
                continue
            sub_files = os.listdir(path_to_f)
            for sub_f in sub_files:
                path_to_sub_f = os.path.join(path_to_f,sub_f)
                if not os.path.isdir(path_to_sub_f) or not 'group.phys-higgs.mc16_13TeV' in path_to_sub_f:
                    continue
                dsid,tag,ptag,FSorAFii = self.parse_sample_name(sub_f)
                cursor = self.DB_XSECTION.cursor()
                cursor.execute('''
                    SELECT Name,Description FROM XSECTIONS
                    WHERE DSID == {0:};
                    '''.format(dsid))
                res = cursor.fetchall()
                if len(res)==0:
                    hname,description = 'NULL','NULL'
                else:
                    hname,description = res[0]
                values = [dsid,f,hname,description,period,tag,ptag,FSorAFii]
                cursor = self.DB_SAMPLE.cursor()
                cursor.execute('''
                    INSERT INTO SAMPLES({0:})\
                    VALUES({1:},"{2:}","{3:}","{4}","{5}","{6}","{7}","{8:}");\
                    '''.format(*([','.join(KEYS)]+values)))
        self.DB_SAMPLE.commit()

    def parse_sample_name(self,sample_name):
        dsid,tag = re.findall('group.phys-higgs.mc16_13TeV.([0-9]+).CAOD_HIGG2D4\.([^\.]+)\.31-10', sample_name)[0]
        ptag = re.findall('p[0-9]+', tag)[0]
        if re.match('.*a[0-9]+.*', tag):
            FSorAFii = 'AFii'
        else:
            FSorAFii = 'FS'
        if tag[-1] == '_'
        return dsid,tag,ptag,FSorAFii




sd = SampleData()
sd.initialize_db_xsection()
sd.initialize_db_sample()
