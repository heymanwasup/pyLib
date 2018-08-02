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
        path_to_xsection = '/afs/cern.ch/work/c/chenc/CxAODFW/CxAODFramework_branch_master_21.2.37_1_TP/source/FrameworkSub/data/XSections_13TeV.txt'
        self.fill_db_xsection(self.DB_XSECTION,'XSECTIONS',path_to_xsection,'VHbb')

    def update_db_xsection(self):
        xsection_ftag_tp = '/afs/cern.ch/work/c/chenc/HEP_pyLib/../XSections_FTAG_TP.txt'
        xsection_ftag_pdf = '/afs/cern.ch/work/c/chenc/bjets_ttbardilepton_PDF/AnalysisTop-21.2.X/grid/TopDataPreparation/XSection-MC15-13TeV.data'

        self.update_xsection(xsection_ftag_pdf,flag='PDF')
        self.update_xsection(xsection_ftag_tp,flag='TP')

    def update_xsection(self,path_to_xsection,flag=''):
        alt_db = sql.connect(':memory:')
        #path_to_xsection = '/afs/cern.ch/work/c/chenc/HEP_pyLib/../XSections_FTAG_TP.txt'
        self.fill_db_xsection(alt_db,'XSECTIONS_ATL',path_to_xsection,flag=flag)
        c_a = self.DB_XSECTION.cursor()
        c_b = alt_db.cursor()
        c_b.execute('SELECT * FROM XSECTIONS_ATL;')
        sKEYS = ','.join(['DSID','XSection','kFactor','fEff','Name','Description'])
        itms_b = c_b.fetchall()
        for dsid,xsec,kfac,feff,name,description in itms_b:
            if 'h125' in (name+description).lower() or\
             'ggh' in (name+description).lower() or\
             'hzz' in (name+description).lower() or\
             'bba' in (name+description).lower():
              continue
            c_a.execute('''
                SELECT * FROM XSECTIONS
                WHERE DSID=?;
                ''',(dsid,))
            itms_a = c_a.fetchall()
            if len(itms_a)==0:
                print 'Add from {0:<5}: {1:<8} {2:<10} {3:<20} '.format(flag,dsid,name,description)
                c_a.execute('''
                    INSERT INTO XSECTIONS({0:})\
                    VALUES(?,?,?,?,?,?);\
                    '''.format(sKEYS),(dsid,xsec,kfac,feff,name,description))
            else:
                itm_a = itms_a[0]

                total_xsec_a = itm_a[1]*itm_a[2]*itm_a[3]
                total_xsec_b = xsec*kfac*feff
                diff = 2.*abs((total_xsec_a-total_xsec_b)/(total_xsec_b+total_xsec_a))
                if diff>0.01:
                    tempFMT = '{0:<10}  tot: {1:<8.3E} | {2:<8.3E} {3:<8.2f} {4:<8.2f} {5:<10} {6:<30}'
                    print '\n---'
                    print '{0:}  {1:.2f}% '.format(dsid,100*diff)
                    print tempFMT.format(*(('init',total_xsec_a)+itm_a[1:]))
                    print tempFMT.format(flag,total_xsec_b,xsec,kfac,feff,name,description)
                    
                    continue
                    
    def fill_db_xsection(self,db,table_name,path_to_xsection,flag=''):
        cursor = db.cursor()
        KEYS = ['DSID','XSection','kFactor','fEff','Name','Description']
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS {0:}(
                DSID INT PRIMARY KEY NOT NULL,
                XSection REAL NOT NULL,
                kFactor REAL DEFAULT 1,
                fEff REAL DEFAULT 1,
                Name TEXT DEFAULT "",
                Description TEXT DEFAULT ""
            );'''.format(table_name))
        
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

            if flag == 'PDF':
                if len(splitted)<4:
                    print 'skip (in PDF)',splitted
                    continue
                sKEYS = ','.join(['DSID','XSection','kFactor','Description'])
                dsid = int(splitted[0])
                xsec = float(splitted[1])
                kFac = float(splitted[2])
                desc = splitted[3]
                cursor.execute('''
                    INSERT INTO {0:}({1:})\
                    VALUES(?,?,?,?);
                    '''.format(table_name,sKEYS),(dsid,xsec,kFac,desc))
            else:
                if len(splitted)<6:
                    print 'skip (in %s)'%(flag),splitted
                    continue      
                sKEYS = ','.join(['DSID','XSection','kFactor','fEff','Name','Description'])                                      
                dsid = int(splitted[0])
                xsec = float(splitted[1])
                kFac = float(splitted[2])
                fEff = float(splitted[3])
                name = splitted[4]
                desc = splitted[5]
                cursor.execute('''
                    INSERT INTO {0:}({1:})\
                    VALUES(?,?,?,?,?,?);
                    '''.format(table_name,sKEYS),(dsid,xsec,kFac,fEff,name,desc))
        db.commit()

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
                sKEYS = ','.join(KEYS)
                cursor.execute('''
                    INSERT INTO SAMPLES({0:})\
                    VALUES(?,?,?,?,?,?,?,?);\
                    '''.format(sKEYS),values)
        self.DB_SAMPLE.commit()

    def parse_sample_name(self,sample_name):
        dsid,tag = re.findall('group.phys-higgs.mc16_13TeV.([0-9]+).CAOD_HIGG2D4\.([^\.]+)\.31-10', sample_name)[0]
        ptag = re.findall('p[0-9]+', tag)[0]
        if re.match('.*a[0-9]+.*', tag):
            FSorAFii = 'AFii'
        else:
            FSorAFii = 'FS'
        return dsid,tag,ptag,FSorAFii

    def add_is_used(self):
        def filter_pdf(dsid,FSorAFii):
            if not False:
                pass

sd = SampleData()
sd.initialize_db_xsection()
sd.update_db_xsection()
sd.initialize_db_sample()
