#!/usr/bin/env python

'''
Parse the CxAOD directories, make the table as this format:
DSID,  SampleName,  HistName,  Description,  Period,  Tag,  pTag,  AFII/FS, Usage

Read Xsection files, make table of xsections 
'''

import os,re,time
import sqlite3 as sql

import toolkit

class XsectionDataBase(object):
    def __init__(self,db_xsections,debug=True):
        self.debug = debug        
        if self.debug:
          os.system('rm '+db_xsections)
        self.DB_XSECTION = sql.connect(db_xsections)

    def make_xsection_table(self,table_name):
        path_to_xsection = '../data/XSections_13TeV.txt'
        xsection_ftag_tp = '../data/XSections_FTAG_TP.txt'
        xsection_ftag_pdf = '../data/XSection-MC15-13TeV.data'        
        self.fill_db_xsection(self.DB_XSECTION,table_name,path_to_xsection,'VHbb')
        self.update_xsection(xsection_ftag_pdf,flag='PDF')
        self.update_xsection(xsection_ftag_tp,flag='TP')
        self.DB_XSECTION.commit()
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
                    tempFMT = '{0:<7} {1:<10.3E} {2:<10.2E} {3:<7.2f} {4:<7.2f} {5:<25} {6:<30}'
                    tempHeader = '{0:<7} {1:<10} {2:<10} {3:<7} {4:<7} {5:<25} {6:<30}'
                    print '\n--------------'
                    print '{0:}  {1:.2f}% '.format(dsid,100*diff)
                    print tempHeader.format('','total','xsec','kFac','fEff','sample','description')
                    print tempFMT.format(*(('init',total_xsec_a)+itm_a[1:]))
                    print tempFMT.format(flag,total_xsec_b,xsec,kfac,feff,name,description)                    
                    continue
        alt_db.close()

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
            );
            '''.format(table_name))        
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
        




class CxAODSampleDataBase(object):
    def __init__(self,db_samples,debug=True):
        self.debug = debug                
        if self.debug:          
          os.system('rm '+ db_samples)
        self.DB_SAMPLE = sql.connect(db_samples) 

    def set_db_xsection(self,db_xsection):
        self.DB_XSECTION = sql.connect(db_xsection)

    def make_cxaod_table(self,table_name):
        self.table_name = table_name
        self.initialize_db_sample()
        self.add_column_usage()        
        self.DB_SAMPLE.commit()
    def initialize_db_sample(self):        
        cursor = self.DB_SAMPLE.cursor()
        KEYS = ['DSID','SampleName','HistName','Description','Period','Tag','pTag','AFII/FS']
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS %s(
            {0:} INT NOT NULL,
            {1:} TEXT,
            {2:} TEXT,
            {3:} TEXT,
            {4:} TEXT NOT NULL,
            {5:} TEXT,
            {6:} TEXT,
            "{7:}" TEXT\
            );'''.format(*KEYS)%(self.table_name))
        path_to_16a = '/eos/atlas/atlascerngroupdisk/phys-higgs/HSG5/Run2/VH/CxAOD_r31-10/HIGG2D4_13TeV/CxAOD_31-10_a'
        path_to_16d = '/eos/atlas/atlascerngroupdisk/phys-higgs/HSG5/Run2/VH/CxAOD_r31-10/HIGG2D4_13TeV/CxAOD_31-10_d'
        self.fill_period(path_to_16a,'mc16a')
        self.fill_period(path_to_16d,'mc16d')        

    def add_column_usage(self):
        self.DB_SAMPLE.create_function('get_samples_usage',2,get_samples_usage)
        cursor = self.DB_SAMPLE.cursor()
        cursor.execute('''
            ALTER TABLE %s ADD COLUMN Usage TEXT;
            '''%(self.table_name))
        cursor.execute('''
            UPDATE %s
            SET Usage = "nominal"
            WHERE get_samples_usage(DSID,"AFII/FS")=1;
            '''%(self.table_name))
        cursor.execute('''
            UPDATE %s
            SET Usage = "alternative"
            WHERE get_samples_usage(DSID,"AFII/FS")=2;
            '''%(self.table_name))
        cursor.execute('''
            UPDATE %s
            SET Usage = "ununsed"
            WHERE get_samples_usage(DSID,"AFII/FS")=0;
            '''%(self.table_name))                

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
                    INSERT INTO %s({0:})\
                    VALUES(?,?,?,?,?,?,?,?);\
                    '''.format(sKEYS)%(self.table_name),values)

    def parse_sample_name(self,sample_name):
        dsid,tag = re.findall('group.phys-higgs.mc16_13TeV.([0-9]+).CAOD_HIGG2D4\.([^\.]+)\.31-10', sample_name)[0]
        ptag = re.findall('p[0-9]+', tag)[0]
        if re.match('.*a[0-9]+.*', tag):
            FSorAFii = 'AFii'
        else:
            FSorAFii = 'FS'
        return dsid,tag,ptag,FSorAFii

def get_samples_usage(dsid,FSorAFii):
    #return 0 not used
    #return 1 nominal
    #return 2 alternative
    ttbar_nominal = [
        410472, #nominal+AF2
    ]
    ttbar_alter = [
        410482, #radHi
        410442, #aMC@NLO+Py8, Matrix Element
        410558, #Pow+HW7, Fragmentation
        410252, #sherpa 2.2.1    
    ]
    stop_nominal = [
        410646, # Wt,DR, TOP, AFII/FS
        410647, # Wt,DR, ANTI-TOP, AFII/FS    
        410648, # Wt,dilep,DR, TOP, AFII/FS
        410649, # Wt,dilep,DR, ANTI-TOP, AFII/FS            
    ]
    stop_alter = [    
        410654, # Wt,DS, TOP
        410655, # Wt,DS, ANTI-TOP
        410656, # Wt,dilep,DS, TOP
        410657, # Wt,dilep,DS, ANTI-TOP
    ]
    diboson_nominal = [
        364250, #sh222 4l
        364253, #sh222 3l
        364254, #sh222 2l
    ]
    diboson_alter = [
        361600,#pow+py8 WWlvlv
        361601,#pow+py8 WZlvll
        361603,#pow+py8 ZZllll
        361604,#pow+py8 ZZvvll
        361607,#pow+py8 WZqqll
        361610,#pow+py8 ZZqqll
    ]
    zjets_nominal = [
        #ztt sherpa221
        364128,
        364129,
        364130,
        364131,
        364132,
        364133,
        364134,
        364135,
        364136,
        364137,
        364138,
        364139,
        364140,
        364141,
        #ztt sherpa 221 low mll
        364210,
        364211,
        364212,
        364213,
        364214,
        364215,
    ]
    zjets_alter = [
        #ztt madgraph
        361510,
        361511,
        361512,
        361513,
        361514,
    ]

    dsids_nominal = set(ttbar_nominal+stop_nominal+diboson_nominal+zjets_nominal)
    dsids_alter = set(ttbar_alter+stop_alter+diboson_alter+zjets_alter)
    res = 0    
    if dsid in dsids_nominal:
        if FSorAFii == 'AFii':
            if dsid in ttbar_nominal or dsid in stop_nominal:
                res = 2
        else:
            res = 1
    elif  dsid in dsids_alter:
        res = 2
    else:
        res = 0
    #print dsid,FSorAFii,res
    return res
def main():
    isDebug = False
    isMakeXsection = False
    isMakeCxAODSamples = False

    db_xsections = './db/XSections.db'
    tab_xsections = 'XSECTIONS'
    db_samples = './db/CxAODSamples.db'
    tab_samples = 'SAMPLES_CxAOD'

    xsecDB = XsectionDataBase(db_xsections,debug=isDebug)
    if isMakeXsection:        
        xsecDB.make_xsection_table(tab_xsections)

    sampleDB = CxAODSampleDataBase(db_samples,debug=isDebug)
    sampleDB.set_db_xsection(db_xsections)
    if isMakeCxAODSamples:                
        sampleDB.make_cxaod_table(tab_samples)

    toolkit.DumpToCSV(db_samples, 'output/CxAODSamples.CSV', tab_samples)

main()