import os
from os import path

from db.pghandler import CONFIG, Connection
from db.tables import TABLES, dropTables


def appendCommas(infile, outfile, ncommas):
    extracommas = ',' * ncommas
    with open(path.join(infile)) as infile:
        with open(path.join(outfile), 'wb') as outfile:
            for line in infile:
                outfile.write(line[:-1] + extracommas + '\n')
            outfile.flush()

def insertComma(infile, outfile, afterComma):
    with open(path.join(infile)) as infile:
        with open(path.join(outfile), 'wb') as outfile:
            for line in infile:
                fields = line.split(',', afterComma)
                newline = fields[0] + ','
                for f in fields[1:-1]:
                    newline += f + ','
                newline += ',' + fields[-1]
                outfile.write(newline)
            outfile.flush()

def readGTFSFromFolder(foldername, datasetId):
    for table in TABLES:
        filepath = path.join (foldername, table.name + '.txt')
        with Connection() as con:
            con.execute(table.getCreateStatement("_raw", True, True))
        cmd = '\copy %s_raw (%s) from %s (format csv, delimiter ",", header true)' % (table.name, table.fieldnames, filepath)
        config = CONFIG['LOCAL']
        h = config.DB_HOST
        p = config.DB_PORT
        d = config.DB_NAME
        u = config.DB_USER
        w = config.DB_PASS
        bashc = "export PGPASSWORD=%s; psql -d %s -h %s -p %s -U %s -w -c '%s'" % (w, d, h, p, u, cmd)
        os.system(bashc)
        with Connection() as con:
            sql = "UPDATE %s_raw SET dataset_id=%d WHERE dataset_id IS NULL" % (table.name, datasetId)
            con.execute(sql)


def readGTFSFoldersFromBase(foldername):
    i = 1
    for name in [path.join(foldername, name) for name in os.listdir(foldername) if os.path.isdir(os.path.join(foldername, name))]:
        readGTFSFromFolder(name, i)
        i += 1


if __name__ == '__main__':
    dropTables("_raw")
    readGTFSFromFolder('~/gtfs_ams')
