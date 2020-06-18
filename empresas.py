from optparse import OptionParser
from Crowler import Crowler
from Db import Postgres
#from Types import , Rendimento, Error, Report
#from Email import Email
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def getOnline(db):
    fc = Crowler()
    codigoexec = datetime.now().isoformat()
    for i in fc.getAll():
        d = fc.getDetail(i[0])
        d['nomecomercial'] = i[1]
        d['razaosocial'] = i[2]
        d['codigoexec'] = codigoexec
        if 'error' in d:
            db.insertError(d)            
        else:
            db.insertEmpresa(d)
            

def makeReport(db):
    logger.info('Making The Report')
    r = Report()
    r.erros = db.getErrors()
    r.subtipos = db.getSubtipos()
    r.recomendacoes = db.getRecomendacoes()
    return r

def buildOptParser():
    parser = OptionParser('usage: %prog [options]')
    parser.add_option('--log', dest='loglevel', default='ERROR', 
        help='set log level')
    parser.add_option('--online', action='store_true', 
        dest='online', default=False, 
        help='update fiis with online info')
    parser.add_option('--db', dest='dbconn', 
        default='host=localhost dbname=empresa user=postgres password=postgres', 
        help='database connection info')
    parser.add_option('--email', dest='email', default=None, 
        help='destination email to send statistics')
    return parser

if __name__ == '__main__':
    try:
        # Command line arguments
        parser = buildOptParser()
        (opt, args) = parser.parse_args()
        # logging
        FORMAT = '%(asctime)s %(levelname)s %(module)s %(message)s'
        logging.basicConfig(level=getattr(logging, opt.loglevel.upper()),
            format=FORMAT)
        # Database 
        db = Postgres(opt.dbconn)
        # Crowler from internet
        if opt.online: getOnline(db)
        # Report
        #report = makeReport(db)
        # Show
        #if opt.email:
        #    fe = Email('lucio.m.prado@hotmail.com', 'Suntech03;', opt.email)
        #    fe.send(report)
        #else:
        #    print(report)
    except Exception as error:
        print('Error: {0}'.format(str(error)))

